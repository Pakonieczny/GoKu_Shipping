// The step rail's milestone circles (Paul, 5 Oct 2026, 12:25 UTC): "Add a slight expanding zoom for each of the timeline milestones similar to the
// Seals and a small extra info popup over each milestone when hovering." Browser test on the app's own shell and CSS (charm-nest-1.html with its
// scripts taken out) around the REAL LaserReview rail, the real seal zoom engine (charm-nest-motion.js), the real card (charm-nest-rail-tip.js) and
// the real '!' panel; offline, no network, no live data. What it holds to, at 1440 and 390 px:
//  · every circle is a zoom dot with a label (role + aria-label = the card's words) and no listener of its own; the rail's li has no native title;
//  · a resting pointer: nothing for ~300 ms, after ~500 ms the circle has grown slightly (x1.25 to x1.35, nothing shifts, thin ring only, no shadow)
//    and the card shows; leaving puts both back; the card says what the card's "what is left" line says (CharmNestReadiness.explain's own words),
//    for a blocked step and a done step, and who and when for a step the laser finished (only from the sheet's record);
//  · the card is above the circle, flips below at the top edge, stays inside the screen (1440 and 390 px), is never clickable;
//  · the red '!' still opens the Order check panel on a click (no zoom, no card), and no card shows while the panel is open;
//  · a redraw (words changing; a circle replaced by another kind) keeps zoom and card without a gap; 20 redraws add no listener, no timer stays;
//  · Tab focuses a circle (one stop per rail), the card shows at once, the arrows move along the rail, Esc hides both;
//  · a tap shows, a second tap or a tap elsewhere hides (touch); reduced motion: no growth, a fade, the ring;
//  · screenshots (SHOTS=<dir>): a done circle, the red '!', a not-started circle, card showing, at 1440 and 390 px.
//   node tests/charm-nest/rail-milestone-hover.cjs [playwright dir]     (PW_DIR=…, CHROMIUM=…; SHOTS=<dir> saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const F = require('./library-issues-fixture.cjs');
const root = path.join(__dirname, '../..');
let chromium;
const dirs = [process.argv[2], process.env.PW_DIR, '/opt/node22/lib/node_modules/playwright', path.join(root, 'node_modules/playwright'), path.join(root, 'node_modules/playwright-core')].filter(Boolean);
for (const d of dirs) { for (const n of [d, path.join(d, 'playwright-core'), path.join(d, 'playwright')]) { try { ({ chromium } = require(n)); if (chromium) break; } catch (_) {} } if (chromium) break; }
if (!chromium) { console.log('  - no playwright: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const KEYS = ['pooled', 'otherSheetNotReady', 'noSku'];
const WHY = { pooled: 'A piece is not on a saved sheet yet', noSku: 'No SKU', otherSheetNotReady: 'SS Sheet 1: engraving needs approval' };
const CUT_AT = Date.UTC(2026, 9, 3, 14, 5);

// GF Sheet 1: every back saved, a QR label, `k` orders another piece holds back (the Order check step is blocked: the red '!'). SS Sheet 1: its back engravings
// wait for approval (the Engraving step is the gold '!'). GF Sheet 2 (cut by hand of the laser, Paul, 3 Oct 14:05 UTC): every step done.
function records(k) {
  const gf = F.sheet('gf1', { n: 6, metal: 'gold', base: 4170250000, seq: 1, index: 1 });
  const ss = F.sheet('ss1', { n: 6, metal: 'silver', base: 4180250000, seq: 1, index: 1 });
  const cut = F.sheet('gf2', { n: 4, metal: 'gold', base: 4190250000, seq: 2, index: 1, setId: 'set2' });
  cut.laserDoneAt = CUT_AT; cut.laserDoneBy = 'Paul'; cut.processReady = true;
  gf.orders.forEach((o, i) => {
    if (i >= k) return;
    const key = KEYS[i % KEYS.length], other = key === 'otherSheetNotReady';
    gf.orderReadiness[o] = { ready: false, key, why: WHY[key], blocks: [{ key, index: 2, label: 'Charm', poolId: o + '_x_2', lineKey: o + '_x', sheetId: other ? 'ss1' : null, sheetLabel: other ? 'SS Sheet 1' : null, ...(other ? { stage: 'approval' } : {}), why: WHY[key] }], onSheets: ['gf1'], pieceCount: 2, customer: F.NAMES[i % F.NAMES.length], listingId: 'L' + i };
  });
  ss.backPool = [];
  const rows = [...gf.poolIds, ...ss.poolIds, ...cut.poolIds].map((p, i) => ({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: F.NAMES[i % F.NAMES.length] } }, line: { title: 'Charm', listingId: 'L' + i }, state: 'written', poolIds: [p], engrave: p.startsWith('41802') ? { needed: true, state: 'words', approved: false } : { needed: true, state: 'approved', approved: true } }));
  return { gf, ss, cut, rows };
}
async function setup(browser, { width = 1440, height = 900, k = 3, reduce = false, touch = false } = {}) {
  const errors = [];
  const { page, context } = await F.openPage(browser, { width, height, fake: false, errors, touch });
  if (reduce) await page.emulateMedia({ reducedMotion: 'reduce' });
  if (width < 600) await page.evaluate(() => document.getElementById('app').classList.add('railOff'));   // (a phone folds the dark rail: the page's own toggle; this shell has no script to do it)
  page.setDefaultTimeout(8000);
  // count what is added to the page (listeners and timers) before anything is drawn
  await page.evaluate(() => {
    window.__adds = []; const add = EventTarget.prototype.addEventListener;
    EventTarget.prototype.addEventListener = function (t, f, o) { try { window.__adds.push([this.nodeName || (this === window ? 'window' : String(this)), String(t), this.closest ? !!this.closest('.flowBox') : false]); } catch (_) {} return add.call(this, t, f, o); };
    window.__ints = new Set(); const si = window.setInterval, ci = window.clearInterval;
    window.setInterval = (f, ms, ...a) => { const id = si(f, ms, ...a); window.__ints.add(id); return id; }; window.clearInterval = id => { window.__ints.delete(id); return ci(id); };
  });
  const { gf, ss, cut, rows } = records(k);
  await page.evaluate(({ gf, ss, cut, rows, picture }) => {
    window.__sheets = [gf, ss, cut]; window.__rows = rows; gf.preview = ss.preview = cut.preview = picture;
    const L = window.LaserReview, body = document.getElementById('libBody'); L.sections(body); for (const s of [gf, ss, cut]) L.record(s);
    const st = { setId: 'set1', seq: 1, name: 'Set 1', day: '2026-10-03', sheetIds: ['gf1', 'ss1'], status: 'open' }, st2 = { setId: 'set2', seq: 2, name: 'Set 2', day: '2026-10-03', sheetIds: ['gf2'], status: 'open', laserDoneAt: cut.laserDoneAt };
    window.__sets = [st, st2]; const card = window.Sets.libraryCard(st, [gf, ss], [gf, ss]); L.place(card, L.group(st, [gf, ss]).ready, body);
    const card2 = window.Sets.libraryCard(st2, [cut], [cut]); L.place(card2, true, body); L.changed();
  }, { gf, ss, cut, rows, picture: F.sheetPicture() });
  await page.waitForSelector('.flowBox'); await sleep(600);
  return { page, context, errors, gf, ss, cut, rows };
}
const circle = (id, step) => `.flowBox[data-flow-for="sheet:${id}"] .flowDot[data-step="${step}"]`;
// (the circle is brought to the middle of the screen first, so the pointer is really over it; `keep` leaves the scroll where the test put it)
const centre = async (page, sel, keep) => { if (!keep) { await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), sel); await sleep(120); } const b = await (await page.$(sel)).boundingBox(); return { x: b.x + b.width / 2, y: b.y + b.height / 2, b }; };
// how much a circle has grown (drawn size over its layout size) and what the card says
const read = (page, sel) => page.evaluate(sel => {
  const d = document.querySelector(sel), r = d.getBoundingClientRect(), t = document.querySelector('.railTip'), tr = t && t.getBoundingClientRect(), cs = getComputedStyle(d);
  return { k: +(r.width / d.offsetWidth).toFixed(3), ring: cs.boxShadow, filter: cs.filter, shown: !!(t && t.hasAttribute('data-on') && getComputedStyle(t).visibility === 'visible'), opacity: t ? +getComputedStyle(t).opacity : 0,
    tip: t && t.hasAttribute('data-on') ? { name: t.querySelector('b')?.textContent, state: t.querySelector('.rtState')?.textContent, line: t.querySelector('.rtLine')?.textContent || '', by: t.querySelector('.rtBy')?.textContent || '', text: t.textContent.replace(/\s+/g, ' ').trim(), below: t.classList.contains('below'), l: tr.left, t: tr.top, r: tr.right, b: tr.bottom, w: tr.width, h: tr.height, ax: parseFloat(t.style.getPropertyValue('--ax')), pe: getComputedStyle(t).pointerEvents } : null,
    circle: { l: r.left, t: r.top, r: r.right, b: r.bottom }, vw: innerWidth, vh: innerHeight, scrollW: document.documentElement.scrollWidth, bar: (() => { const b = document.querySelector('.topbar'); return b ? b.getBoundingClientRect().bottom : 0; })() };
}, sel);
const layout = page => page.evaluate(() => [...document.querySelectorAll('.flowBox, .librarySheet, [data-laser-card]')].map(n => { const r = n.getBoundingClientRect(); return [Math.round(r.left * 10), Math.round(r.top * 10), Math.round(r.width * 10), Math.round(r.height * 10)].join(','); }).join('|'));
const explainStep = (page, id, key) => page.evaluate(({ id, key }) => { const e = window.LaserReview.explain('sheet', id), s = e.steps.find(x => x.key === key); return { label: s.label, state: s.state, detail: s.detail, current: !!s.current }; }, { id, key });
async function hover(page, sel, ms = 700, keep) { const c = await centre(page, sel, keep); await page.mouse.move(c.x - 40, c.y - 40); await page.mouse.move(c.x, c.y, { steps: 4 }); await sleep(ms); return c; }
async function away(page, ms = 450) { await page.mouse.move(5, 5, { steps: 3 }); await sleep(ms); }
const shot = async (page, name, sel, w) => {
  if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true });
  const m = await read(page, sel), vw = m.vw, vh = m.vh, cx = (m.circle.l + m.circle.r) / 2, x = Math.max(0, Math.min(vw - Math.min(vw, w), cx - w / 2)), top = Math.max(0, (m.tip ? Math.min(m.tip.t, m.circle.t) : m.circle.t) - 14), bottom = Math.min(vh, (m.tip ? Math.max(m.tip.b, m.circle.b) : m.circle.b) + 70);
  await page.screenshot({ path: path.join(SHOTS, name + '.png'), clip: { x, y: top, width: Math.min(vw, w), height: bottom - top } });
};

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    for (const width of [1440, 390]) {
      const tag = `${width}px`, { page, context, errors, ss, rows } = await setup(browser, { width, height: width === 390 ? 844 : 900 });
      await page.evaluate(() => { window.__rail = document.querySelector('.flowBox[data-flow-for="sheet:gf1"]'); });

      // ── 1. what is drawn: five circles per rail (round 13: no Back files, no QR label circle), each a zoom dot with its label, none with a listener of its own, no native title on the step
      { const info = await page.evaluate(() => [...document.querySelectorAll('.flowBox')].map(b => ({ f: b.dataset.flowFor, dots: [...b.querySelectorAll('.flowDot')].map(d => ({ tag: d.tagName, zoom: d.hasAttribute('data-zoom-dot'), role: d.getAttribute('role'), tab: d.getAttribute('tabindex'), label: d.getAttribute('aria-label'), tip: (d.getAttribute('data-tip') || '').split('\n'), step: d.dataset.step })), titles: b.querySelectorAll('li[title], .flowDot[title]').length })));
        assert.equal(info.length, 3, `${tag}: three sheets, three rails`);
        for (const b of info) { assert.equal(b.dots.length, 5, `${tag}: five circles in ${b.f}`); assert.equal(b.titles, 0, `${tag}: no native tooltip on the steps (the card says it)`);
          assert.deepEqual(b.dots.map(d => d.step), ['nesting', 'engraving', 'orders', 'laser', 'completed']);
          for (const d of b.dots) { assert(d.zoom && d.label, `${tag}: ${b.f} ${d.step} is a labelled zoom dot`); assert.equal(d.tip.length, 4); assert.equal(d.label, `${d.tip[0]}. ${d.tip[1]}. ${d.tip[2]}${d.tip[3] ? ` ${d.tip[3]}.` : ''}`, `${tag}: the label is the card's words`);
            assert(['Done', 'Blocked', 'Waiting', 'In progress', 'Not started'].includes(d.tip[1]), `${tag}: a plain state word (${d.tip[1]})`);
            if (d.tag === 'I') { assert.equal(d.role, 'img'); assert(d.tab === '0' || d.tab === '-1'); } else assert.equal(d.tag, 'BUTTON'); }
          assert(b.dots.filter(d => d.tag === 'I' && d.tab === '0').length <= 1, `${tag}: one Tab stop among the plain circles of a rail`); }
        const gf = info.find(b => b.f === 'sheet:gf1').dots.map(d => d.tip[1]);
        assert.deepEqual(gf, ['Done', 'Done', 'Blocked', 'Not started', 'Not started'], `${tag}: GF Sheet 1 reads like the screenshot (two done, the red '!' on Order check, two not started)`);
        const adds = await page.evaluate(() => window.__adds.filter(a => a[2]).length); assert.equal(adds, 0, `${tag}: no listener on anything inside a rail`); }

      // ── 2. a resting pointer on a done circle: nothing at first, then a slight growth and the card; nothing moves; leaving puts both back
      { const sel = circle('gf1', 'nesting'), ex = await explainStep(page, 'gf1', 'nesting');
        assert.equal(ex.state, 'done');
        const c = await centre(page, sel), before = await layout(page); await page.mouse.move(c.x - 40, c.y - 40); await page.mouse.move(c.x, c.y, { steps: 4 }); await sleep(260);
        let m = await read(page, sel); assert.equal(m.shown, false, `${tag}: no card while the pointer has only just arrived`); assert(m.k < 1.02, `${tag}: no growth yet (${m.k})`);
        await sleep(420); m = await read(page, sel);
        assert(m.k >= 1.25 && m.k <= 1.35, `${tag}: grown slightly (${m.k}), about x1.3`); assert.equal(m.shown, true, `${tag}: the card shows with the zoom`); assert.equal(m.filter, 'none', `${tag}: no shadow, a thin ring only`);
        assert.match(m.ring, /rgb/, `${tag}: the thin ring (${m.ring})`); assert(/ 1\.(1|2|3|4)\d*px| 1px /.test(m.ring) || /\b1(\.\d+)?px\b/.test(m.ring), `${tag}: the ring is thin (${m.ring})`);
        assert.equal(await layout(page), before, `${tag}: nothing shifts while a circle is grown (the card, the sheet and the rail keep their places)`);
        assert.equal(m.tip.name, ex.label); assert.equal(m.tip.state, 'Done'); assert.equal(m.tip.line, ex.detail, `${tag}: the card says the step's own line`); assert.doesNotMatch(m.tip.text, /\blines?\b/i, 'pieces, never lines');
        assert.equal(m.tip.pe, 'none', `${tag}: the card takes no press`); assert.equal(await page.evaluate(({ x, y }) => { const t = document.elementFromPoint(x, y); return !!(t && t.closest('.railTip')); }, { x: (m.tip.l + m.tip.r) / 2, y: (m.tip.t + m.tip.b) / 2 }), false, `${tag}: nothing under the card is shadowed by it`);
        assert(m.tip.b <= m.circle.t && !m.tip.below, `${tag}: the card sits above the circle (${m.tip.b} vs ${m.circle.t})`); assert(m.tip.l >= 7.5 && m.tip.r <= m.vw - 7.5 && m.tip.t >= 7.5, `${tag}: inside the screen (${m.tip.l}..${m.tip.r})`); assert(m.scrollW <= m.vw, `${tag}: no sideways scroll`);
        assert(m.tip.ax >= 13.5 && m.tip.ax <= m.tip.w - 13.5, 'the arrow stays on the card');
        await shot(page, `done-circle-${width}`, sel, width === 390 ? 380 : 420);
        await away(page); m = await read(page, sel); assert(m.k < 1.02 && !m.shown, `${tag}: leaving puts the circle and the card back (${m.k}, ${m.shown})`); }

      // ── 3. the red '!' (blocked step): the card gives the card's own reason; a click still opens the Order check panel, with no zoom and no card
      { const sel = circle('gf1', 'orders'), ex = await explainStep(page, 'gf1', 'orders'); assert.equal(ex.state, 'blocked');
        await hover(page, sel, 700); let m = await read(page, sel);
        assert(m.k >= 1.25 && m.k <= 1.35, `${tag}: the '!' grows slightly too (${m.k})`); assert.equal(m.tip.state, 'Blocked'); assert.equal(m.tip.line, ex.detail, `${tag}: the blocked step says the card's reason`); assert.equal(m.tip.name, 'Order check');
        assert.equal(await page.evaluate(sel => document.querySelector(sel).getAttribute('aria-label'), sel), `Order check. Blocked. ${ex.detail}`);
        await shot(page, `red-bang-${width}`, sel, width === 390 ? 380 : 420);
        await page.mouse.down(); await sleep(330); m = await read(page, sel); assert(m.k < 1.05 && !m.shown, `${tag}: a press on the '!' puts the zoom and the card back, the button held down (${m.k})`); await page.mouse.up(); await sleep(450);
        assert.equal(await page.evaluate(() => !!document.querySelector('.lisPanel')), true, `${tag}: the click opens the Order check panel`); assert.equal(await page.evaluate(() => document.querySelector('.lisHead b')?.textContent), 'Order check');
        m = await read(page, sel); assert(m.k < 1.05 && !m.shown, `${tag}: the pressed '!' is not left grown`);
        // while the panel is open no card comes over another circle (no pop-up on a pop-up)
        await hover(page, circle('gf1', 'engraving'), 800); m = await read(page, circle('gf1', 'engraving')); assert.equal(m.shown, false, `${tag}: no card while the panel is open`);
        await page.keyboard.press('Escape'); await sleep(450); assert.equal(await page.evaluate(() => !!document.querySelector('.lisPanel')), false, `${tag}: Esc closes the panel`); await page.evaluate(() => document.activeElement && document.activeElement.blur());   // (Esc gave the '!' the keyboard back: its ring would show in the screenshots below)
        await away(page, 300); await hover(page, circle('gf1', 'engraving'), 700); m = await read(page, circle('gf1', 'engraving')); assert.equal(m.shown, true, `${tag}: the card is back once the panel is closed`); await away(page); }

      // ── 4. a not-started circle: its words; the grey circle grows too
      { const sel = circle('gf1', 'laser'), ex = await explainStep(page, 'gf1', 'laser'); await hover(page, sel, 700); const m = await read(page, sel);
        assert(m.k >= 1.25 && m.k <= 1.35); assert.equal(m.tip.state, 'Not started'); assert.equal(m.tip.line, ex.detail); assert.equal(m.tip.by, '', 'no who and when for a step nobody did');
        await shot(page, `not-started-circle-${width}`, sel, width === 390 ? 380 : 420); await away(page); }

      // ── 5. a done step the laser finished says who and when, from the sheet's own record
      { const sel = circle('gf2', 'laser'); await hover(page, sel, 700); const m = await read(page, sel);
        assert.equal(m.tip.state, 'Done'); assert.match(m.tip.by, /^Paul · Oct 3, \d{1,2}:\d{2}\s?(AM|PM)$/, `${tag}: who and when (${m.tip.by})`);
        await hover(page, circle('gf2', 'nesting'), 700); const n = await read(page, circle('gf2', 'nesting')); assert.equal(n.tip.by, '', 'no who and when for a step the record does not hold one for'); await away(page); }

      // ── 6. the gold '!' (the sheet's own step in progress): its count, the zoom of a button
      { const sel = circle('ss1', 'engraving'), ex = await explainStep(page, 'ss1', 'engraving'); assert.equal(ex.state, 'waiting'); await hover(page, sel, 700); const m = await read(page, sel);
        assert.equal(m.tip.state, 'In progress'); assert.equal(m.tip.line, ex.detail); assert.match(m.tip.line, /back engravings? still needs? approval \(0 of 6 approved\)/); assert(m.k >= 1.25 && m.k <= 1.35); await away(page); }

      // ── 7. keyboard: Tab reaches a rail with one stop, the card shows at once, arrows move along, Esc hides both
      { await page.evaluate(() => { document.activeElement && document.activeElement.blur && document.activeElement.blur(); window.scrollTo(0, 0); });
        let found = ''; for (let i = 0; i < 80 && !found; i++) { await page.keyboard.press('Tab'); found = await page.evaluate(() => document.activeElement && document.activeElement.matches('.flowDot') ? document.activeElement.closest('.flowBox').dataset.flowFor + ':' + document.activeElement.dataset.step : ''); }
        assert(found, `${tag}: Tab reaches a circle`); await sleep(220);
        const sel = (() => { const [f, s] = [found.split(':').slice(0, 2).join(':'), found.split(':')[2]]; return `.flowBox[data-flow-for="${f}"] .flowDot[data-step="${s}"]`; })();
        let m = await read(page, sel); assert.equal(m.shown, true, `${tag}: keyboard focus shows the card at once (${found})`); assert(m.k >= 1.25, `${tag}: and the zoom (${m.k})`);
        // arrows move along the rail, the card follows the focus
        const stepOf = () => page.evaluate(() => document.activeElement && document.activeElement.dataset ? document.activeElement.dataset.step : '');
        const cardName = () => page.evaluate(() => { const t = document.querySelector('.railTip[data-on]'); return t ? t.querySelector('b').textContent : ''; });
        await page.keyboard.press('Home'); await sleep(250); assert.equal(await stepOf(), 'nesting', `${tag}: Home goes to the first circle`); assert.equal(await cardName(), 'Nesting', `${tag}: and the card follows`);
        await page.keyboard.press('ArrowRight'); await sleep(250); assert.equal(await stepOf(), 'engraving', `${tag}: ArrowRight moves along the rail`); assert.equal(await cardName(), 'Engraving');
        await page.keyboard.press('ArrowLeft'); await sleep(200); assert.equal(await stepOf(), 'nesting'); await page.keyboard.press('End'); await sleep(250); assert.equal(await stepOf(), 'completed'); assert.equal(await cardName(), 'Completed');
        await page.keyboard.press('Escape'); await sleep(450); const gone = await page.evaluate(() => ({ shown: !!document.querySelector('.railTip[data-on]'), grown: [...document.querySelectorAll('.flowDot.sealZoomed')].length }));
        assert.deepEqual(gone, { shown: false, grown: 0 }, `${tag}: Esc puts the circle and the card back`); await page.evaluate(() => { document.activeElement && document.activeElement.blur(); for (const n of document.querySelectorAll('*')) if (n.scrollLeft) n.scrollLeft = 0; }); }   // (Tab scrolled a box sideways to bring a circle into view: put it back)

      // ── 8. the card flips below at the top edge, whatever the width
      { await page.evaluate(() => { if (!document.getElementById('railPad')) document.getElementById('libBody').insertAdjacentHTML('beforeend', '<div id="railPad" style="height:1600px"></div>'); const b = document.querySelector('.flowBox[data-flow-for="sheet:gf1"]'); b.scrollIntoView({ block: 'start' }); }); await sleep(300);
        const sel = circle('gf1', 'engraving'); await hover(page, sel, 700, true); const m = await read(page, sel);
        assert.equal(m.shown, true); assert(m.circle.t < 120, `${tag}: the circle is near the top (${m.circle.t})`); assert.equal(m.tip.below, true, `${tag}: no room above, so the card is below`); assert(m.tip.t >= m.circle.b, `${tag}: under the circle (${m.tip.t} vs ${m.circle.b})`);
        assert(m.tip.t >= m.bar + 3, `${tag}: never under the top bar`); assert(m.tip.b <= m.vh - 7.5 && m.tip.l >= 7.5 && m.tip.r <= m.vw - 7.5, `${tag}: inside the screen`);
        await shot(page, `flipped-below-${width}`, sel, width === 390 ? 380 : 420); await away(page); }

      // ── 9. the leftmost and the rightmost circle at 390 px (and 1440): the card never leaves the screen
      for (const step of ['nesting', 'completed']) { const sel = circle('gf1', step); await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), sel); await sleep(250); await hover(page, sel, 700); const m = await read(page, sel);
        assert.equal(m.shown, true); assert(m.tip.l >= 7.5 && m.tip.r <= m.vw - 7.5, `${tag}: the ${step} card is inside the screen (${m.tip.l}..${m.tip.r} of ${m.vw})`); assert(m.scrollW <= m.vw); assert(m.tip.ax >= 13.5 && m.tip.ax <= m.tip.w - 13.5, `${tag}: arrow on the card (${m.tip.ax} of ${m.tip.w})`);
        const cx = (m.circle.l + m.circle.r) / 2; assert(Math.abs(m.tip.l + m.tip.ax - cx) < 1.6 || m.tip.ax <= 14.5 || m.tip.ax >= m.tip.w - 14.5, `${tag}: the arrow points at the circle (${m.tip.l + m.tip.ax} vs ${cx})`); await away(page); }

      // ── 10. a redraw under the pointer: the words change (same element), then a circle is replaced by another kind: zoom and card go on
      { await page.evaluate(() => document.querySelector('.flowBox[data-flow-for="sheet:ss1"]').scrollIntoView({ block: 'center' })); await sleep(300);
        const sel = circle('ss1', 'engraving'); await hover(page, sel, 750); let m = await read(page, sel); assert.equal(m.shown, true); assert(m.k >= 1.25);
        await page.evaluate(sel => { const d = document.querySelector(sel); d.__mine = 1; window.__gaps = []; const t = document.querySelector('.railTip'); const tick = () => { const r = d.getBoundingClientRect(); window.__gaps.push([+(r.width / d.offsetWidth).toFixed(2), t.hasAttribute('data-on') && getComputedStyle(t).opacity > 0.9 ? 1 : 0, d.isConnected ? 1 : 0]); window.__raf = requestAnimationFrame(tick); }; tick(); }, sel);
        const lineBefore = m.tip.line;
        await page.evaluate(() => { window.__rows = window.__rows.map(r => r.key.startsWith('41802') && !window.__done ? (window.__done = 1, { ...r, engrave: { needed: true, state: 'approved', approved: true } }) : r); const ss = window.__sheets.find(x => x.id === 'ss1'); window.LaserReview.record({ ...ss, updatedAt: ss.updatedAt + 1 }); window.LaserReview.changed(); });
        await sleep(900); m = await read(page, sel);
        assert.equal(await page.evaluate(sel => !!document.querySelector(sel).__mine, sel), true, `${tag}: the very same circle after the redraw (patched where it stands, not replaced)`);
        assert.notEqual(m.tip.line, lineBefore, `${tag}: the card follows the new words (${lineBefore} -> ${m.tip.line})`); assert.match(m.tip.line, /\(1 of 6 approved\)/); assert(m.k >= 1.25 && m.shown, `${tag}: zoom and card stay`);
        const g = await page.evaluate(() => { cancelAnimationFrame(window.__raf); return window.__gaps; }); assert(g.length > 20); assert(g.every(x => x[0] >= 1.24 && x[1] === 1 && x[2] === 1), `${tag}: not one frame without the zoom or the card during the redraw (${JSON.stringify(g.filter(x => !(x[0] >= 1.24 && x[1] === 1)).slice(0, 4))})`);
        await away(page);
        // a circle replaced by one of another kind (the plain Engraving circle becomes a '!' button when a saved back file goes missing): the card and the zoom come back to the new one with no pointer move
        const q = circle('gf1', 'engraving'); await page.evaluate(q => document.querySelector(q).scrollIntoView({ block: 'center' }), q); await sleep(300); await hover(page, q, 750); m = await read(page, q); assert.equal(m.shown, true);
        assert.equal(await page.evaluate(q => document.querySelector(q).tagName, q), 'I');
        await page.evaluate(() => { const gf = window.__sheets.find(x => x.id === 'gf1'); gf.backPool = gf.backPool.slice(1); window.LaserReview.record({ ...gf, updatedAt: gf.updatedAt + 1 }); window.LaserReview.changed(); });
        await sleep(1300); assert.equal(await page.evaluate(q => document.querySelector(q).tagName, q), 'BUTTON', `${tag}: the Engraving circle became a '!' button`);
        m = await read(page, q); assert(m.k >= 1.25 && m.shown, `${tag}: zoom and card are on the new circle without moving the pointer (${m.k}, ${m.shown})`); assert.equal(m.tip.state, 'In progress'); assert.equal(m.tip.line, 'Saving back files: 5 of 6.', `${tag}: the plain line`); await away(page); }

      // ── 11. 20 redraws: no listener, no timer is left behind
      { await page.evaluate(() => { window.__marks = { adds: window.__adds.length, ints: window.__ints.size }; });
        for (let i = 0; i < 20; i++) { await page.evaluate(i => { const ss = window.__sheets.find(x => x.id === 'ss1'); ss.placedCount = 6 + (i % 2); window.LaserReview.record({ ...ss, updatedAt: ss.updatedAt + 1 + i, placedCount: 6 + (i % 2) }); window.LaserReview.changed(); }, i); await sleep(90); }
        const after = await page.evaluate(() => ({ adds: window.__adds.length - window.__marks.adds, inRail: window.__adds.filter(a => a[2]).length, ints: window.__ints.size - window.__marks.ints, dots: document.querySelectorAll('.flowDot[data-zoom-dot]').length, boxes: document.querySelectorAll('.flowBox').length }));
        assert.equal(after.inRail, 0, `${tag}: still no listener inside a rail`); assert(after.adds <= 3, `${tag}: no listener growth over 20 redraws (+${after.adds} on the whole page, none from the rail)`); assert.equal(after.ints, 0, `${tag}: no timer left running while idle`);
        assert.equal(after.dots, 15); assert.equal(after.boxes, 3); }

      assert.deepEqual(errors, [], `${tag}: no page errors`); await context.close();
    }

    // ── 12. touch: a tap shows (zoom and card), a second tap or a tap elsewhere hides; a tap on the '!' opens its panel
    { const { page, context, errors } = await setup(browser, { width: 390, height: 844, touch: true });
      const sel = circle('gf1', 'nesting');
      await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), sel); await sleep(250);
      await page.tap(sel); await sleep(450); let m = await read(page, sel); assert.equal(m.shown, true, 'touch: a tap shows the card'); assert(m.k >= 1.25 && m.k <= 1.35, `touch: and the zoom (${m.k})`);
      await page.tap(sel); await sleep(450); m = await read(page, sel); assert(!m.shown && m.k < 1.05, 'touch: a second tap hides it');
      await page.tap(sel); await sleep(400); assert.equal((await read(page, sel)).shown, true); await page.tap('.topbar', { position: { x: 4, y: 4 } }); await sleep(450); m = await read(page, sel); assert(!m.shown && m.k < 1.05, 'touch: a tap elsewhere hides it');
      await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), circle('gf1', 'orders')); await sleep(250);
      await page.tap(circle('gf1', 'orders')); await sleep(500); assert.equal(await page.evaluate(() => !!document.querySelector('.lisPanel')), true, "touch: a tap on the '!' opens its panel"); assert.equal((await read(page, circle('gf1', 'orders'))).shown, false, 'and no card');
      assert.deepEqual(errors, []); await context.close(); }

    // ── 13. reduced motion: the circle does not grow, the card fades in, the ring is the sign
    { const { page, context, errors } = await setup(browser, { reduce: true });
      const sel = circle('gf1', 'nesting'); await centre(page, sel); const before = await layout(page); await hover(page, sel, 700, true); const m = await read(page, sel);
      assert(m.k < 1.02, `reduced motion: no growth (${m.k})`); assert.equal(m.shown, true, 'reduced motion: the card still shows'); assert.match(m.ring, /rgb/, 'reduced motion: the thin ring is the sign');
      assert.equal(await layout(page), before); assert.deepEqual(errors, []); await context.close(); }
  } finally { await browser.close(); }
  console.log('Rail milestone hover OK: every circle a labelled zoom dot with no listener of its own; 500 ms rest then a slight x1.3 growth and the card (above, below at the top edge, inside the screen at 1440 and 390 px, never clickable, thin ring, nothing shifts); the card says the step\'s own words (done, blocked, not started, who and when from the record); the red \'!\' still opens its panel, no card while a panel is open; redraws keep zoom and card (same circle, replaced circle); no listener growth in 20 redraws; keyboard (Tab, arrows, Esc), touch, reduced motion');
})().catch(e => { console.error(e); process.exitCode = 1; });
