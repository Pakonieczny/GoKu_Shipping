// Browser test of the Library's '!' issues panel (charm-nest-library-issues.js) on the app's own shell and CSS (charm-nest-1.html with its
// scripts taken out) around the REAL LaserReview, set card code, CharmNestReadiness.issues and panel; offline listing photos, no network.
// Paul, 5 Oct 2026: "click on the '!' in the progress timeline and have that expand a menu to see the specific issues" · "minimal text and
// maximum visuals" · nothing under the top bar, nothing at the bottom, no pop-up on a pop-up. What it holds to, at 1440 and 390 px wide:
// a sheet with nothing wrong has no '!'; 1, 5 and 40 issues, long customer names; the panel is as wide as its sheet's card at most, never
// under the top bar, never past the bottom, never a horizontal page scroll; it covers its own Approve button whole (no strip of it along
// the edge) and leaves the neighbouring sheet's rail and Approve button alone; one short chip per row, never clipped mid-letter; a row opens
// its order with the page's own hand-off; Esc closes and the '!' gets the keyboard back; a live change keeps it open and updates its rows.
// Round 7 (Paul: "both sheets are in the same set"): a sheet that is done and only waits for a mate sheet of its own set has NO '!' but one quiet clock
// on Laser cutting; its panel says "Waiting for SS Sheet 1 · Engraving" once, uncounted, with a shortcut to that sheet, and fits like every other panel.
//   node tests/charm-nest/library-issues-browser.cjs [playwright-core dir]     (PW_DIR=…, CHROMIUM=…; SHOTS=<dir> saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const F = require('./library-issues-fixture.cjs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium;
try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '';
const KEYS = ['pooled', 'otherSheetNotReady', 'noSku', 'pooled', 'otherSheetNotReady', 'unmatched', 'held', 'noSku'];
const WHY = { pooled: 'A piece is not on a saved sheet yet', noSku: 'No SKU', unmatched: 'SKU not in a master', held: 'Check customer changes', otherSheetNotReady: 'SS Sheet 1: engraving needs approval' };

// a gold sheet with `k` orders another piece holds back, and a silver sheet beside it (engraving not approved when `engraving`)
function records(k, { names = F.NAMES, engraving = true, mateReady = false } = {}) {
  const gf = F.sheet('gf1', { n: Math.max(k, 6), metal: 'gold', base: 4170250000, seq: 1, index: 1 });
  const ss = F.sheet('ss1', { n: 6, metal: 'silver', base: 4180250000, seq: 1, index: 1 });
  gf.orders.forEach((o, i) => {
    if (i >= k) return;
    const key = KEYS[i % KEYS.length], other = key === 'otherSheetNotReady';
    gf.orderReadiness[o] = { ready: false, key, why: WHY[key], blocks: [{ key, index: 2, label: 'Charm', poolId: o + '_x_2', lineKey: o + '_x', sheetId: other ? 'ss1' : null, sheetLabel: other ? 'SS Sheet 1' : null, ...(other ? { stage: 'approval' } : {}), why: WHY[key] }], onSheets: ['gf1'], pieceCount: 2, customer: names[i % names.length], listingId: 'L' + (i % 12) };
  });
  if (engraving) ss.backPool = [];
  const rows = [...gf.poolIds, ...ss.poolIds].map((p, i) => ({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: F.NAMES[i % F.NAMES.length] } }, line: { title: 'Charm', listingId: 'L' + (i % 12) }, state: 'written', poolIds: [p], engrave: engraving && p.startsWith('41802') ? { needed: true, state: 'words', approved: false } : { needed: true, state: 'approved', approved: true } }));
  return { gf, ss, rows };
}
async function setup(browser, k, o = {}) {
  const errors = [], { page, context } = await F.openPage(browser, { width: o.width || 1440, height: o.height || 900, fake: false, errors });
  page.setDefaultTimeout(8000); if (process.env.VERBOSE) console.log('  setup', k, JSON.stringify({ w: o.width, h: o.height }));
  const { gf, ss, rows } = records(k, o);
  await page.evaluate(({ gf, ss, rows, picture, photos }) => {
    window.__sheets = [gf, ss]; window.__rows = rows; window.__photoOf = id => photos[(parseInt(String(id).replace(/\D/g, ''), 10) || 0) % photos.length]; gf.preview = ss.preview = picture;
    const L = window.LaserReview, body = document.getElementById('libBody'); L.sections(body); L.record(gf); L.record(ss);
    const st = { setId: 'set1', seq: 1, name: 'Set 1', day: '2026-10-03', sheetIds: ['gf1', 'ss1'], status: 'open' };
    window.__sets = [st]; const card = window.Sets.libraryCard(st, [gf, ss], [gf, ss]); L.place(card, L.group(st, [gf, ss]).ready, body); L.changed();
  }, { gf, ss, rows, picture: F.sheetPicture(), photos: [...Array(12)].map((_, i) => F.photo(i)) });
  await page.waitForSelector('.flowBox'); await page.waitForTimeout(500);
  return { page, context, errors, gf, ss };
}
const bang = async (page, id, step) => {
  const b = await page.$(`[data-issues-open][data-issues-id="${id}"]${step ? `[data-issues-step="${step}"]` : ''}`);
  if (!b) throw new Error(`no '!' for ${id} ${step}: ` + JSON.stringify(await page.evaluate(() => [...document.querySelectorAll('.flowBox')].map(x => x.dataset.flowFor + ' ' + x.dataset.state + ' ' + [...x.querySelectorAll('.flowStep')].map(t => t.querySelector('span').textContent + ':' + t.className.replace('flowStep ', '') + (t.querySelector('[data-issues-open]') ? '!' : '')).join(' | ')))));
  return b;
};
// everything measured in the page
const measure = page => page.evaluate(() => {
  const p = document.querySelector('.lisPanel'); if (!p) return null;
  const r = el => { const b = el.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
  const bar = document.querySelector('.topbar'), vw = innerWidth, vh = innerHeight;
  const card = id => document.querySelector(`.librarySheet:has([data-flow-for="sheet:${id}"])`) || document.querySelector(`[data-flow-for="sheet:${id}"]`)?.closest('article');
  const covered = el => { if (!el) return null; const b = el.getBoundingClientRect(), pts = [[b.left + 2, b.top + 2], [b.right - 2, b.top + 2], [b.left + 2, b.bottom - 2], [b.right - 2, b.bottom - 2], [b.left + b.width / 2, b.top + b.height / 2]]; return pts.map(([x, y]) => { const t = document.elementFromPoint(x, y); return !!(t && p.contains(t)); }); };
  const hit = el => { if (!el) return null; const b = el.getBoundingClientRect(), t = document.elementFromPoint(b.left + b.width / 2, b.top + b.height / 2); return !!(t && (el === t || el.contains(t))); };
  const gfCard = card('gf1'), ssCard = card('ss1');
  const rows = [...p.querySelectorAll('.lisRow')];
  return { panel: r(p), bar: bar ? r(bar) : null, vw, vh, scrollW: document.documentElement.scrollWidth, bodyScrollW: document.body.scrollWidth, panelScrollW: p.scrollWidth, panelClientW: p.clientWidth,
    gfCard: gfCard ? r(gfCard) : null, ssCard: ssCard ? r(ssCard) : null, up: p.classList.contains('up'),
    gfApprove: covered(gfCard && gfCard.querySelector('.approveBox')), gfFlow: gfCard && gfCard.querySelector('.flowBox') ? r(gfCard.querySelector('.flowBox')) : null, ssApproveHit: hit(ssCard && ssCard.querySelector('.approveBox [data-approve-btn]')), ssBangHit: hit(ssCard && ssCard.querySelector('[data-issues-open]')), gfBangHit: hit(gfCard && gfCard.querySelector('[data-issues-open]')),
    waits: p.querySelectorAll('.lisWait').length, count: p.querySelector('.lisCount')?.textContent || '', countQuiet: !!p.querySelector('.lisCount.quiet'), waitText: [...p.querySelectorAll('.lisWait')].map(x => x.textContent.replace(/\s+/g, ' ').trim()),
    rows: rows.length, groups: p.querySelectorAll('.lisGroup').length, own: p.querySelectorAll('.lisOwn').length, head: p.querySelector('.lisHead b')?.textContent || '', text: p.textContent.replace(/\s+/g, ' '),
    chips: rows.map(x => { const c = x.querySelector('.lisChip'); return c ? { text: c.textContent, clipped: c.scrollWidth > c.clientWidth + 1, ellipsis: getComputedStyle(c).textOverflow === 'ellipsis' } : null; }),
    rowOverflow: rows.some(x => x.scrollWidth > x.clientWidth + 1), whoOverflow: [...p.querySelectorAll('.lisWho')].some(x => x.scrollWidth > x.clientWidth + 1), tileText: [...p.querySelectorAll('.lisTh:not(.lisTile)')].map(x => x.textContent.trim()).filter(Boolean) };
});
const within = (m, why) => {
  const pa = m.panel; assert(pa.l >= 7.5 && pa.r <= m.vw - 7.5, `${why}: inside the screen sideways (${pa.l}..${pa.r} of ${m.vw})`);
  assert(pa.t >= (m.bar ? m.bar.b : 0) + 3.5, `${why}: never under the top bar (${pa.t} vs bar ${m.bar && m.bar.b})`); assert(pa.b <= m.vh - 7.5, `${why}: never past the bottom (${pa.b} of ${m.vh})`);
  assert(m.scrollW <= m.vw && m.bodyScrollW <= m.vw, `${why}: no horizontal page scroll`); assert(m.panelScrollW <= m.panelClientW + 1, `${why}: nothing scrolls sideways inside the panel`);
};
// below its rail: the panel starts under the rail's label line and covers its own Approve button whole (no strip along the edge); above it: it leaves the rail and the button alone
const approveRule = (m, why) => {
  if (m.up) { assert(m.panel.b <= m.gfFlow.t - 2, `${why}: opening upward it stays off the rail (${m.panel.b} vs ${m.gfFlow.t})`); return; }
  assert(m.panel.t >= m.gfFlow.b + 3, `${why}: opening downward it starts below the rail's label line (${m.panel.t} vs ${m.gfFlow.b})`);
  if (m.gfApprove === null) return;   // (a sheet that is done has no Approve button of its own to cover)
  assert(m.gfApprove && m.gfApprove.every(Boolean), `${why}: its own Approve button is covered whole, no strip of it shows (${m.gfApprove})`);
};
const shot = async (page, name, m) => { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '-page.png') }); if (m) { const pa = m.panel; await page.screenshot({ path: path.join(SHOTS, name + '-panel.png'), clip: { x: Math.max(0, pa.l - 28), y: Math.max(0, pa.t - 80), width: Math.min(m.vw, pa.w + 56), height: Math.min(m.vh, pa.h + 110) } }); } };

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    // 0. nothing wrong: no '!' at all, nothing to open
    { const { page, context, errors } = await setup(browser, 0, { engraving: false });
      const seen = await page.evaluate(() => [...document.querySelectorAll('[data-issues-open]')].map(b => b.dataset.issuesId + ':' + b.dataset.issuesStep));
      assert.deepEqual(seen, [], 'a sheet with nothing holding it back has no \'!\''); assert.equal(await page.evaluate(() => document.querySelectorAll('.lisPanel').length), 0);
      await shot(page, 'issues-0'); assert.deepEqual(errors, []); await context.close(); }

    // 1. one issue: the panel grows from the '!' of the Order check step; a short row; the hand-off; Esc
    { const { page, context, errors } = await setup(browser, 1);
      const b = await bang(page, 'gf1', 'orders'); assert(b, 'the Order check step carries the \'!\''); assert(await bang(page, 'ss1', 'engraving'), 'and so does the silver sheet\'s Engraving step');
      await b.click(); await page.waitForTimeout(500);
      const m = await measure(page); assert(m, 'the panel is open'); within(m, '1 issue'); assert.equal(m.rows, 1); assert.equal(m.head, 'Order check');
      assert(m.panel.w <= m.gfCard.w + 1, `no wider than its sheet's card (${m.panel.w} vs ${m.gfCard.w})`); assert(m.panel.l >= m.gfCard.l - 1 && m.panel.r <= m.gfCard.r + 1, 'inside its own card sideways, so the next sheet stays in view');
      assert.equal(m.ssBangHit, true, 'the next sheet\'s \'!\' is not covered'); assert.equal(m.ssApproveHit, true, 'nor its Approve button');
      approveRule(m, '1 issue');
      assert.deepEqual(m.tileText, [], 'no status words in the picture tiles'); assert(!/\blines?\b/i.test(m.text), 'pieces, never lines');
      await shot(page, 'issues-1', m);
      assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.closest('.lisPanel') !== null), true, 'the keyboard is inside the panel');
      const rid = await page.evaluate(() => document.querySelector('.lisRow').dataset.id); assert.match(rid, /^\d{6,}$/);
      await page.evaluate(() => { window.__calls.length = 0; }); await page.click('.lisRow'); await page.waitForTimeout(300);
      assert.deepEqual((await page.evaluate(() => window.__calls[0])).slice(0, 2), ['order', rid], 'a row opens its order with the page\'s own hand-off');
      assert.equal(await page.evaluate(() => document.querySelector('.lisPanel').classList.contains('handed')), true, 'the panel steps aside (no pop-up on a pop-up)');
      // the order window opens, then closes (Esc): the panel is back where it was, with the keyboard inside it
      await page.evaluate(() => { const win = document.createElement('dialog'); win.setAttribute('open', ''); document.body.appendChild(win); window.__win = win; });
      await page.evaluate(() => { const win = window.__win; win.removeAttribute('open'); win.dispatchEvent(new Event('close')); win.remove(); }); await page.waitForTimeout(500);
      assert.equal(await page.evaluate(() => { const lp = document.querySelector('.lisPanel'); return !!lp && !lp.classList.contains('handed'); }), true, 'when the order window closes the panel is back');
      await page.keyboard.press('Escape'); await page.waitForTimeout(500);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.lisPanel').length), 0, 'Esc closes'); assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.hasAttribute('data-issues-open')), true, 'and the \'!\' has the keyboard again');
      assert.deepEqual(errors, []); await context.close(); }

    // 2. five issues; the live update keeps it open
    { const { page, context, errors, gf } = await setup(browser, 5);
      await (await bang(page, 'gf1', 'orders')).click(); await page.waitForTimeout(500);
      let m = await measure(page); within(m, '5 issues'); assert.equal(m.rows, 5); assert.equal(m.groups, 0);
      assert(m.chips.every(c => c && c.text.split(' ').length <= 6 && !c.clipped), `one short chip each, never clipped: ${JSON.stringify(m.chips)}`); approveRule(m, '5 issues');
      assert.equal(m.ssBangHit, true); assert.equal(m.ssApproveHit, true); await shot(page, 'issues-5', m);
      const first = Object.keys(gf.orderReadiness).find(o => gf.orderReadiness[o].ready === false);
      await page.evaluate(({ gf, first }) => { gf.orderReadiness[first] = { ready: true }; window.__sheets = window.__sheets.map(x => x.id === 'gf1' ? gf : x); window.LaserReview.record(gf); window.LaserReview.changed(); }, { gf, first }); await page.waitForTimeout(700);
      m = await measure(page); assert(m, 'the panel stays open through a live change'); assert.equal(m.rows, 4, 'and one row fewer: the order that is no longer held back'); within(m, 'after the live change');
      await page.keyboard.press('Escape'); await page.waitForTimeout(300); assert.deepEqual(errors, []); await context.close(); }

    // 3. forty issues: folded groups, nothing taller than the screen
    { const { page, context, errors } = await setup(browser, 40);
      await (await bang(page, 'gf1', 'orders')).click(); await page.waitForTimeout(600);
      let m = await measure(page); within(m, '40 issues'); assert(m.groups >= 3 && m.groups <= 9, `grouped by reason (${m.groups})`); assert.equal(m.rows, 0, 'folded: counts and pictures, not forty rows');
      assert(m.panel.h <= m.vh - 16 - (m.bar ? m.bar.b : 0), 'never taller than the screen'); await shot(page, 'issues-40', m);
      await page.click('.lisGroup'); await page.waitForTimeout(500); m = await measure(page); within(m, '40 issues, a group open'); assert(m.rows >= 3 && m.rows <= 4, `a group opens to a few rows (${m.rows})`);
      assert(m.chips.length === 0 || m.chips.every(c => !c || !c.clipped)); await shot(page, 'issues-40-open', m);
      await page.keyboard.press('Escape'); await page.waitForTimeout(300); assert.deepEqual(errors, []); await context.close(); }

    // 4. long customer names stay on one line, the chip is never cut mid-letter
    { const { page, context, errors } = await setup(browser, 5, { names: ['Maximiliana Alexandria von Habsburg-Lothringen-Esterházy'] });
      await (await bang(page, 'gf1', 'orders')).click(); await page.waitForTimeout(500);
      const m = await measure(page); within(m, 'long names'); assert.equal(m.rowOverflow, false, 'a long name never pushes the row wider'); assert.equal(m.whoOverflow, false, 'the number and the name are one short line');
      assert(m.chips.every(c => c && !c.clipped), `chips are whole: ${JSON.stringify(m.chips)}`); await shot(page, 'issues-long', m); await context.close(); assert.deepEqual(errors, []); }

    // 5. a narrow screen (390 px): inside the screen, no sideways scroll, the same panel
    for (const k of [5, 40]) { const { page, context, errors } = await setup(browser, k, { width: 390, height: 844 });
      await (await bang(page, 'gf1', 'orders')).click(); await page.waitForTimeout(600);
      let m = await measure(page); within(m, `narrow, ${k} issues`); assert(m.panel.w <= 390 - 16 + 0.5); if (k === 5) assert.equal(m.rows, 5); await shot(page, `issues-narrow-${k}`, m);
      assert(m.chips.every(c => !c || !c.clipped), 'chips are whole on a narrow screen'); await page.keyboard.press('Escape'); await page.waitForTimeout(300); assert.deepEqual(errors, []); await context.close(); }

    // 6. the Engraving step: just the one link, no counts, the Engraving tab opens
    { const { page, context, errors } = await setup(browser, 3);
      await (await bang(page, 'ss1', 'engraving')).click(); await page.waitForTimeout(500);
      let m = await measure(page); within(m, 'engraving'); assert.equal(m.rows, 0, 'no rows'); assert.equal(m.own, 1); assert.match(m.text, /Open engraving approvals/); assert.doesNotMatch(m.text.replace(/Engraving|Open engraving approvals|\d+ issue/gi, ''), /\d/, 'no counts');
      await shot(page, 'issues-engraving', m);
      await page.evaluate(() => { window.__calls.length = 0; }); await page.click('.lisOwn'); await page.waitForTimeout(400);
      assert.deepEqual(await page.evaluate(() => window.__calls[0]), ['mode', 'engrave']); assert.deepEqual(errors, []); await context.close(); }

    // 8. round 7: the set's wait. GF Sheet 1 is done, SS Sheet 1 (same set) still has back engravings to approve: GF has a quiet clock, never a '!'
    for (const width of [1440, 390]) { const { page, context, errors } = await setup(browser, 0, { width, height: width === 390 ? 844 : 900 });
      const seen = await page.evaluate(() => [...document.querySelectorAll('[data-issues-open]')].map(b => b.dataset.issuesId + ':' + b.dataset.issuesStep + (b.hasAttribute('data-issues-quiet') ? ':quiet' : '') + (b.classList.contains('flowBang') ? ':bang' : '')).sort());
      assert.deepEqual(seen, ['gf1:laser:quiet', 'ss1:engraving:bang'], `${width}px: the done sheet shows a quiet clock, only the sheet with real work left shows a '!' (${seen})`);
      const qb = await page.evaluate(() => { const b = document.querySelector('[data-issues-id="gf1"][data-issues-quiet]'), cs = getComputedStyle(b), r = b.getBoundingClientRect(); return { text: b.textContent.trim(), svg: !!b.querySelector('svg path'), label: b.getAttribute('aria-label'), w: r.width, h: r.height, bg: cs.backgroundColor, border: cs.borderTopColor, step: b.closest('.flowStep').className }; });
      assert.equal(qb.text, '', 'a clock, not an exclamation mark'); assert(qb.svg, 'the clock is drawn'); assert.match(qb.label, /waits for another sheet/i); assert(qb.w >= 15 && qb.w <= 17 && qb.h >= 15 && qb.h <= 17, 'the size of the rail\'s other dots');
      assert.doesNotMatch(qb.border, /176, 86, 63/, 'not the clay of a problem'); assert.match(qb.step, /\bcurrent\b/);
      await (await bang(page, 'gf1', 'laser')).click(); await page.waitForTimeout(500);
      const m = await measure(page); assert(m, 'the quiet clock opens the panel'); within(m, `set wait ${width}px`); approveRule(m, `set wait ${width}px`);
      assert.equal(m.rows, 0, 'no issue row'); assert.equal(m.waits, 1, 'the wait is said once'); assert.equal(m.waitText.length, 1); assert.match(m.waitText[0], /^Waiting for SS Sheet 1 · Engraving ?(\d+ \/ \d+)?$/); assert.equal(m.head, 'Laser cutting');
      assert.equal(m.count, 'Waiting', 'a quiet header, not an issue count'); assert.equal(m.countQuiet, true); assert.doesNotMatch(m.text, /\bissues?\b|\bWaits on\b|\blines?\b/i); if (width > 600) assert.equal(m.ssBangHit, true, 'the next sheet\'s \'!\' is not covered');   // (on a narrow screen the sheets stack: the panel lies over the one below, as it does for every panel)
      assert.equal(m.panel.h < 150, true, `a quiet panel is small (${m.panel.h})`);
      await shot(page, `issues-set-wait-${width}`, m);
      await page.evaluate(() => { window.__calls.length = 0; }); await page.click('.lisWait'); await page.waitForTimeout(300);
      assert.deepEqual((await page.evaluate(() => window.__calls[0])).slice(0, 2), ['sheet', 'ss1'], 'the wait row opens the sheet that holds the set');
      await page.evaluate(() => { const lp = document.querySelector('.lisPanel'); if (lp) lp.classList.remove('handed'); window.LibraryIssues.close(); }); await page.waitForTimeout(300);
      // the sheet that holds the set is fixed: the clock and its wait are gone
      await page.evaluate(() => { const ss = window.__sheets.find(x => x.id === 'ss1'); window.__rows = window.__rows.map(r => ({ ...r, engrave: { needed: true, state: 'approved', approved: true } })); window.LaserReview.record(ss); window.LaserReview.changed(); }); await page.waitForTimeout(700);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('[data-issues-open]')].map(b => b.dataset.issuesId + ':' + b.dataset.issuesStep)), ['ss1:backFiles'], "the engravings are approved: GF Sheet 1's clock is gone (SS Sheet 1 has its own back files left, a real '!' on its own rail)");
      assert.deepEqual(errors, []); await context.close(); }

    // 7. near the bottom of the screen the panel opens upward; near the top bar it stays below it
    for (const height of [640, 560]) { const { page, context, errors } = await setup(browser, 5, { height });
      await (await bang(page, 'gf1', 'orders')).click(); await page.waitForTimeout(500);
      const m = await measure(page); within(m, `short screen ${height}`); assert.deepEqual(errors, []); await context.close(); }
  } finally { await browser.close(); }
  console.log('Library issues browser OK: no \'!\' when nothing is wrong; 1, 5 and 40 issues, long names, 390 px, Engraving link; inside the screen, under no bar, never past the bottom, no sideways scroll; as wide as its card at most, over its own Approve button whole, the next sheet untouched; short whole chips; hand-off, Esc, live update');
})().catch(e => { console.error(e); process.exitCode = 1; });
