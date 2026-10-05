// The sheet drop-down on a Nest card (Paul, 5 Oct: "Add drop down menu for the Sheets just like the new back engraving menu").
// A card's sheets were a tab each in its controls row; beside Cut Sheet / Options / the back-engraving control / Front | Back they ran
// out of room and were clipped ("Sheet 2" cut off). They are now ONE pill (charm-nest-sheet-menu.js, window.SheetMenu, the
// back-engraving control's own look) that names the sheet on the card with the same mark its tab had and opens a menu of every sheet.
//  1 · the component alone (jsdom; skipped without it): the pill's name and mark, a chip with one sheet, update() keeps what did not change,
//      the API and the event shape, destroy;
//  2 · the real Nest page (Chromium): the pill shows the current sheet and its tab's mark, and never clips its name at 320 / 480 / 900 /
//      1440 px; the menu lists every sheet with the tabs' facts; a pick switches the card exactly as the old tab did (same state, same
//      redraws); Esc / outside / second press; the keyboard; the menu stays inside the screen (and opens upward when there is no room);
//      the controls row is as tall as it was with tabs; it survives renderCard / refreshAllCards; it follows the sheet list live while
//      open; a six-sheet card, a long list and a one-sheet card; a modal dialog layer; without the script the old tabs stay.
//   NODE_PATH=<jsdom node_modules> PW_DIR=<playwright node_modules> node tests/charm-nest/sheet-menu.cjs   (CHROMIUM=<chrome>)
//   SHM_SHOTS=<dir> also writes the before / after pictures there.
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const SRC = fs.readFileSync(path.join(root, 'charm-nest-sheet-menu.js'), 'utf8');
const tick = (n = 30) => new Promise(r => setTimeout(r, n));
const PNG_1 = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');

/* ── 1 · the component ── */
async function component() {
  let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('  – no jsdom (set NODE_PATH): the component checks were not run'); return; }
  const dom = new JSDOM('<!doctype html><body><div id="rowA"></div><div id="rowB"></div></body>', { url: 'http://localhost/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document; w.eval(SRC); const SM = w.SheetMenu;
  assert(SM && ['mount', 'closeAll', 'isOpen', 'rows'].every(f => typeof SM[f] === 'function') && SM.EVENT === 'sheetmenu', 'the API is there');
  const events = []; d.addEventListener('sheetmenu', e => events.push(e.detail));
  const A = d.getElementById('rowA'), B = d.getElementById('rowB');
  const items = [{ id: 'a', label: 'Sheet 1', mark: '✓ 77', active: true, title: 'Gold 14K · sheet 1' }, { id: 'b', label: 'Sheet 2', mark: '12', state: 'waiting' }, { id: 'c', label: 'Sheet 3', spin: true, spinLabel: 'Nesting' }];
  const picks = [];
  const mA = SM.mount(A, { key: 'A', label: 'Sheets of Gold 14K', items, onPick: (id, it) => picks.push([id, it.label]) });
  const pill = () => A.querySelector('.shMenu'), btn = () => A.querySelector('button.shmBtn');
  assert(pill().classList.contains('owSeg'), 'the engraving control\'s own pill class');
  assert.equal(btn().querySelector('.shmName').textContent, 'Sheet 1', 'the current sheet\'s name'); assert.equal(btn().querySelector('.shmMark').textContent, '✓ 77', 'and its tab\'s mark');
  assert.equal(btn().getAttribute('aria-haspopup'), 'menu'); assert.equal(btn().getAttribute('aria-expanded'), 'false'); assert(btn().querySelector('.shmChev'), 'a chevron');
  assert.match(btn().getAttribute('aria-label'), /^Sheet 1, ✓ 77\. Choose a sheet/); assert.equal(btn().type, 'button'); assert.equal(A.querySelector('.shMenu').dataset.sheets, '3');
  assert.doesNotMatch(btn().title + btn().getAttribute('aria-label'), /\blines?\b/i, 'pieces, never lines');
  assert.equal(mA.button, btn(), 'the button is exposed');
  // the current sheet moves: the pill follows, nothing else is rebuilt
  const nameNode = btn().querySelector('.shmName');
  mA.update(items.map(i => Object.assign({}, i, { active: i.id === 'c' })));
  assert.equal(btn().querySelector('.shmName').textContent, 'Sheet 3'); assert(btn().querySelector('.shmSpin'), 'a spinner while it nests'); assert.equal(btn().querySelector('.shmSpin').getAttribute('aria-label'), 'Nesting', 'a labelled spinner');
  assert.equal(btn().querySelector('.shmName'), nameNode, 'the same node is rewritten, not rebuilt');
  const html0 = pill().innerHTML; mA.update(items.map(i => Object.assign({}, i, { active: i.id === 'c' }))); assert.equal(pill().innerHTML, html0, 'the same list again changes nothing');
  // one sheet: a plain chip, no menu, no chevron; two again: the pill
  mA.update([{ id: 'a', label: 'Sheet 1', mark: '✓ 50', active: true }]);
  assert.equal(A.querySelector('button'), null, 'a one-sheet card has no button'); assert(A.querySelector('.shmChip'), 'a plain chip'); assert.equal(A.querySelector('.shmChev'), null, 'no chevron: nothing to choose');
  assert.equal(A.querySelector('.shmChip').textContent.trim(), 'Sheet 1✓ 50'); assert.equal(mA.open(), false, 'nothing to open');
  mA.update(items); assert(btn() && btn().querySelector('.shmChev'), 'a second sheet brings the pill back in place');
  // a bad list never throws
  mA.update(null); mA.update([{}, null, { id: 1, label: 'x' }, { id: 1, label: 'dup' }]); mA.update(items);
  // a second mount, independent
  const mB = SM.mount(B, { key: 'B', items: [{ id: 'x', label: 'Sheet 1', active: true }, { id: 'y', label: 'Sheet 2' }] });
  assert.equal(B.querySelector('.shmName').textContent, 'Sheet 1'); assert.equal(SM.isOpen(), false);
  // no layout in jsdom: open() declines (a hidden pill cannot anchor a menu) and says nothing
  assert.equal(mA.open(), false); assert.equal(events.length, 0, 'no event for a menu that did not open');
  mA.destroy(); assert.equal(A.querySelector('.shMenu'), null, 'destroy takes the pill away'); mB.destroy();
  assert.equal(w.localStorage.length + w.sessionStorage.length, 0, 'nothing written to storage');
  console.log('  ✓ component: the pill names the current sheet with its tab\'s mark, a chip for one sheet, update keeps what did not change, destroy');
  w.close();
}

/* ── 2 · the real Nest page ── */
async function page() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shots = process.env.SHM_SHOTS || '';
  const errors = [];
  try {
    // `old`: the script left out (how the card was before, and what stays when it fails to load)
    const open = async (opts = {}) => {
      const context = await browser.newContext({ viewport: { width: opts.width || 1440, height: opts.height || 900 } });
      if (opts.old) await context.route(u => /charm-nest-sheet-menu\.js/.test(u.href), r => r.fulfill({ status: 200, contentType: 'text/javascript', body: '/* the sheet menu left out: the tabs as they were */' }));
      if (opts.reduce) await context.addInitScript(() => { const mm = window.matchMedia.bind(window); window.matchMedia = q => /prefers-reduced-motion/.test(q) ? { matches: true, media: q, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {}, onchange: null } : mm(q); });
      await context.route(u => /\/eng-back-\d+\.png/.test(u.href), r => r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }, body: PNG_1 }));
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
      const pg = await context.newPage(); pg.setDefaultTimeout(20000);
      pg.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
      await pg.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await pg.waitForFunction(() => window.CN && window.Engrave && window.CharmNestBacks && CN.S.cloud.ok === true, null, { timeout: 60000 });
      return { context, pg };
    };
    // Five cards, every kind of tab: 14K with three sheets (complete, partial, waiting), Gold with ONE sheet, Silver with SIX (complete, partial,
    // empty, nesting, waiting, complete), 10K with fourteen (a long day), Rose with two.
    const seed = async pg => pg.evaluate(origin => {
      const MM = 72 / 25.4, sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [6 * MM, 0]], ['l', [6 * MM, 6 * MM]], ['l', [0, 6 * MM]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 6 * MM, 6 * MM] };
      let seq = 0, nb = 0;
      const fill = (pgx, sheetId, n, status, placed, backs) => {
        const ids = Array.from({ length: n }, (_, i) => `${sheetId}c${i}`);
        pgx.sheetId = sheetId; pgx.status = status; pgx.dirty = false; pgx.poolIds = ids.map(i => `${i}_1_1`);
        pgx.charms = ids.map((id, i) => ({ id, name: `41737${++seq} · TAG`, poolId: `${id}_1_1`, order: `41737${seq}`, sku: 'TAG', centerPt: [3 * MM, 3 * MM], widthPt: 6 * MM, heightPt: 6 * MM, areaPt2: 36 * MM * MM, outline: sq, members: [sq] }));
        const k = placed == null ? (status === 'complete' ? n : 0) : placed;
        pgx.placements = ids.slice(0, k).map((id, i) => ({ id, cxPt: (10 + (i % 8) * 9) * MM, cyPt: (12 + Math.floor(i / 8) * 9) * MM, angle: 0, wPt: 6 * MM, hPt: 6 * MM }));
        // (the first sheet of a card may carry back engraving: the control beside the pill in the row, as in the cards Paul's picture showed)
        if (backs) pgx.backPool = ids.slice(0, backs).map((id, i) => ({ poolId: `${id}_1_1`, order: pgx.charms[i].order, sku: 'TAG', copy: 1, text: 'For ' + id, approvedAt: Date.now() - 1000 - i, previewWPt: 12 * MM, previewHPt: 12 * MM, preview: `${origin}/eng-back-${nb++}.png` }));
      };
      const add = (metal, specs) => {
        const prim = S.sheets[metal];
        specs.forEach((sp, i) => { const pgx = i === 0 ? prim : Object.assign(makeSheet(metal, i + 1), {}); fill(pgx, `shm-${metal}-${i + 1}`, ...sp); if (i) prim.pages.push(pgx); });
      };
      add('gold14k', [[3, 'complete', null, 3], [4, 'partial', 2], [5, 'ready']]);
      add('gold', [[4, 'complete']]);
      add('silver', [[12, 'complete', null, 4], [11, 'partial', 9], [0, 'idle'], [6, 'nesting'], [7, 'ready'], [8, 'complete']]);
      add('gold10k', Array.from({ length: 14 }, (_, i) => [2 + i % 4, 'complete', null, i ? 0 : 2]));
      add('rose', [[3, 'complete'], [2, 'ready']]);
      CN.setMode('nest');
      for (const m of ['gold14k', 'gold', 'silver', 'gold10k', 'rose']) { for (const p of S.sheets[m].pages) p.el = null; S.sheets[m].active = 0; S.sheets[m].pages[0].el = S.sheets[m].cardEl; }
      refreshAllCards();
    }, srv.sorterOrigin);
    // a width: the page's rail (344px of ladder) takes the whole of a narrow screen, so below 900px a person folds it away (its own button), as here
    const fit = async (pg, w, h = 900) => { await pg.setViewportSize({ width: w, height: h }); await pg.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w); await tick(320); };
    const card = m => `.sheetCard[data-m="${m}"]`;
    // what a card's controls row shows of its sheets (new pill or old tabs)
    const look = (pg, metal) => pg.evaluate(m => {
      const c = document.querySelector(`.sheetCard[data-m="${m}"]`), host = c.querySelector('[data-r="tabs"]'), row = c.querySelector('.shControls'), r = host.getBoundingClientRect();
      const wrap = host.querySelector('.shMenu'), btn = host.querySelector('button.shmBtn'), name = host.querySelector('.shmName'), mark = host.querySelector('.shmMark, .shmSpin'), chev = host.querySelector('.shmChev');
      const cr = c.getBoundingClientRect(), rr = row.getBoundingClientRect(), wr = wrap && wrap.getBoundingClientRect(), nr = name && name.getBoundingClientRect(), chr = chev && chev.getBoundingClientRect();
      // (a row too narrow for all its controls goes on to a second line, left aligned: only a control on the pill's own line can be run under)
      const vis = [...row.children].filter(x => !x.hidden && x.getClientRects().length && !x.classList.contains('hidden')), mid = e => { const b = e.getBoundingClientRect(); return b.top + b.height / 2; };
      const sibs = vis.filter(x => x !== host && Math.abs(mid(x) - mid(host)) < 10).map(x => x.getBoundingClientRect());
      const nRows = vis.map(mid).sort((a, b) => a - b).reduce((n, v, i, a) => n + (i && v - a[i - 1] > 10 ? 1 : 0), vis.length ? 1 : 0);
      const oldTabs = [...host.querySelectorAll('button[data-i]')];
      return { rowH: rr.height, nRows, hostW: Math.round(r.width), pill: !!wrap, chip: !!host.querySelector('.shmChip'), btn: !!btn, text: wrap ? wrap.innerText.trim().replace(/\s+/g, ' ') : oldTabs.map(b => `${b.firstChild.textContent} ${b.querySelector('[data-st]').textContent}`.trim()).join(' | '),
        name: name && name.textContent, mark: mark && (mark.classList.contains('shmSpin') ? 'spin:' + mark.getAttribute('aria-label') : mark.textContent), sheets: host.dataset.sheets || String(oldTabs.length),
        wrapL: wr && Math.round(wr.left), wrapR: wr && Math.round(wr.right), wrapW: wr && Math.round(wr.width), wrapH: wr && wr.height, cardL: Math.round(cr.left), cardR: Math.round(cr.right), rowL: Math.round(rr.left), rowR: Math.round(rr.right),
        nameW: nr && Math.round(nr.width), nameFull: name ? name.scrollWidth <= name.clientWidth + 1 : null, nameEll: name ? getComputedStyle(name).textOverflow === 'ellipsis' : null,
        chevIn: chr && wr ? chr.right <= wr.right + 1 && chr.left >= wr.left - 1 && chr.width > 0 : null, chevR: chr && Math.round(chr.right), sibL: sibs.length ? Math.round(Math.min(...sibs.map(s => s.left))) : null, sibN: sibs.length,
        expanded: btn && btn.getAttribute('aria-expanded'), label: btn && btn.getAttribute('aria-label'), oldClipped: oldTabs.length ? host.scrollWidth > host.clientWidth + 1 : null, oldCount: oldTabs.length, docOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth };
    }, metal);
    const panel = pg => pg.evaluate(() => {
      const p = document.querySelector('.shmPanel:not([inert])'); if (!p) return null;
      const r = p.getBoundingClientRect(), bar = document.querySelector('.topbar'), list = p.querySelector('.shmList');
      return { l: Math.round(r.left), r: Math.round(r.right), t: Math.round(r.top), b: Math.round(r.bottom), h: Math.round(r.height), vw: innerWidth, vh: innerHeight, barB: bar ? Math.round(bar.getBoundingClientRect().bottom) : 0, up: p.classList.contains('up'), opacity: getComputedStyle(p).opacity,
        parent: p.parentElement.tagName, position: getComputedStyle(p).position, role: p.getAttribute('role'), label: p.getAttribute('aria-label'), scrolls: list.scrollHeight > list.clientHeight + 1, listH: list.clientHeight, docOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        rows: [...p.querySelectorAll('.shmRow:not(.gone)')].map(x => ({ id: x.dataset.id, text: x.innerText.trim().replace(/\s+/g, ' '), checked: x.getAttribute('aria-checked'), role: x.getAttribute('role'), state: x.dataset.state, label: x.getAttribute('aria-label'), h: Math.round(x.getBoundingClientRect().height), inside: x.getBoundingClientRect().left >= r.left - 1 && x.getBoundingClientRect().right <= r.right + 1 })),
        focus: document.activeElement && document.activeElement.classList.contains('shmRow') ? document.activeElement.innerText.trim().replace(/\s+/g, ' ') : null };
    });
    const pillSel = m => `${card(m)} [data-r="tabs"] button.shmBtn`;
    const settle = pg => pg.waitForFunction(() => !document.getAnimations().some(a => a.playState === 'running' && a.effect && a.effect.target && a.effect.target.matches && a.effect.target.matches('.shmPanel,.shmRow')));
    const gone = pg => pg.waitForFunction(() => !document.querySelector('.shmPanel'));
    // what a card is, after a sheet was chosen (the same for a tab and for the menu): which page it shows, what is drawn, the controls beside it
    const snap = (pg, metal) => pg.evaluate(m => {
      const prim = S.sheets[m], c = prim.cardEl, cv = c.querySelector('[data-r="canvas"]'), q = r => c.querySelector(`[data-r="${r}"]`);
      return { active: prim.active, elOn: prim.pages.map(p => p.el === c), cardSheet: activePage(m).sheetId, head: q('prov').textContent, pill: q('pill').textContent, pillHidden: q('pill').hidden, hint: q('hint').className, nest: q('nest').textContent, nestHidden: q('nest').classList.contains('hidden'),
        stop: q('stop').classList.contains('hidden'), dl: q('dl').classList.contains('hidden'), face: q('face').hidden, eng: q('eng').hidden, queue: q('queue').children.length, canvas: [cv.width, cv.height], gate: q('gate').className + '|' + q('gate').textContent.trim(), rejects: q('rejects').className,
        backs: q('backs').getAttribute('data-eng'), rowH: c.querySelector('.shControls').getBoundingClientRect().height };
    }, metal);

    // ── before: how the cards were (the script left out: the old tabs), then the same cards with the menu
    const old = await open({ old: true });
    assert.equal(await old.pg.evaluate(() => typeof window.SheetMenu), 'undefined', 'the script is left out');
    await seed(old.pg); await tick(500);
    const before = {}; for (const m of ['gold14k', 'gold', 'silver', 'gold10k', 'rose']) before[m] = await look(old.pg, m);
    assert(before.gold14k.oldCount === 3 && before.silver.oldCount === 6, `without the script the old tabs stay (${before.gold14k.oldCount} and ${before.silver.oldCount})`);
    assert.equal(before.gold14k.pill, false, 'no pill'); assert.equal(await old.pg.evaluate(() => document.querySelectorAll('.shMenu').length), 0);
    // the old tab click, kept as the reference for "a pick does exactly what a tab did"
    const oldSnaps = {};
    await old.pg.click(`${card('silver')} .shTabs button[data-i="4"]`); await tick(250); oldSnaps.silver4 = await snap(old.pg, 'silver');
    await old.pg.click(`${card('silver')} .shTabs button[data-i="0"]`); await tick(250); oldSnaps.silver0 = await snap(old.pg, 'silver');
    await old.pg.click(`${card('gold14k')} .shTabs button[data-i="1"]`); await tick(250); oldSnaps.gold14k1 = await snap(old.pg, 'gold14k');
    const oldWide = {}; for (const w of [320, 480, 900, 1440]) { await fit(old.pg, w, 900); oldWide[w] = {}; for (const m of ['gold14k', 'gold', 'silver', 'gold10k', 'rose']) oldWide[w][m] = await look(old.pg, m); }
    const oldHtml = await old.pg.evaluate(() => document.querySelector('.sheetCard[data-m="silver"] .shControls').innerHTML.length);
    assert(oldHtml > 0);
    if (shots) {
      await fit(old.pg, 1440, 900);
      await old.pg.evaluate(() => { S.sheets.silver.active = 0; showPage('silver', 0); });
      await (await old.pg.$(card('silver'))).screenshot({ path: path.join(shots, 'sheet-menu-nest-before-silver.png') });
      for (const m of ['gold14k', 'silver', 'gold']) { await old.pg.evaluate(mm => showPage(mm, 0), m); await (await old.pg.$(card(m) + ' .shControls')).screenshot({ path: path.join(shots, `sheet-menu-nest-row-${m}-before.png`) }); }
      await fit(old.pg, 480, 900);
      for (const m of ['silver', 'gold14k']) await (await old.pg.$(card(m))).screenshot({ path: path.join(shots, `sheet-menu-nest-before-${m}-480.png`) });
      await fit(old.pg, 320, 900); for (const m of ['silver', 'gold14k']) { await old.pg.evaluate(mm => showPage(mm, 0), m); await (await old.pg.$(card(m) + ' .shControls')).screenshot({ path: path.join(shots, `sheet-menu-nest-row-${m}-before-320.png`) }); }
    }
    await old.context.close();
    console.log(`  ✓ page: without the script the old tabs stay (${before.silver.oldCount} tabs on the six-sheet card; the row ${before.silver.rowH}px high)`);

    const { context, pg } = await open();
    await seed(pg); await tick(500);
    const M = ['gold14k', 'gold', 'silver', 'gold10k', 'rose'];
    // 1 · the pill names the current sheet with the tab's mark
    let g = {}; for (const m of M) g[m] = await look(pg, m);
    assert.equal(g.gold14k.pill, true); assert.equal(g.gold14k.btn, true); assert.equal(g.gold14k.name, 'Sheet 1'); assert.equal(g.gold14k.mark, '✓ 3', 'the tab\'s own mark: a check and the count'); assert.equal(g.gold14k.sheets, '3');
    assert.equal(g.gold14k.expanded, 'false'); assert.match(g.gold14k.label, /^Sheet 1, complete, 3 placed\. Choose a sheet, 3 in this list$/);
    assert.equal(g.silver.name, 'Sheet 1'); assert.equal(g.silver.mark, '✓ 12'); assert.equal(g.silver.sheets, '6'); assert.equal(g.gold10k.sheets, '14');
    // a one-sheet card keeps what it showed: a plain chip with its mark, no menu, no chevron, nothing to press
    assert.equal(g.gold.chip, true, 'one sheet: a chip'); assert.equal(g.gold.btn, false, 'not a button'); assert.equal(g.gold.name, 'Sheet 1'); assert.equal(g.gold.mark, '✓ 4'); assert.equal(g.gold.chevIn, null, 'no chevron');
    assert(g.gold.wrapW <= before.gold.wrapW || before.gold.oldCount === 1, 'the chip takes no more room than the tab did');
    // the same facts the old tab showed, sheet by sheet (the tab text was "Sheet 1✓ 77"): the pill's name + mark are the tab's text
    for (const m of M) assert.equal(g[m].text, before[m].text.split(' | ')[0], `${m}: the pill says what the first tab said (${g[m].text} / ${before[m].text.split(' | ')[0]})`);
    console.log('  ✓ page: the pill names the current sheet with its tab\'s mark; a one-sheet card keeps a plain chip');

    // 2 · never clips its name; the row as tall as with tabs; at 320 / 480 / 900 / 1440
    const widths = {};
    for (const w of [320, 480, 900, 1440]) {
      await fit(pg, w, 900);
      widths[w] = {};
      for (const m of M) {
        const x = widths[w][m] = await look(pg, m), o = oldWide[w][m];
        // one row: as tall as with tabs; a row too narrow for every control wraps to two rows, never clips (Options is on the title line now)
        if (x.nRows === 1) assert.equal(x.rowH, o.rowH, `${m} @${w}px: the controls row is ${x.rowH}px, and was ${o.rowH}px with tabs`);
        else assert(x.nRows === 2 && x.rowH > o.rowH && x.rowH <= 2 * o.rowH + 2 && w <= 480, `${m} @${w}px: a row that wraps is two rows high (${x.rowH}px, ${x.nRows} rows)`);
        assert(x.wrapL >= x.cardL && x.wrapR <= x.cardR + 1, `${m} @${w}px: the pill is inside its card (${x.wrapL}-${x.wrapR} in ${x.cardL}-${x.cardR})`);
        assert(x.wrapR <= x.rowR + 1 || x.sibN > 0, `${m} @${w}px: the pill is inside the row`);
        if (x.btn) assert.equal(x.chevIn, true, `${m} @${w}px: the chevron is whole, inside the pill (${x.chevR})`);
        assert(x.nameFull || x.nameEll, `${m} @${w}px: a name that shrinks says so with an ellipsis`);
        if (x.sibL != null && x.wrapR > x.sibL + 1 && x.rowR - x.rowL >= 300) assert.fail(`${m} @${w}px: the pill runs under the next control (${x.wrapR} > ${x.sibL})`);
        assert(x.wrapH <= x.rowH, `${m} @${w}px: the pill fits the row (${x.wrapH} in ${x.rowH})`);
        assert.equal(x.docOverflow <= 0, true, `${m} @${w}px: no sideways scroll (${x.docOverflow})`);
        if (w >= 900) assert.equal(x.nameFull, true, `${m} @${w}px: the whole name "${x.name}" is shown`);
      }
      const clippedBefore = M.filter(m => oldWide[w][m].oldClipped).length;
      console.log(`  ✓ page @${w}px: ${M.map(m => `${m} ${widths[w][m].wrapW}px${widths[w][m].nameFull ? '' : ' (name shortened)'}`).join(', ')}; the old tab row was cut off on ${clippedBefore} of ${M.length} cards; rows ${widths[w].silver.rowH}px as before${M.some(m => widths[w][m].nRows > 1) ? ' (wrapped to a second row on ' + M.filter(m => widths[w][m].nRows > 1).join(', ') + ')' : ''}`);
    }
    await fit(pg, 1440, 900);
    // the pill is the engraving control's pill: the same height, border, radius and chevron
    const sameFamily = await pg.evaluate(() => {
      const c = document.querySelector('.sheetCard[data-m="silver"]'), a = c.querySelector('.shMenu'), b = c.querySelector('.owSeg.shFace');
      const cs = x => getComputedStyle(x);
      return { aBorder: cs(a).borderTopWidth + cs(a).borderTopStyle + cs(a).borderTopColor, bBorder: cs(b).borderTopWidth + cs(b).borderTopStyle + cs(b).borderTopColor, aRad: cs(a).borderTopLeftRadius, bRad: cs(b).borderTopLeftRadius, aBg: cs(a).backgroundColor, bBg: cs(b).backgroundColor, aPad: cs(a).padding, bPad: cs(b).padding, aH: a.getBoundingClientRect().height, hasToggle: !!window.EngravingToggle };
    });
    assert.equal(sameFamily.aBorder, sameFamily.bBorder, 'the same border as the order view\'s switch'); assert.equal(sameFamily.aRad, sameFamily.bRad); assert.equal(sameFamily.aBg, sameFamily.bBg); assert.equal(sameFamily.aPad, sameFamily.bPad);
    console.log(`  ✓ page: the pill is an .owSeg like Front | Back (border, radius, background and padding equal), ${sameFamily.aH}px high`);

    // 3 · open: every sheet, with the facts the tabs showed; the current one marked
    await pg.click(pillSel('silver')); await settle(pg);
    let p = await panel(pg);
    assert(p, 'the menu opens'); assert.equal(p.role, 'menu'); assert.equal(p.label, 'Sheets of Sterling silver'); assert.equal(p.parent, 'BODY', 'a fixed layer on the page body'); assert.equal(p.position, 'fixed'); assert.equal(p.opacity, '1');
    assert.deepEqual(p.rows.map(r => r.text), ['Sheet 1 ✓ 12', 'Sheet 2 ⚠ 9/11', 'Sheet 3 0', 'Sheet 4', 'Sheet 5 7', 'Sheet 6 ✓ 8'], 'every sheet of the card with the mark its tab had: ' + JSON.stringify(p.rows.map(r => r.text)));
    assert(p.rows.every(r => r.role === 'menuitemradio' && r.inside), 'menuitemradio rows, inside the menu'); assert.deepEqual(p.rows.map(r => r.checked), ['true', 'false', 'false', 'false', 'false', 'false'], 'the current one is marked');
    assert.deepEqual(p.rows.map(r => r.state), ['complete', 'partial', 'waiting', 'nesting', 'waiting', 'complete']);
    assert.match(p.rows[3].label, /^Sheet 4, nesting$/); assert.equal(await pg.evaluate(() => document.querySelectorAll('.shmPanel .shmSpin[aria-label="Nesting"]').length), 1, 'the nesting sheet has a labelled spinner');
    assert.match(p.rows[1].label, /^Sheet 2, partial, 9 of 11 placed$/); assert.match(p.rows[2].label, /^Sheet 3, 0 pieces waiting$/); assert.doesNotMatch(JSON.stringify(p.rows), /\blines?\b/i, 'pieces, never lines');
    assert.equal(p.focus, 'Sheet 1 ✓ 12', 'the keyboard is on the current sheet'); assert.equal(await pg.getAttribute(pillSel('silver'), 'aria-expanded'), 'true'); assert.equal(await pg.getAttribute(pillSel('silver'), 'aria-haspopup'), 'menu');
    assert.equal(p.docOverflow <= 0, true, 'no sideways scroll');
    assert(p.t >= p.barB + 4 && p.b <= p.vh - 4 && p.l >= 4 && p.r <= p.vw - 4, `inside the screen: ${JSON.stringify({ l: p.l, r: p.r, t: p.t, b: p.b, barB: p.barB })}`);
    const origin = await pg.evaluate(() => { const p = document.querySelector('.shmPanel'), b = document.querySelector('.sheetCard[data-m="silver"] .shMenu').getBoundingClientRect(), r = p.getBoundingClientRect(); return { dx: Math.abs(r.left - b.left), dy: r.top - b.bottom, ox: p.style.getPropertyValue('--ox') }; });
    assert(origin.dx <= 2 && origin.dy >= 4 && origin.dy <= 14, `the menu hangs from the pill: ${JSON.stringify(origin)}`);
    if (shots) { await pg.screenshot({ path: path.join(shots, 'sheet-menu-nest-open-silver.png'), clip: await pg.evaluate(() => { const c = document.querySelector('.sheetCard[data-m="silver"]').getBoundingClientRect(); return { x: Math.max(0, Math.floor(c.left) - 6), y: Math.max(0, Math.floor(c.top) - 6), width: Math.ceil(c.width) + 12, height: 360 }; }) }); }
    console.log('  ✓ page: the menu lists all six sheets with the tabs\' marks (check + count, partial, spinner), the current one marked and focused; a fixed layer, inside the screen');

    // 4 · picking switches the card exactly as the old tab did
    await pg.click(`.shmPanel .shmRow[data-id] >> nth=4`); await gone(pg); await tick(250);
    const s4 = await snap(pg, 'silver'); assert.deepEqual(s4, oldSnaps.silver4, 'a pick = the old tab: the same card state'); assert.equal(s4.active, 4);
    assert.equal(await pg.evaluate(() => document.activeElement && document.activeElement.classList.contains('shmBtn')), true, 'focus is back on the pill');
    let l = await look(pg, 'silver'); assert.equal(l.name, 'Sheet 5'); assert.equal(l.mark, '7'); assert.equal(l.expanded, 'false');
    await pg.click(pillSel('silver')); await settle(pg); p = await panel(pg); assert.deepEqual(p.rows.map(r => r.checked), ['false', 'false', 'false', 'false', 'true', 'false'], 'the current one moved'); assert.equal(p.focus, 'Sheet 5 7');
    await pg.click(`.shmPanel .shmRow >> nth=0`); await gone(pg); await tick(250); assert.deepEqual(await snap(pg, 'silver'), oldSnaps.silver0, 'back to sheet 1: the old tab\'s card state');
    await pg.click(pillSel('gold14k')); await settle(pg); await pg.click(`.shmPanel .shmRow >> nth=1`); await gone(pg); await tick(250); assert.deepEqual(await snap(pg, 'gold14k'), oldSnaps.gold14k1, '14K sheet 2: the old tab\'s card state');
    l = await look(pg, 'gold14k'); assert.equal(l.name, 'Sheet 2'); assert.equal(l.mark, '⚠ 2/4');
    await pg.evaluate(() => showPage('gold14k', 0)); await tick(150); assert.equal((await look(pg, 'gold14k')).name, 'Sheet 1', 'showPage (what any other code calls) moves the pill');
    // the fly / pulse target the tab used to be: the pill now
    assert.equal(await pg.evaluate(() => { const el = S.sheets.silver.cardEl; return sheetTabEl(el, S.sheets.silver.pages[2]) === el._sheetMenu.button; }), true, 'the flight to "the sheet they wait on" lands on the pill');
    console.log('  ✓ page: a pick leaves the card exactly as the old tab did (state, preview, controls, focus on the pill); showPage moves the pill');

    // 5 · Esc, outside, second press
    const isOpen = () => pg.evaluate(() => !!document.querySelector('.shmPanel:not([inert])') && SheetMenu.isOpen());
    const events = []; await pg.evaluate(() => { window.__ev = []; document.addEventListener('sheetmenu', e => window.__ev.push(e.detail)); });
    await pg.click(pillSel('silver')); await settle(pg); assert.equal(await isOpen(), true);
    await pg.keyboard.press('Escape'); await gone(pg); assert.equal(await pg.evaluate(() => document.activeElement.classList.contains('shmBtn')), true, 'Esc: focus back on the pill');
    await pg.click(pillSel('silver')); await settle(pg);
    await pg.click(pillSel('silver')); await gone(pg); assert.equal(await pg.evaluate(() => document.activeElement.classList.contains('shmBtn')), true, 'a second press closes (and does not reopen)'); await tick(300); assert.equal(await isOpen(), false);
    await pg.click(pillSel('silver')); await settle(pg);
    await pg.mouse.click(700, 8 + (await pg.evaluate(() => document.querySelector('.topbar').getBoundingClientRect().bottom))); await gone(pg); assert.equal(await isOpen(), false, 'a press outside closes');
    // an outside press elsewhere does what it would have done (the menu does not swallow it): a press on another card's pill closes this one and opens that one
    await pg.click(pillSel('silver')); await settle(pg); await pg.click(pillSel('gold14k')); await settle(pg);
    assert.equal(await pg.evaluate(() => document.querySelectorAll('.shmPanel:not([inert])').length), 1, 'one menu at a time'); assert.equal((await panel(pg)).label, 'Sheets of 14K solid gold');
    await pg.keyboard.press('Escape'); await gone(pg);
    const ev = await pg.evaluate(() => window.__ev.map(e => `${e.key}:${e.open}:${e.source}${e.picked ? ':' + e.picked : ''}`));
    assert(ev.includes('nest:silver:true:button') && ev.includes('nest:silver:false:escape') && ev.includes('nest:silver:false:button') && ev.includes('nest:silver:false:outside') && ev.includes('nest:gold14k:true:button'), 'the sheetmenu event says which and why: ' + ev.join(' '));
    console.log('  ✓ page: Esc, a second press and a press outside close it (focus back on the pill for the first two); one menu at a time; the event reports each');

    // 6 · the keyboard
    await pg.focus(pillSel('silver')); await pg.keyboard.press('Enter'); await settle(pg); p = await panel(pg); assert(p, 'Enter opens'); assert.equal(p.focus, 'Sheet 1 ✓ 12');
    await pg.keyboard.press('ArrowDown'); assert.equal((await panel(pg)).focus, 'Sheet 2 ⚠ 9/11'); await pg.keyboard.press('End'); assert.equal((await panel(pg)).focus, 'Sheet 6 ✓ 8');
    await pg.keyboard.press('ArrowDown'); assert.equal((await panel(pg)).focus, 'Sheet 1 ✓ 12', 'Down wraps'); await pg.keyboard.press('ArrowUp'); assert.equal((await panel(pg)).focus, 'Sheet 6 ✓ 8', 'Up wraps'); await pg.keyboard.press('Home'); assert.equal((await panel(pg)).focus, 'Sheet 1 ✓ 12');
    await pg.keyboard.press('ArrowDown'); await pg.keyboard.press('ArrowDown'); await pg.keyboard.press('Enter'); await gone(pg); await tick(200);
    assert.equal((await look(pg, 'silver')).name, 'Sheet 3', 'Enter picks'); assert.equal(await pg.evaluate(() => document.activeElement.classList.contains('shmBtn')), true);
    await pg.keyboard.press('ArrowDown'); await settle(pg); assert.equal((await panel(pg)).focus, 'Sheet 3 0', 'Down on the pill opens it, on the current sheet');
    await pg.keyboard.press('ArrowUp'); await pg.keyboard.press('Space'); await gone(pg); await tick(200); assert.equal((await look(pg, 'silver')).name, 'Sheet 2', 'Space picks');
    await pg.keyboard.press('Space'); await settle(pg); assert(await panel(pg), 'Space opens'); await pg.keyboard.press('Tab'); await gone(pg); assert.equal(await pg.evaluate(() => !document.activeElement.classList.contains('shmRow') && document.activeElement !== document.body), true, 'Tab closes the menu and moves on');
    await pg.evaluate(() => showPage('silver', 0)); await tick(200);
    console.log('  ✓ page: keyboard: Enter / Space / Down open it on the current sheet; Up, Down (wrapping), Home, End move; Enter and Space pick; Tab closes');

    // 7 · inside the screen, at every width, from the top and the bottom of the page; open upward when there is no room
    for (const w of [320, 480, 900, 1440]) {
      await fit(pg, w, 900);
      for (const m of ['silver', 'gold10k']) {
        await pg.evaluate(mm => document.querySelector(`.sheetCard[data-m="${mm}"]`).scrollIntoView({ block: 'start' }), m); await tick(150);
        await pg.click(pillSel(m)); await settle(pg); p = await panel(pg);
        assert(p.l >= 4 && p.r <= p.vw - 4 && p.t >= p.barB + 4 && p.b <= p.vh - 4, `${m} @${w}px: inside the screen ${JSON.stringify({ l: p.l, r: p.r, t: p.t, b: p.b, vw: p.vw, barB: p.barB })}`); assert(p.docOverflow <= 0, `${m} @${w}px: no sideways scroll`);
        await pg.keyboard.press('Escape'); await gone(pg);
      }
    }
    await fit(pg, 900, 420);
    // a short screen: the long list scrolls inside itself and stays whole inside the screen; its pill near the bottom: it opens upward
    await pg.evaluate(() => { const c = document.querySelector('.sheetCard[data-m="gold10k"]'); c.scrollIntoView({ block: 'start' }); }); await tick(150);
    await pg.click(pillSel('gold10k')); await settle(pg); p = await panel(pg);
    assert(p.rows.length === 14 && p.scrolls && p.b <= p.vh - 4 && p.t >= p.barB + 4, `14 sheets on a short screen scroll inside the menu: ${JSON.stringify({ t: p.t, b: p.b, vh: p.vh, scrolls: p.scrolls, listH: p.listH })}`);
    await pg.keyboard.press('End'); const endFocus = await panel(pg); assert.equal(endFocus.focus.startsWith('Sheet 14'), true, 'End reaches the last of fourteen'); assert.equal(await pg.evaluate(() => { const r = document.activeElement.getBoundingClientRect(), l = document.querySelector('.shmList').getBoundingClientRect(); return r.top >= l.top - 1 && r.bottom <= l.bottom + 1; }), true, 'and scrolls it into view');
    await pg.keyboard.press('Escape'); await gone(pg);
    assert.equal(p.up, false, 'with room below the pill it opens downward');
    // a card lower in the page, brought (by the page's own scroller) to 40px above the bottom of a short screen: no room below, so it opens upward
    await fit(pg, 900, 330);
    const bring = (metal, below) => pg.evaluate(([m, gap]) => { const c = document.querySelector(`.sheetCard[data-m="${m}"]`); let n = c; while (n && n !== document.body) { if (/(auto|scroll)/.test(getComputedStyle(n).overflowY) && n.scrollHeight > n.clientHeight) break; n = n.parentElement; } const b = c.querySelector('.shMenu').getBoundingClientRect(); if (n && n !== document.body) n.scrollTop += b.bottom - (innerHeight - gap); const a = c.querySelector('.shMenu').getBoundingClientRect(); return { top: Math.round(a.top), bottom: Math.round(a.bottom), vh: innerHeight }; }, [metal, below]);
    const posn = await bring('rose', 40); await tick(150);
    assert(Math.abs(posn.vh - posn.bottom - 40) <= 3, `the pill is brought near the bottom of the screen (${JSON.stringify(posn)})`);
    await pg.click(pillSel('rose')); await settle(pg); p = await panel(pg);
    assert.equal(p.up, true, `no room below: it opens upward (${JSON.stringify({ posn, t: p.t, b: p.b })})`); assert(p.b <= posn.top && p.t >= p.barB + 4, 'above the pill, under the top bar'); assert.equal(p.rows.length, 2);
    await pg.keyboard.press('Escape'); await gone(pg);
    // the same with fourteen sheets: there is room neither way, so it takes the larger side and scrolls inside itself
    const posn2 = await bring('gold10k', 60); await tick(150);
    await pg.click(pillSel('gold10k')); await settle(pg); p = await panel(pg);
    assert(p.t >= p.barB + 4 && p.b <= p.vh - 4 && p.scrolls && p.rows.length === 14, `fourteen sheets with little room: whole inside the screen, scrolling inside (${JSON.stringify({ posn2, t: p.t, b: p.b, vh: p.vh, listH: p.listH })})`);
    await pg.keyboard.press('Escape'); await gone(pg);
    console.log('  ✓ page: inside the screen at 320 / 480 / 900 / 1440 px; on a 420px screen fourteen sheets scroll inside the menu; with room below it opens downward; with the pill 40px from the bottom of a 330px screen it opens upward under the top bar, and fourteen sheets there still fit the screen');
    await fit(pg, 1440, 900);

    // 8 · it survives renderCard / refreshAllCards, and follows the list live while open
    await pg.evaluate(() => showPage('silver', 0)); await tick(200);
    await pg.click(pillSel('silver')); await settle(pg);
    await pg.evaluate(() => { window.__panel = document.querySelector('.shmPanel'); window.__row = document.querySelector('.shmRow'); window.__btn = document.querySelector('.sheetCard[data-m="silver"] .shmBtn'); });
    await pg.keyboard.press('ArrowDown'); await pg.keyboard.press('ArrowDown');
    await pg.evaluate(() => { renderCard(activePage('silver')); refreshAllCards(); renderTabs(activePage('silver')); tabMarks(activePage('silver')); });
    await tick(100);
    const same = await pg.evaluate(() => ({ panel: window.__panel === document.querySelector('.shmPanel') && window.__panel.isConnected, row: window.__row === document.querySelector('.shmRow'), btn: window.__btn === document.querySelector('.sheetCard[data-m="silver"] .shmBtn'), open: SheetMenu.isOpen('nest:silver'), focus: document.activeElement.innerText.trim().replace(/\s+/g, ' ') }));
    assert.deepEqual(same, { panel: true, row: true, btn: true, open: true, focus: 'Sheet 3 0' }, 'a redraw of the card keeps the open menu, its rows, the focus and the very pill');
    // a mark changes while it is open: the row rewrites itself, nothing is rebuilt
    await pg.evaluate(() => { const p = S.sheets.silver.pages[2]; p.charms.push(...S.sheets.silver.pages[4].charms.slice(0, 3).map((c, i) => Object.assign({}, c, { id: 'live' + i, poolId: 'live' + i + '_1_1' }))); refreshAllCards(); });
    await tick(100); p = await panel(pg); assert.equal(p.rows[2].text, 'Sheet 3 3', 'a mark that changed rewrites itself while open'); assert.equal(await pg.evaluate(() => window.__row === document.querySelector('.shmRow')), true, 'without rebuilding the rows');
    await pg.evaluate(() => { const p = S.sheets.silver.pages[3]; p.status = 'complete'; p.placements = p.charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 10, cyPt: 20, angle: 0, wPt: 10, hPt: 10 })); tabMarks(activePage('silver')); });
    await tick(100); p = await panel(pg); assert.equal(p.rows[3].text, 'Sheet 4 ✓ 6', 'the sheet that was nesting is done: its spinner became its mark'); assert.equal(p.rows[3].state, 'complete'); assert.equal(await pg.evaluate(() => document.querySelectorAll('.shmPanel .shmSpin').length), 0);
    // a sheet is added while open: it slides in at its place
    await pg.evaluate(() => { const pgx = addPage('silver'); pgx.charms = S.sheets.silver.pages[4].charms.slice(0, 2).map((c, i) => Object.assign({}, c, { id: 'add' + i, poolId: 'add' + i + '_1_1' })); pgx.status = 'ready'; refreshAllCards(); });
    await tick(300); p = await panel(pg); assert.equal(p.rows.length, 7, 'a new sheet joins the open menu'); assert.equal(p.rows[6].text, 'Sheet 7 2'); assert.equal(await pg.evaluate(() => window.__panel === document.querySelector('.shmPanel')), true, 'the menu stayed open, in place');
    assert.equal((await look(pg, 'silver')).sheets, '7'); assert.equal(await isOpen(), true);
    // a sheet is removed while open (the focused one): it folds away, the focus moves to its neighbour, the rest stays
    await pg.keyboard.press('End'); assert.equal((await panel(pg)).focus, 'Sheet 7 2');
    await pg.evaluate(() => { removePage(S.sheets.silver.pages[6]); refreshAllCards(); });
    await pg.waitForFunction(() => !document.querySelector('.shmRow.gone'), null, { timeout: 5000 }); p = await panel(pg); assert.equal(p.rows.length, 6, 'a removed sheet leaves the open menu'); assert(p.focus && /^Sheet 6/.test(p.focus), `the focus moved to its neighbour (${p.focus})`);
    // removing the shown sheet: the card shows its neighbour; the menu follows (the current mark moves)
    await pg.evaluate(() => { showPage('silver', 5); }); await tick(200); p = await panel(pg); assert.equal(p.rows[5].checked, 'true'); assert.equal((await look(pg, 'silver')).name, 'Sheet 6');
    await pg.evaluate(() => { removePage(S.sheets.silver.pages[5]); }); await tick(450); p = await panel(pg); assert.equal(p.rows.length, 5); assert.equal(p.rows[4].checked, 'true', 'the current mark follows to the sheet now shown'); assert.equal((await look(pg, 'silver')).name, 'Sheet 5');
    // down to one sheet while open: the menu closes (nothing to choose) and the pill is a plain chip
    await pg.evaluate(() => { for (const x of S.sheets.silver.pages.slice(1).reverse()) removePage(x); refreshAllCards(); }); await tick(450);
    assert.equal(await isOpen(), false, 'with one sheet left the menu closes'); l = await look(pg, 'silver'); assert.equal(l.chip, true, 'and the pill is a chip'); assert.equal(l.btn, false);
    await pg.evaluate(() => { const x = addPage('silver'); x.charms = []; x.status = 'idle'; refreshAllCards(); }); await tick(200); l = await look(pg, 'silver'); assert.equal(l.btn, true, 'a second sheet turns the chip into the pill again'); assert.equal(l.sheets, '2');
    // the card is drawn anew (buildCards): the menu does not outlive its pill
    await pg.click(pillSel('silver')); await settle(pg); assert.equal(await isOpen(), true);
    await pg.evaluate(() => { document.querySelector('.sheetCard[data-m="silver"] [data-r="tabs"]').remove(); }); await gone(pg); assert.equal(await pg.evaluate(() => document.querySelectorAll('.shmPanel').length), 0, 'a pill that leaves the page takes its menu with it');
    console.log('  ✓ page: an open menu survives renderCard / refreshAllCards (same panel, rows, focus); marks rewrite in place, a spinner becomes its mark, a sheet slides in, one folds away with the focus moving on, the current mark follows, one sheet left closes it, and a pill that leaves takes its menu');

    // 9 · a modal dialog: the layer goes into the open dialog (a modal makes everything outside it inert)
    await pg.evaluate(() => {
      const dlg = document.createElement('dialog'); dlg.id = 'shmDlg'; dlg.style.cssText = 'width:420px;height:220px;padding:16px'; const host = document.createElement('div'); dlg.appendChild(host); document.body.appendChild(dlg); dlg.showModal();
      window.__dlgMenu = SheetMenu.mount(host, { key: 'dlg', label: 'Sets', items: [{ id: 'a', label: 'GF 1', color: '#c8a24e', sub: 'Set 1', badge: '4', mark: 'freed room', current: true }, { id: 'b', label: 'GF 2', color: '#8d95a0', sub: 'Set 2', badge: '2' }, { id: 'c', label: 'SS 1', color: '#c08578', disabled: true }], onPick: id => { window.__dlgPicked = id; } });
    });
    await pg.click('#shmDlg .shmBtn'); await settle(pg);
    const dl = await pg.evaluate(() => { const p = document.querySelector('.shmPanel'), r = p && p.getBoundingClientRect(); return { inDialog: !!p && p.parentElement.id === 'shmDlg', visible: !!p && document.elementFromPoint(r.left + r.width / 2, r.top + 20) && p.contains(document.elementFromPoint(r.left + r.width / 2, r.top + 20)), rows: p ? [...p.querySelectorAll('.shmRow')].map(x => x.innerText.trim().replace(/\s+/g, ' ')) : [], dots: p ? p.querySelectorAll('.shmDot').length : 0, subs: p ? p.querySelectorAll('.shmSub').length : 0, disabled: p ? p.querySelectorAll('[aria-disabled="true"]').length : 0, pill: document.querySelector('#shmDlg .shmBtn').innerText.trim().replace(/\s+/g, ' ') }; });
    assert.equal(dl.inDialog, true, 'the layer goes into the modal dialog'); assert.equal(dl.visible, true, 'and is on top and reachable'); assert.deepEqual(dl.rows, ['GF 1 Set 1 4 freed room', 'GF 2 Set 2 2', 'SS 1'], JSON.stringify(dl.rows)); assert.equal(dl.dots, 3); assert.equal(dl.subs, 2); assert.equal(dl.disabled, 1); assert.equal(dl.pill, 'GF 1 4 freed room');
    await pg.click('.shmPanel .shmRow >> nth=2', { force: true }); await tick(150); assert.equal(await pg.evaluate(() => window.__dlgPicked || null), null, 'a disabled row is not picked'); assert.equal(await isOpen(), true);
    await pg.click('.shmPanel .shmRow >> nth=1'); await gone(pg); assert.equal(await pg.evaluate(() => window.__dlgPicked), 'b');
    await pg.evaluate(() => { window.__dlgMenu.open(); }); await settle(pg); await pg.keyboard.press('Escape'); await gone(pg); assert.equal(await pg.evaluate(() => document.getElementById('shmDlg').open), true, 'Esc closes the menu, not the dialog under it');
    await pg.evaluate(() => { window.__dlgMenu.destroy(); document.getElementById('shmDlg').close(); document.getElementById('shmDlg').remove(); });
    // with a layer given
    console.log('  ✓ page: inside a modal dialog the layer goes into the dialog (on top, reachable), with a dot, a second line, a badge and a note per row; a disabled row is not picked; Esc closes the menu only');

    // 10 · a six-sheet card and a one-sheet card in the card's own words, and nothing asked of the network by the menu
    const reqs = []; pg.on('request', r => { if (/\/\.netlify\/functions\//.test(r.url())) reqs.push(r.url()); });
    const n0 = reqs.length; await pg.click(pillSel('gold14k')); await settle(pg); await pg.keyboard.press('ArrowDown'); await pg.keyboard.press('Escape'); await gone(pg);
    assert.equal(reqs.length, n0, 'opening and closing the menu asks the network for nothing');
    assert.equal(await pg.evaluate(() => { let n = 0; for (const st of [localStorage, sessionStorage]) for (let i = 0; i < st.length; i++) if (/sheetmenu|shm/i.test(st.key(i))) n++; return n; }), 0, 'nothing about it in the browser storage');

    // 11 · pictures
    if (shots) {
      await fit(pg, 1440, 900); await pg.evaluate(() => { for (const x of S.sheets.silver.pages) x.el = null; });
      await pg.evaluate(() => location.reload()); await pg.waitForFunction(() => window.CN && CN.S.cloud.ok === true, null, { timeout: 60000 }); await seed(pg); await tick(500);
      await (await pg.$(card('silver'))).screenshot({ path: path.join(shots, 'sheet-menu-nest-after-silver.png') });
      for (const m of ['gold14k', 'silver', 'gold']) { await pg.evaluate(mm => showPage(mm, 0), m); await (await pg.$(card(m) + ' .shControls')).screenshot({ path: path.join(shots, `sheet-menu-nest-row-${m}-after.png`) }); }
      await fit(pg, 480, 900);
      for (const m of ['silver', 'gold14k']) await (await pg.$(card(m))).screenshot({ path: path.join(shots, `sheet-menu-nest-after-${m}-480.png`) });
      await fit(pg, 320, 900); for (const m of ['silver', 'gold14k']) { await pg.evaluate(mm => showPage(mm, 0), m); await (await pg.$(card(m) + ' .shControls')).screenshot({ path: path.join(shots, `sheet-menu-nest-row-${m}-after-320.png`) }); }
      await fit(pg, 480, 900);
      await pg.click(pillSel('silver')); await settle(pg); await tick(200);
      await pg.screenshot({ path: path.join(shots, 'sheet-menu-nest-open-silver-480.png'), clip: await pg.evaluate(() => { const c = document.querySelector('.sheetCard[data-m="silver"]').getBoundingClientRect(); return { x: Math.max(0, Math.floor(c.left) - 6), y: Math.max(0, Math.floor(c.top) - 6), width: Math.min(innerWidth - Math.max(0, Math.floor(c.left) - 6), Math.ceil(c.width) + 12), height: 320 }; }) });
      await pg.keyboard.press('Escape'); await gone(pg);
    }
    assert.deepEqual(errors, [], 'no page errors');
    await context.close();

    // 12 · reduced motion: a fade only
    { const o = await open({ reduce: true }); await seed(o.pg); await tick(400);
      await o.pg.evaluate(() => { window.__kf = []; const orig = Element.prototype.animate; Element.prototype.animate = function (f, opt) { if (this.classList && this.classList.contains('shmPanel')) window.__kf.push(Object.keys(f[0] || {}).sort().join(',')); return orig.call(this, f, opt); }; });
      await o.pg.click(pillSel('silver')); await tick(30);
      const anims = await o.pg.evaluate(() => window.__kf.slice());
      assert(anims.length >= 1 && anims.every(k => k === 'opacity'), `reduced motion: opacity only (${anims.join(' | ')})`);
      await settle(o.pg); await o.pg.keyboard.press('Escape'); await o.pg.waitForFunction(() => !document.querySelector('.shmPanel')); assert.equal(await o.pg.evaluate(() => window.__kf.every(k => k === 'opacity')), true, 'and closes with a fade only'); await o.context.close();
      console.log('  ✓ page: reduced motion: the menu only fades'); }
  } finally { await browser.close(); await srv.close(); }
}

(async () => { await component(); await page(); console.log('PASS: sheet-menu'); })().catch(err => { console.error(err); process.exitCode = 1; });
