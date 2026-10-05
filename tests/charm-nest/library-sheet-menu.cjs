/* The Library's sheet window: a set's sheets as one pill and a menu, like the back-engraving pill (charm-nest-sheetwin.js + the shared
   charm-nest-sheet-menu.js). Paul, 5 Oct 2026, 04:33 UTC: "Add drop down menu for the Sheets just like the new back engraving menu."
   What the Library has: its sheet cards and set cards carry NO row of sheet tabs (each sheet is a card of its own, a set card shows its
   sheets side by side), so the only sheet tabs on the Library side are the sheet window's chips for the sheets of the set (nav.swSheets:
   a hidden-scrollbar row that ran out of room, and was not drawn at all below 980 px). This test drives the REAL page (charm-nest-1.html
   with every script, served locally, no network: every request that is not to the local server is refused and counted) and its real
   sheet window over a set of sheets. What it holds to:
     1  the pill shows the sheet in the window (metal + number) without clipping, at 1440, 900, 390 and 320 px wide, shows below 980 px
        too (where the old chips were not drawn), and adds no height to the header or width past the window; nothing overlaps it
     2  the menu lists every sheet of the set with what the chips said (metal colour, number, the pieces of the order in hand, freed room),
        the current one marked; it is a layer inside the window (a modal dialog: a body layer would sit behind it) and stays on screen
     3  a pick switches the window exactly as a chip did (same sheet read, same title, same set kept, same page calls), the pill follows
     4  Esc closes the menu and not the window; a press outside closes it; a second press on the pill closes it
     5  keyboard: Enter / Space / ArrowDown open it, arrows move, Enter picks, Esc closes and gives the focus back to the pill
     6  the window repainting (a sheet read again, the order in hand changing by hover) keeps an open menu open, in the same element, with the new facts
     7  no per-sheet cost while it is closed (no menu items in the page), and a set of 300 sheets draws and opens inside the budget the chips needed
     8  without the component on the page the chips are drawn exactly as before (nothing changed)
     node tests/charm-nest/library-sheet-menu.cjs [playwright-core dir]   (PW_DIR, CHROMIUM; SHOTS=<dir> saves screenshots; MENU_JS=<path> loads the component from a file) */
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules', '/opt/node22/lib/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core'))) || path.join(root, 'node_modules');
let chromium;
try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '', MENU_JS = process.env.MENU_JS || '';
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
}).listen(0);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const METALS = ['gold', 'gold', 'silver', 'silver', 'gold14k', 'rose'];
const ORDER = '4100000001';   // the order in hand: pieces on three of the sheets

/** A Library of one set of `n` sheets (the first six in four metals), the window's record of each, and what the set says about ORDER. */
async function open(browser, { width = 1440, height = 800, n = 6, component = true, start = 'sh1', errors = [], requests = [] } = {}) {
  const context = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2 });
  // (room freed on the fourth sheet, as the window keeps it for twelve hours)
  await context.addInitScript(() => { try { localStorage.setItem('cn.sheetwin.freed', JSON.stringify({ sh3: [{ p: { cxPt: 40, cyPt: 40, angle: 0, scale: 1, wPt: 20, hPt: 20 }, id: 'x1', rid: '4100000099', sku: 'RING', at: Date.now(), s: null, b: null }] })); } catch (_) {} });
  const page = await context.newPage();
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  page.on('console', m => { if (m.type() === 'error') { const t = m.text(); if (!/Failed to load resource/.test(t)) errors.push('console: ' + t); } });
  await page.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { requests.push(r.request().url()); r.abort(); });
  if (!component) await page.route(u => /charm-nest-sheet-menu\.js/.test(u.href), r => r.fulfill({ status: 404, body: '' }));
  await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.SheetWin && window.Gate);
  // (MENU_JS=<path>: the component from a file while it is not yet in the page's script list)
  if (component && MENU_JS && !(await page.evaluate(() => !!window.SheetMenu))) await page.addScriptTag({ content: fs.readFileSync(MENU_JS, 'utf8') });
  await page.evaluate(({ n, METALS, ORDER, start }) => {
    const ids = [...Array(n)].map((_, i) => 'sh' + i);
    const rows = ids.map((id, i) => {
      const pool = [0, 1, 2].map(k => `${4100000001 + (k === 0 ? 0 : k)}_t${i}${k}_1`);
      const charms = [0, 1, 2].map(k => ({ id: `c${i}_${k}`, name: `${k === 0 ? ORDER : 4100000001 + k} · RING-${k}`, poolId: pool[k], order: k === 0 ? ORDER : String(4100000001 + k), sku: `RING-${k}` }));
      return { id, metal: METALS[i % METALS.length], metalLabel: METALS[i % METALS.length], folder: `GF_Oct.04.26_Set-3_Sheet-${i + 1}`, fileBase: `GF_Oct.04.26_Set-3_Sheet-${i + 1}`, day: '2026-10-04', sheetIndex: i + 1, setSeq: 3, setId: 'set-3',
        placedCount: 3, charmCount: 3, density: .6, freePt2: 100, orders: [ORDER], stock: { wIn: 3.94, hIn: 1.97, wPt: 283.5, hPt: 141.7 }, charms,
        placements: charms.map((c, k) => ({ id: c.id, cxPt: 40 + k * 60, cyPt: 70, angle: 0, scale: 1, wPt: 24, hPt: 24 })), outputs: {}, sources: [], createdAt: Date.now(), updatedAt: Date.now() };
    });
    CN.S.library.rows = rows;
    // the order in hand has pieces on three sheets: sh0 ×1, sh1 ×1 (the sheet in the window), sh2 ×2
    const set = { setId: 'set-3', seq: 3, sheetIds: ids, labelFiles: [], orders: { [ORDER]: { lines: [{ sku: 'RING-0', copies: [{ sheetId: 'sh0', poolId: ORDER + '_t00_1', copy: 1 }, { sheetId: 'sh1', poolId: ORDER + '_t10_1', copy: 4 }, { sheetId: 'sh2', poolId: ORDER + '_t20_1', copy: 2 }, { sheetId: 'sh2', poolId: ORDER + '_t21_1', copy: 3 }] }] },
      // a second order in hand (hovered in the list): sh3 ×1, sh4 ×3
      '4100000002': { lines: [{ sku: 'RING-1', copies: [{ sheetId: 'sh3', poolId: '4100000002_t31_1', copy: 1 }, ...[1, 2, 3].map(k => ({ sheetId: 'sh4', poolId: `4100000002_t4${k}_1`, copy: 1 + k }))] }] } } };
    window.__calls = []; window.__setGets = 0;
    const realApi = window.api;
    window.api = async (fn, body, opts) => {
      if (fn === 'charmNestLibrary' && body.op === 'getSheet') { window.__calls.push('getSheet:' + body.id); await new Promise(r => setTimeout(r, 25)); const r = rows.find(x => x.id === body.id); return { sheet: r ? JSON.parse(JSON.stringify(r)) : null }; }
      if (fn === 'charmNestLibrary' && body.op === 'setGet') { window.__calls.push('setGet'); window.__setGets++; return { set: JSON.parse(JSON.stringify(set)) }; }
      return realApi(fn, body, opts);
    };
    window.__rows = rows; window.__set = set;
    // Where an order's pieces are is OrderPieces' one truth now (the window lights its sheets from it): a small stand-in with the module's
    // shape for the two orders above, [sheet, pieces on it]; window.__opCb() is "a read landed" (the window repaints its sheets from it)
    window.__spec = { [ORDER]: [['sh0', 1], ['sh1', 1], ['sh2', 2]], '4100000002': [['sh3', 1], ['sh4', 3]] };
    window.OrderPieces = {
      of: rid => (window.__spec[rid] || []).flatMap(([sid, k]) => [...Array(k)].map((_, j) => { const i = +sid.slice(2); return { key: `${rid}_${sid}_${j}`, lineKey: `${rid}_t${i}${j}`, poolId: `${rid}_t${i}${j}_1`, sku: 'RING-' + (rid === ORDER ? 0 : 1), label: 'RING', copy: j + 1, qty: k, nested: true, sheetId: sid, sheetLabel: `${METALS[i % METALS.length]} ${i + 1}`, sheetNo: i + 1, metal: METALS[i % METALS.length], gone: false, loading: false, unsure: false, reason: '' }; })),
      load: () => Promise.resolve(true), subscribe: cb => { window.__opCb = cb; return () => {}; }
    };
    SheetWin.open(start);
  }, { n, METALS, ORDER, start });
  await page.waitForFunction(() => { const W = SheetWin._W; return SheetWin.isOpen() && W.rec && W.setSheets.length && !W.flip; }, null, { timeout: 20000 });
  await sleep(500);
  return { page, context };
}
const shot = async (page, name, clip) => { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png'), clip }); };


/* ── what the page says ── */
/** What the chips (the old row) or the menu's rows say about each sheet, by id. */
const facts = page => page.evaluate(() => {
  const out = {}, order = [];
  for (const b of document.querySelectorAll('.swSheets .swChip')) { order.push(b.dataset.sheet); out[b.dataset.sheet] = { label: [...b.childNodes].filter(n => n.nodeType === 3).map(n => n.textContent).join('').trim(), color: b.style.getPropertyValue('--c').trim(), badge: b.querySelector('b').textContent, freed: b.hasAttribute('data-freed'), current: b.getAttribute('aria-current') === 'true', name: b.title.replace(/ · has freed room$/, '') }; }
  for (const r of document.querySelectorAll('.shmPanel .shmRow')) { order.push(r.dataset.id); const dot = r.querySelector('.shmDot'), n = r.querySelector('.shmNote'), sub = r.querySelector('.shmSub'), bd = r.querySelector('.shmBadge');
    out[r.dataset.id] = { label: r.querySelector('.shmName').textContent, color: dot ? dot.style.getPropertyValue('--shm-dot').trim() : '', badge: bd ? bd.textContent : '', freed: !!(n && /freed room/.test(n.textContent)), current: r.getAttribute('aria-checked') === 'true', name: sub ? sub.textContent : '' }; }
  return { out, order };
});
/** The header's boxes (viewport px), the pill's, and whether anything is clipped or overlaps. */
const layout = page => page.evaluate(() => {
  const R = e => { const b = e.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom, w: b.width, h: b.height }; };
  const shown = e => e && !e.hidden && e.getClientRects().length > 0;
  const head = document.querySelector('.swHead'), dlg = document.querySelector('dialog.sheetWin'), host = head.querySelector('.swSheets'), pill = head.querySelector('.shMenu'), name = pill && pill.querySelector('.shmName');
  const parts = { metal: head.querySelector('.swMetal'), title: head.querySelector('.swTitle h3'), pill, state: head.querySelector('.swState'), done: head.querySelector('[data-r=done]'), more: head.querySelector('[data-r=moreBtn]'), close: head.querySelector('[data-r=close]') };
  const boxes = {}; for (const [k, e] of Object.entries(parts)) if (shown(e)) boxes[k] = R(e);
  const overlaps = []; const ks = Object.keys(boxes);
  for (let i = 0; i < ks.length; i++) for (let j = i + 1; j < ks.length; j++) { const a = boxes[ks[i]], b = boxes[ks[j]]; if (a.l < b.r - .5 && b.l < a.r - .5 && a.t < b.b - .5 && b.t < a.b - .5) overlaps.push(ks[i] + '×' + ks[j]); }
  const hr = R(head);
  return { vw: innerWidth, vh: innerHeight, head: hr, headH: hr.h, headScroll: [head.scrollWidth, head.clientWidth], dlgScroll: [dlg.scrollWidth, dlg.clientWidth], pageScroll: [document.documentElement.scrollWidth, document.documentElement.clientWidth], boxes, overlaps,
    hostShown: shown(host), hostDisplay: getComputedStyle(host).display, hostClass: host.className, hasPill: !!pill, chips: host.querySelectorAll('.swChip').length, chev: !!(pill && pill.querySelector('.shmChev')),
    label: name ? name.textContent : null, nameClip: name ? [name.scrollWidth, name.clientWidth] : null, titleClip: parts.title ? [parts.title.scrollWidth, parts.title.clientWidth] : null, nodes: host.querySelectorAll('*').length, rows: document.querySelectorAll('.shmRow').length, panel: !!document.querySelector('.shmPanel') };
});
const panelBox = page => page.evaluate(() => { const p = document.querySelector('.shmPanel'); if (!p) return null; const b = p.getBoundingClientRect(), d = document.querySelector('dialog.sheetWin'); return { l: b.left, t: b.top, r: b.right, b: b.bottom, vw: innerWidth, vh: innerHeight, inDialog: d.contains(p), role: p.getAttribute('role'), rows: [...p.querySelectorAll('.shmRow')].map(r => r.dataset.id) }; });
const isOpen = page => page.evaluate(() => !!document.querySelector('.shmPanel') && SheetMenu.isOpen('sheetwin'));
const closed = page => page.waitForFunction(() => !document.querySelector('.shmPanel') && !SheetMenu.isOpen('sheetwin'), null, { timeout: 15000 });
const shown = page => page.waitForSelector('.shmPanel .shmRow', { state: 'visible', timeout: 15000 });
const winOpen = page => page.evaluate(() => SheetWin.isOpen());
/** Click an order's row in the window's list: the order in hand (its piece is selected, the sheets say where its pieces are). */
const hand = async (page, rid = ORDER) => { await page.click(`.swOrd[data-rid="${rid}"]`); await page.waitForFunction(rid => { const W = SheetWin._W; return W.sel && W.sel.rid === rid; }, rid, { timeout: 15000 }); await sleep(250); };
const onSheet = (page, id) => page.waitForFunction(id => { const W = SheetWin._W; return SheetWin.current() === id && W.rec && W.rec.id === id && W.geom !== undefined; }, id, { timeout: 30000 });

/** One pick, the chip's way or the menu's: what the window and the page's calls come to. */
async function pickRun(browser, component, errors, requests) {
  const { page, context } = await open(browser, { component, errors, requests });
  await hand(page);
  const calls0 = await page.evaluate(() => (window.__pill = document.querySelector('.shMenu'), window.__calls.length));
  if (component) { await page.click('.swSheets .shmBtn'); await shown(page); await page.click('.shmPanel .shmRow[data-id="sh3"]'); } else await page.click('.swSheets .swChip[data-sheet="sh3"]');
  await onSheet(page, 'sh3'); await sleep(600);
  const r = await page.evaluate(() => { const W = SheetWin._W; return { current: SheetWin.current(), title: document.querySelector('.swTitle h3').textContent, sub: document.querySelector('.swTitle span').textContent, open: SheetWin.isOpen(), setId: W.set && W.set.setId, sheets: W.setSheets.map(s => s.id), calls: window.__calls.slice(), pillKept: window.__pill ? window.__pill === document.querySelector('.shMenu') : null, panel: !!document.querySelector('.shmPanel'), current_: document.querySelector('.swSheets .shmBtn, .swSheets [aria-current=true]') ? (document.querySelector('.swSheets .shmName') ? document.querySelector('.swSheets .shmName').textContent : document.querySelector('.swSheets [aria-current=true]').textContent.trim()) : '' }; });
  const focus = component ? await page.evaluate(() => document.activeElement === document.querySelector('.swSheets .shmBtn')) : null;
  await context.close(); return Object.assign(r, { calls0, focus });
}

module.exports = { open, shot, sleep, ORDER };
if (require.main === module) (async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [], requests = [], log = m => console.log('  ' + m);
  try {
    // the component and its place on the page: its script tag and the build's file list
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
    assert(/<script src="charm-nest-sheet-menu\.js[^"]*"><\/script>/.test(html), 'charm-nest-1.html loads the shared component');
    assert(build.includes('"charm-nest-sheet-menu.js"'), 'the build ships it');

    /* ── 8. without the component: the chips are drawn as they always were ── */
    const oldFacts = {}, oldLayout = {};
    for (const width of [1440, 900]) {
      const { page, context } = await open(browser, { width, component: false, errors, requests });
      const l = await layout(page); oldLayout[width] = l;
      assert.equal(l.hasPill, false, 'no pill without the component'); assert.equal(l.chips, 6, 'six chips, as before'); assert(!/swPill/.test(l.hostClass));
      if (width === 1440) { assert.equal(l.hostShown, true); await shot(page, `r5-2-sheetwin-before-${width}`, { x: 0, y: 0, width, height: 130 }); await hand(page); const f = await facts(page); oldFacts.chips = f; assert.deepEqual(f.order, ['sh0', 'sh1', 'sh4', 'sh5', 'sh2', 'sh3'], 'the chips run by metal then number');
        assert.deepEqual(f.order.map(id => f.out[id].badge), ['1', '1', '', '', '2', ''], 'the order in hand lights the sheets it is on, with its pieces there'); assert.equal(f.out.sh3.freed, true, 'sheet 4 has freed room'); assert.equal(f.out.sh1.current, true); }
      else { assert.equal(l.hostShown, false); assert.equal(l.hostDisplay, 'none', 'below 980 px the chips were not drawn at all'); await shot(page, `r5-2-sheetwin-before-${width}`, { x: 0, y: 0, width, height: 130 }); }
      await context.close(); log(`without the component, ${width} px: ${l.chips} chips, header ${l.headH} px, row ${l.hostShown ? 'shown' : 'hidden'}`);
    }

    /* ── 1. the pill at every width ── */
    for (const width of [1440, 900, 390, 320]) {
      const { page, context } = await open(browser, { width, errors, requests });
      const l = await layout(page);
      assert.equal(l.hasPill, true, `${width}: the sheets are a pill`); assert.equal(l.hostShown, true, `${width}: it shows (the chips did not below 980 px)`); assert(/swPill/.test(l.hostClass)); assert.equal(l.chips, 0, 'no chips beside it'); assert.equal(l.chev, true, 'with a chevron: there is a choice');
      assert.equal(l.label, 'GF 2', `${width}: it names the sheet in the window`); assert(l.nameClip[0] <= l.nameClip[1], `${width}: its name is whole, not clipped (${l.nameClip})`);
      assert(l.titleClip[0] <= l.titleClip[1] + 1, `${width}: the window's title is whole (${l.titleClip})`);
      assert.deepEqual(l.overlaps, [], `${width}: nothing in the bar overlaps (${l.overlaps})`);
      assert(l.headScroll[0] <= l.headScroll[1] + 1 && l.dlgScroll[0] <= l.dlgScroll[1] + 1, `${width}: no sideways scroll in the bar or the window (${JSON.stringify([l.headScroll, l.dlgScroll])})`);   // (the page behind the window is wider than a 320 px screen with or without it)
      const p = l.boxes.pill; assert(p.l >= l.head.l && p.r <= l.head.r && p.l >= 0 && p.r <= l.vw, `${width}: the pill is inside the bar and the screen`);
      assert.equal(l.nodes <= 14, true, `${width}: a closed pill is a handful of nodes (${l.nodes})`); assert.equal(l.rows, 0, 'no rows while closed'); assert.equal(l.panel, false);
      if (width >= 900) assert(Math.abs(l.headH - oldLayout[width].headH) <= 1.2, `${width}: the bar is as tall as it was (${l.headH} vs ${oldLayout[width].headH}; under a pixel is the title's font arriving)`);
      else assert(l.headH <= 100, `${width}: the bar takes two short lines on a phone (${l.headH} px), the title, its menu and Close above the pill and the status`);
      await shot(page, `r5-2-sheetwin-after-${width}`, { x: 0, y: 0, width, height: 130 });
      // the menu at this width: every sheet, on screen, in the window's own layer
      await page.click('.swSheets .shmBtn'); await shown(page); await sleep(350);
      const pb = await panelBox(page);
      assert.equal(pb.inDialog, true, `${width}: the menu is a layer in the window (a modal dialog: a layer on the body would sit behind it)`); assert.equal(pb.role, 'menu');
      assert(pb.l >= 0 && pb.t >= 0 && pb.r <= pb.vw + .5 && pb.b <= pb.vh + .5, `${width}: the menu stays on the screen ${JSON.stringify([pb.l, pb.t, pb.r, pb.b, pb.vw, pb.vh])}`);
      assert.deepEqual(pb.rows, ['sh0', 'sh1', 'sh4', 'sh5', 'sh2', 'sh3'], `${width}: every sheet of the set, in the chips' order`);
      await shot(page, `r5-2-sheetwin-menu-${width}`, { x: 0, y: 0, width, height: Math.min(560, l.vh) });
      await page.keyboard.press('Escape'); await closed(page);
      assert.equal(await winOpen(page), true, `${width}: Esc shut the menu, not the window`);
      await context.close(); log(`pill at ${width} px: "${l.label}", bar ${l.headH} px, ${l.nodes} nodes, menu inside ${pb.r - pb.l | 0} × ${pb.b - pb.t | 0}`);
    }

    /* ── 2–7 at desktop width, one window ── */
    { const { page, context } = await open(browser, { width: 1440, errors, requests });
      const sid = () => page.evaluate(() => SheetWin.current());
      // 2. the facts: the same as the chips said, sheet by sheet, once an order is in hand
      await hand(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.shmRow').length), 0, 'closed: no rows at all');
      assert.equal(await page.evaluate(() => document.querySelector('.swSheets .shmBadge') && document.querySelector('.swSheets .shmBadge').textContent), '1', 'the pill carries the pieces of the order in hand on this sheet');
      await page.click('.swSheets .shmBtn'); await shown(page); await sleep(300);
      let f = await facts(page);
      await shot(page, 'r5-2-sheetwin-menu-facts-1440', { x: 0, y: 0, width: 1440, height: 430 });
      assert.deepEqual(f.order, oldFacts.chips.order, 'the same sheets in the same order as the chips');
      for (const id of f.order) { const a = f.out[id], b = oldFacts.chips.out[id];
        assert.equal(a.label, b.label, `${id}: the same name`); assert.equal(a.color, b.color, `${id}: the same metal colour`); assert.equal(a.badge, b.badge, `${id}: the same pieces of the order in hand`);
        assert.equal(a.freed, b.freed, `${id}: the same freed room`); assert.equal(a.current, b.current, `${id}: the same current sheet`); assert.equal(a.name, b.name, `${id}: the same file name`); }
      assert.deepEqual(f.order.filter(id => f.out[id].current), ['sh1'], 'only the sheet in the window is marked');
      assert.match(await page.evaluate(() => document.querySelector('.shmPanel .shmRow[data-id="sh2"]').title), /2 pieces of order 4100000001 here/, 'pieces, never lines');
      assert(!/\blines?\b/i.test(await page.evaluate(() => document.querySelector('.shmPanel').textContent + [...document.querySelectorAll('.shmRow')].map(r => r.title + r.getAttribute('aria-label')).join(' '))), 'no "lines" anywhere in it');
      // the hover that lit the chips: another order's row lights its own sheets while the menu is open elsewhere (below)
      await page.keyboard.press('Escape'); await closed(page);

      // 4. Esc shuts the menu and not the window; a press outside; a second press on the pill
      await page.click('.swSheets .shmBtn'); await shown(page); await page.keyboard.press('Escape'); await closed(page);
      assert.equal(await winOpen(page), true, 'Esc closed the menu and not the window');
      assert.equal(await page.evaluate(() => document.activeElement === document.querySelector('.swSheets .shmBtn')), true, 'the focus is back on the pill');
      await page.click('.swSheets .shmBtn'); await shown(page); await page.click('.swTitle h3'); await closed(page);
      assert.equal(await winOpen(page), true, 'a press outside shut the menu and nothing else'); assert.equal(await sid(), 'sh1');
      await page.click('.swSheets .shmBtn'); await shown(page); await sleep(300); await page.click('.swSheets .shmBtn'); await closed(page);
      assert.equal(await isOpen(page), false, 'a second press on the pill shuts it, and it does not reopen'); assert.equal(await winOpen(page), true);

      // 5. the keyboard: Enter, Space and ArrowDown open it; the arrows, Home and End move; Enter picks
      await page.focus('.swSheets .shmBtn'); await page.keyboard.press('Enter'); await shown(page);
      assert.equal(await page.evaluate(() => document.activeElement.dataset.id), 'sh1', 'it opens on the current sheet'); await page.keyboard.press('Escape'); await closed(page);
      await page.focus('.swSheets .shmBtn'); await page.keyboard.press('Space'); await shown(page); await page.keyboard.press('Escape'); await closed(page);
      await page.focus('.swSheets .shmBtn'); await page.keyboard.press('ArrowDown'); await shown(page);
      await page.keyboard.press('ArrowDown'); assert.equal(await page.evaluate(() => document.activeElement.dataset.id), 'sh4', 'Down moves to the next sheet');
      await page.keyboard.press('End'); assert.equal(await page.evaluate(() => document.activeElement.dataset.id), 'sh3'); await page.keyboard.press('ArrowDown'); assert.equal(await page.evaluate(() => document.activeElement.dataset.id), 'sh0', 'Down wraps round');
      await page.keyboard.press('Home'); assert.equal(await page.evaluate(() => document.activeElement.dataset.id), 'sh0');
      assert.equal(await winOpen(page), true, 'the arrows moved in the menu, they did not step the window');
      await page.keyboard.press('Enter'); await closed(page); await onSheet(page, 'sh0'); await sleep(500);
      assert.equal(await sid(), 'sh0', 'Enter picked sheet 1'); assert.equal(await page.evaluate(() => document.querySelector('.swTitle h3').textContent), 'Sheet 1');
      assert.equal(await page.evaluate(() => document.activeElement === document.querySelector('.swSheets .shmBtn')), true, 'the focus is back on the pill after a pick');
      assert.equal(await page.evaluate(() => document.querySelector('.swSheets .shmName').textContent), 'GF 1', 'and the pill names the new sheet');

      // 6. a live repaint: an open menu stays open, in the same elements, and takes the new facts (the order hovered in the list)
      await page.click('.swSheets .shmBtn'); await shown(page); await sleep(300);
      await page.evaluate(() => {
        const p = document.querySelector('.shmPanel'); window.__panel = p; window.__rowEls = [...p.querySelectorAll('.shmRow')]; window.__pillEl = document.querySelector('.swSheets .shMenu'); window.__btnEl = document.querySelector('.swSheets .shmBtn');
        window.__removed = []; window.__mo = new MutationObserver(l => { for (const m of l) for (const n of m.removedNodes) if (n.nodeType === 1) window.__removed.push(n.className || n.tagName); });
        window.__mo.observe(document.querySelector('.swSheets'), { childList: true, subtree: true }); window.__mo.observe(p, { childList: true, subtree: true });
      });
      await page.hover('.swOrd[data-rid="4100000002"]'); await sleep(300);
      assert.equal(await isOpen(page), true, 'the menu is still open while the list is hovered');
      f = await facts(page); assert.deepEqual(f.order.map(id => f.out[id].badge), ['', '', '3', '', '', '1'], 'the hovered order lights its own sheets (sheet 5 ×3, sheet 4 ×1) in the open menu');
      // the window reading its sheet again (after a change in it): same sheet, repainted
      await page.evaluate(() => SheetWin.open(SheetWin.current(), { keepWork: true, keepSet: true })); await sleep(900);
      const same = await page.evaluate(() => ({ open: SheetMenu.isOpen('sheetwin'), panel: window.__panel === document.querySelector('.shmPanel'), pill: window.__pillEl === document.querySelector('.swSheets .shMenu'), btn: window.__btnEl === document.querySelector('.swSheets .shmBtn'), rows: window.__rowEls.every(r => r.isConnected), removed: window.__removed.slice() }));
      assert.equal(same.open, true, 'a sheet read again leaves the menu open'); assert.equal(same.panel, true, 'in the same layer'); assert.equal(same.pill && same.btn, true, 'and the pill is the same element (no redraw, no flicker)'); assert.equal(same.rows, true, 'every row the same element');
      assert.deepEqual(same.removed.filter(c => !/^shm(Mark|Badge|Note)$/.test(c)), [], `no pill, name, chevron, layer or row was removed through all of it (only the small facts that came and went: ${same.removed})`);
      await page.evaluate(() => window.__mo.disconnect());
      // a sheet that goes from the set while it is open folds away, the rest stay (the set read again with one sheet less)
      await page.evaluate(() => { const W = SheetWin._W; W.set.sheetIds = W.set.sheetIds.filter(id => id !== 'sh5'); SheetWin.open(SheetWin.current(), { keepWork: true, keepSet: true }); });
      await sleep(900); assert.equal(await isOpen(page), true); assert.deepEqual((await panelBox(page)).rows, ['sh0', 'sh1', 'sh4', 'sh2', 'sh3'], 'a sheet that left the set is gone from the open menu, the rest stay');
      await page.keyboard.press('Escape'); await closed(page);
      // the order picked in the list is the order in hand; a read of its pieces landing (OrderPieces tells the window) repaints the sheets: the open menu takes it in place
      await page.click('.swOrd[data-rid="4100000002"]'); await page.waitForFunction(() => { const W = SheetWin._W; return W.sel && W.sel.rid === '4100000002'; }, null, { timeout: 15000 }); await sleep(250);
      await page.click('.swSheets .shmBtn'); await shown(page); await sleep(250);
      f = await facts(page); assert.equal(f.out.sh4.badge, '3', 'the order in hand has three pieces on sheet 5'); assert.equal(f.out.sh3.badge, '1');
      await page.evaluate(() => { window.__panel = document.querySelector('.shmPanel'); window.__row4 = document.querySelector('.shmRow[data-id="sh4"]'); window.__spec['4100000002'] = [['sh3', 1], ['sh4', 5]]; window.__opCb(); });
      await sleep(500);
      f = await facts(page); assert.equal(f.out.sh4.badge, '5', 'the pieces read since: five on sheet 5, in the open menu');
      assert.equal(await page.evaluate(() => SheetMenu.isOpen('sheetwin') && window.__panel === document.querySelector('.shmPanel') && window.__row4 === document.querySelector('.shmRow[data-id="sh4"]')), true, 'the same menu and the same row, rewritten in place');
      await page.keyboard.press('Escape'); await closed(page);

      // 7. the window closing shuts the menu with it; the pill is the same element when the window opens again
      await page.click('.swSheets .shmBtn'); await shown(page);
      await page.evaluate(() => { window.__pillEl = document.querySelector('.swSheets .shMenu'); SheetWin.close(); });
      await page.waitForFunction(() => !SheetWin.isOpen(), null, { timeout: 15000 }); await closed(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.shmPanel').length), 0, 'no menu is left behind when its window goes');
      await page.evaluate(() => SheetWin.open('sh2')); await onSheet(page, 'sh2'); await sleep(500);
      assert.equal(await page.evaluate(() => window.__pillEl === document.querySelector('.swSheets .shMenu') && document.querySelector('.swSheets .shmName').textContent), 'SS 3', 'opened again: the same pill, naming the sheet it opened on');
      assert.equal(await isOpen(page), false);
      await context.close(); log('menu: facts match the chips, Esc/outside/second press, keyboard, live repaint, window close all hold'); }

    /* ── 3. a pick switches the window exactly as the chip did ── */
    { const oldRun = await pickRun(browser, false, errors, requests), newRun = await pickRun(browser, true, errors, requests);
      assert.equal(newRun.current, 'sh3'); assert.equal(newRun.title, oldRun.title, 'the same title'); assert.equal(newRun.title, 'Sheet 4'); assert.equal(newRun.sub, oldRun.sub, 'the same line under it');
      assert.equal(newRun.setId, oldRun.setId, 'the same set kept'); assert.deepEqual(newRun.sheets, oldRun.sheets, 'the same sheets in it'); assert.equal(newRun.open, true);
      assert.deepEqual(newRun.calls, oldRun.calls, `the same reads of the page: ${JSON.stringify(newRun.calls)}`); assert.equal(newRun.current_, 'SS 4', 'the pill names the sheet picked');
      assert.equal(newRun.pillKept, true, 'the pill element was kept through the switch'); assert.equal(newRun.panel, false, 'the menu shut before the sheet changed'); assert.equal(newRun.focus, true, 'the focus is on the pill');
      assert.equal(newRun.calls.filter(c => c === 'setGet').length, 1, 'the set is not read again for a switch (keepSet)');
      log(`pick: same title "${newRun.title}", same reads ${JSON.stringify(newRun.calls)} as the chip`); }

    /* ── 7. no cost per sheet while closed; 300 sheets ── */
    { const timing = async (component) => {
        const t0 = Date.now(); const { page, context } = await open(browser, { n: 300, component, errors, requests }); const loadMs = Date.now() - t0;
        const r = await page.evaluate(async component => {
          const W = SheetWin._W, host = document.querySelector('.swSheets');
          const nodes = host.querySelectorAll('*').length, rows = document.querySelectorAll('.shmRow').length;
          // one repaint of the sheet row, as the window does on every sheet read and every hover: the chips' innerHTML, or the pill's update.
          // What is timed is that call's own work (script and DOM); the layout the page does after it is the page's, timed apart
          const reps = [], lay = [];
          if (!component) { const d = Object.getOwnPropertyDescriptor(Element.prototype, 'innerHTML'); const html = host.innerHTML; for (let i = 0; i < 7; i++) { const t = performance.now(); d.set.call(host, html); reps.push(performance.now() - t); const l = performance.now(); void host.offsetWidth; lay.push(performance.now() - l); } }
          else { const items = W.menu.items().map(i => ({ id: i.id, label: i.label, color: i.color, sub: i.sub, badge: i.badge, note: i.note, current: i.active })); for (let i = 0; i < 7; i++) { const next = items.map(x => Object.assign({}, x, { badge: x.id === 'sh7' ? String(i + 1) : '' })); const t = performance.now(); W.menu.update(next); reps.push(performance.now() - t); const l = performance.now(); void host.offsetWidth; lay.push(performance.now() - l); } }
          reps.sort((a, b) => a - b); lay.sort((a, b) => a - b); const ms = reps[3];
          let openMs = null, rowsOpen = null;
          if (component) { const t = performance.now(); W.menu.open(); void document.querySelector('.shmPanel').offsetWidth; openMs = performance.now() - t; rowsOpen = document.querySelectorAll('.shmRow').length; W.menu.close({ now: true }); }
          return { nodes, rows, ms: +ms.toFixed(2), layoutMs: +lay[3].toFixed(1), openMs: openMs == null ? null : +openMs.toFixed(1), rowsOpen, sheets: W.setSheets.length };
        }, component);
        await context.close(); return Object.assign(r, { loadMs });
      };
      const chips = await timing(false), pill = await timing(true);
      assert.equal(chips.sheets, 300); assert.equal(pill.sheets, 300);
      assert(chips.nodes > 600, `the chips were three nodes a sheet (${chips.nodes})`); assert(pill.nodes <= 14, `the pill is a handful of nodes whatever the sheets (${pill.nodes})`); assert.equal(pill.rows, 0, 'no rows for 300 sheets while closed');
      assert(pill.ms < chips.ms, `a repaint of the row costs less than the chips did (${pill.ms} ms against ${chips.ms} ms)`); assert(pill.ms < 15, `and stays well within a frame (${pill.ms} ms)`);
      assert.equal(pill.rowsOpen, 300, 'opened, it lists all 300'); assert(pill.openMs < 1500, `and opens in about a second at the most, here on a machine busy with other jobs (${pill.openMs} ms)`);
      log(`300 sheets: chips ${chips.nodes} nodes, repaint ${chips.ms} ms + layout ${chips.layoutMs} ms · pill ${pill.nodes} nodes, repaint ${pill.ms} ms + layout ${pill.layoutMs} ms, a menu of 300 rows opens in ${pill.openMs} ms (the machine was busy with other jobs: read these as ratios)`); }

    /* ── a set of one sheet has no row, as before; the Library's cards have no sheet tabs to change ── */
    { const { page, context } = await open(browser, { n: 1, start: 'sh0', errors, requests }); const l = await layout(page);
      assert.equal(l.hostShown, false, 'a one-sheet set shows no sheet row, as before'); assert.equal(l.hasPill, false, 'and mounts nothing'); await context.close();
      const lib = await open(browser, { n: 3, errors, requests });
      await lib.page.evaluate(async () => { SheetWin.close(); });
      await lib.page.waitForFunction(() => !SheetWin.isOpen(), null, { timeout: 15000 });
      const cards = await lib.page.evaluate(async () => { CN.S.mode = 'library'; const body = document.querySelector('#libBody'); if (!body || typeof renderLibrary !== 'function') return null; try { await renderLibrary(); } catch (e) { return { err: String(e) }; } return { cards: body.querySelectorAll('.libCard').length, pills: body.querySelectorAll('.shMenu, [data-sheetmenu]').length, tabs: body.querySelectorAll('.libCard .shTabs, .libCard [role=tablist]').length }; });
      if (cards && cards.cards) { assert.equal(cards.pills, 0, 'the Library cards carry no sheet pill: each sheet is a card of its own, there is no row of sheet tabs on them'); assert.equal(cards.tabs, 0); log(`Library cards (${cards.cards}) carry no sheet row and no pill`); } else log('(the real Library could not be drawn in this page: its cards were not checked)');
      await lib.context.close(); }

    assert.deepEqual(requests, [], 'no request left the page'); assert.deepEqual(errors, [], 'no page errors');
    console.log('library-sheet-menu: all passed');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
