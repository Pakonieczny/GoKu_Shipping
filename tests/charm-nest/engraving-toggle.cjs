// The show/hide control for a sheet's back-engraving shelf (Paul, 5 Oct: "Add a colapsable button functionality to hide/show all of
// the back engraving on top of a given sheet … Nest tab sheets and Library Tab Sheets … small and elegant … By default keep it
// collapsed"). The shelf is the strip of back-engraving thumbnails above a sheet's picture (CharmNestBacks.markup).
//  1 · the component alone (jsdom; skipped without it): collapsed by default, opens and closes, the event, per-sheet independence,
//      no control without engraving, a redraw of the shelf's children keeps the choice, rekey, destroy, nothing stored;
//  2 · the real Nest page (Chromium): every card starts collapsed, a press opens that sheet's shelf and only that one, a press
//      again hides it; survives renderCard / the live refresh (children replaced) and a tab round trip; a sheet with no
//      engraving has no control; the keyboard works; toggling asks the network for nothing; a reload starts collapsed again;
//      and the controls row is exactly as tall with the control as without it, at 320, 480 and 900 px.
//   NODE_PATH=<jsdom node_modules> PW_DIR=<playwright node_modules> node tests/charm-nest/engraving-toggle.cjs   (CHROMIUM=<chrome>)
//   ENG_SHOTS=<dir> also writes the before / after pictures of a card there.
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const SRC = fs.readFileSync(path.join(root, 'charm-nest-engraving-toggle.js'), 'utf8');
const tick = (n = 30) => new Promise(r => setTimeout(r, n));

/* ── 1 · the component ── */
async function component() {
  let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('  – no jsdom (set NODE_PATH): the component checks were not run'); return; }
  const dom = new JSDOM('<!doctype html><body><div id="row"><span id="hostA" hidden></span><span id="hostB" hidden></span></div>'
    + '<div id="shelfA"><section class="sheetBacks"><div class="backPieces"><figure></figure><figure></figure></div></section></div>'
    + '<div id="shelfB"><section class="sheetBacks"><div class="backPieces"><figure></figure></div></section></div></body>', { url: 'http://localhost/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document; w.eval(SRC); const ET = w.EngravingToggle;
  assert(ET && ['mount', 'follow', 'isOpen', 'set', 'toggle', 'rekey', 'countIn'].every(f => typeof ET[f] === 'function'), 'the API is there');
  const events = []; d.addEventListener('engravingtoggle', e => events.push(e.detail));
  const A = d.getElementById('hostA'), B = d.getElementById('hostB'), sA = d.getElementById('shelfA'), sB = d.getElementById('shelfB');
  assert.equal(ET.countIn(sA), 2); assert.equal(ET.countIn(sB), 1);
  const tA = ET.mount(A, { sheetKey: 'A', count: ET.countIn(sA) }), tB = ET.mount(B, { sheetKey: 'B', count: ET.countIn(sB) });
  ET.follow(sA, 'A'); ET.follow(sB, 'B');
  const btn = h => h.querySelector('button');
  // collapsed by default, and the control says so
  assert.equal(A.hidden, false, 'a sheet with engraving has its control');
  assert.equal(btn(A).getAttribute('aria-expanded'), 'false'); assert.equal(btn(A).getAttribute('aria-pressed'), 'false');
  assert.equal(btn(A).textContent.trim(), '2', 'the count of pieces'); assert.match(btn(A).getAttribute('aria-label'), /Back engraving, 2 pieces/); assert.match(btn(B).getAttribute('aria-label'), /1 piece\b/, 'one piece, not "1 pieces"');
  assert.equal(sA.getAttribute('data-eng'), 'closed'); assert.equal(sB.getAttribute('data-eng'), 'closed'); assert.equal(ET.isOpen('A'), false);
  assert.equal(btn(A).type, 'button', 'a real button: Enter and Space work'); assert.notEqual(btn(A).tabIndex, -1);
  assert.doesNotMatch(btn(A).title + A.textContent, /\blines?\b/i, 'pieces, never lines');
  // open one
  btn(A).click();
  assert.equal(ET.isOpen('A'), true); assert.equal(sA.getAttribute('data-eng'), 'open'); assert.equal(btn(A).getAttribute('aria-expanded'), 'true'); assert.equal(btn(A).getAttribute('aria-pressed'), 'true');
  assert.equal(sB.getAttribute('data-eng'), 'closed', 'the other sheet stays collapsed'); assert.equal(btn(B).getAttribute('aria-expanded'), 'false');
  assert.equal(JSON.stringify(events.at(-1)), JSON.stringify({ sheetKey: 'A', open: true, source: 'button' }), 'the event says which sheet and which way');
  // a redraw replaces the shelf's children, never its host: the choice stays
  sA.innerHTML = '<section class="sheetBacks"><div class="backPieces"><figure></figure><figure></figure><figure></figure></div></section>';
  ET.follow(sA, 'A'); assert.equal(sA.getAttribute('data-eng'), 'open', 'still open after the shelf was drawn again');
  tA.update(ET.countIn(sA)); assert.equal(btn(A).textContent.trim(), '3'); assert.equal(btn(A).getAttribute('aria-expanded'), 'true', 'a new count keeps the state');
  const n0 = events.length; ET.follow(sA, 'A'); ET.follow(sA, 'A'); assert.equal(events.length, n0, 'following again is silent');
  // close it
  btn(A).click(); assert.equal(ET.isOpen('A'), false); assert.equal(sA.getAttribute('data-eng'), 'closed'); assert.equal(btn(A).getAttribute('aria-expanded'), 'false');
  // the API, and any picture listening
  const seen = []; d.addEventListener('engravingtoggle', e => seen.push(e.detail.sheetKey + ':' + e.detail.open));
  ET.set('B', true); ET.set('B', true); ET.toggle('B'); assert.deepEqual(seen, ['B:true', 'B:false'], 'a change fires once; no change fires nothing');
  ET.set('B', true); assert.equal(sB.getAttribute('data-eng'), 'open'); assert.equal(btn(B).getAttribute('aria-expanded'), 'true', 'the API moves the control too'); ET.set('B', false);
  // no engraving, no control; it comes back with the first piece
  tA.update(0); assert.equal(A.hidden, true); tA.update(2); assert.equal(A.hidden, false);
  // one control walks from sheet to sheet (a card's tabs): each sheet has its own state
  ET.set('C', true); tB.update(1, 'C'); assert.equal(btn(B).getAttribute('aria-expanded'), 'true'); tB.update(1, 'B'); assert.equal(btn(B).getAttribute('aria-expanded'), 'false'); ET.set('C', false);
  // a page saved for the first time takes its choice to its sheet id
  ET.set('page:x', true); ET.rekey('page:x', 'sheet-9'); assert.equal(ET.isOpen('page:x'), false); assert.equal(ET.isOpen('sheet-9'), true); ET.set('sheet-9', false);
  // the host's own state and callback
  let mine = false; const calls = []; const H = d.createElement('span'); d.body.appendChild(H);
  const tH = ET.mount(H, { sheetKey: 'H', count: 4, getState: () => mine, onChange: (o, k) => { mine = o; calls.push([o, k]); } });
  btn(H).click(); assert.equal(JSON.stringify(calls), JSON.stringify([[true, 'H']])); assert.equal(btn(H).getAttribute('aria-expanded'), 'true'); tH.destroy();
  assert.equal(H.querySelector('button'), null, 'destroy takes the control away'); ET.set('H', false);
  // nothing is stored: a load starts collapsed
  assert.equal(w.localStorage.length + w.sessionStorage.length, 0, 'nothing written to storage');
  console.log('  ✓ component: collapsed by default, opens and closes, per sheet, event, redraw-proof, no control without engraving, nothing stored');
  w.close();
}

/* ── 2 · the real Nest page ── */
async function page() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shots = process.env.ENG_SHOTS || '';
  try {
    const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');
    let backHits = 0;
    const open = async (opts = {}) => {
      const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
      await context.route(u => /\/eng-back-\d+\.png/.test(u.href), r => { backHits++; r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }, body: PNG }); });
      if (opts.noToggle) await context.route(u => /charm-nest-engraving-toggle\.js/.test(u.href), r => r.fulfill({ status: 200, contentType: 'text/javascript', body: '/* the control left out: how the shelf was before */' }));
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
      const pg = await context.newPage(); pg.setDefaultTimeout(20000);
      pg.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
      await pg.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await pg.waitForFunction(() => window.CN && window.Engrave && window.CharmNestBacks && CN.S.cloud.ok === true, null, { timeout: 60000 });
      return { context, pg };
    };
    const errors = [];
    // three cards: 14K with two sheets (3 and 2 backs), GF with one sheet and no engraving, SS with one sheet (4 backs)
    const seed = async pg => pg.evaluate(origin => {
      const MM = 72 / 25.4, sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [6 * MM, 0]], ['l', [6 * MM, 6 * MM]], ['l', [0, 6 * MM]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 6 * MM, 6 * MM] };
      let n = 0;
      const fill = (pgx, sheetId, nBacks, tag) => {
        const ids = Array.from({ length: Math.max(nBacks, 2) }, (_, i) => `${tag}${i}`);
        pgx.sheetId = sheetId; pgx.status = 'complete'; pgx.dirty = false; pgx.poolIds = ids.map(i => `${i}_1_1`);
        pgx.charms = ids.map((id, i) => ({ id, name: `41737${tag}${i} · TAG`, poolId: `${id}_1_1`, order: `41737${tag}${i}`, sku: 'TAG', centerPt: [3 * MM, 3 * MM], widthPt: 6 * MM, heightPt: 6 * MM, areaPt2: 36 * MM * MM, outline: sq, members: [sq] }));
        pgx.placements = ids.map((id, i) => ({ id, cxPt: (10 + i * 9) * MM, cyPt: 12 * MM, angle: 0, wPt: 6 * MM, hPt: 6 * MM }));
        pgx.backPool = ids.slice(0, nBacks).map((id, i) => ({ poolId: `${id}_1_1`, order: `41737${tag}${i}`, sku: 'TAG', copy: 1, text: 'For ' + id, approvedAt: Date.now() - 1000 - i, previewWPt: 12 * MM, previewHPt: 12 * MM, preview: `${origin}/eng-back-${n++}.png` }));
      };
      const k14 = S.sheets.gold14k; fill(k14, 'eng-14a', 3, 'a');
      const p2 = Object.assign(makeSheet('gold14k', 2), {}); fill(p2, 'eng-14b', 2, 'b'); k14.pages.push(p2);
      fill(S.sheets.gold, 'eng-gf', 0, 'g');
      fill(S.sheets.silver, 'eng-ss', 4, 's');
      CN.setMode('nest'); for (const m of ['gold14k', 'gold', 'silver']) { for (const p of S.sheets[m].pages) p.el = null; S.sheets[m].active = 0; S.sheets[m].pages[0].el = S.sheets[m].cardEl; }
      refreshAllCards();
    }, srv.sorterOrigin);
    // what a card shows of its shelf and its control
    const look = (pg, metal) => pg.evaluate(m => {
      const card = document.querySelector(`.sheetCard[data-m="${m}"]`), shelf = card.querySelector('[data-r="backs"]'), host = card.querySelector('[data-r="eng"]'), btn = host.querySelector('button');
      const r = shelf.getBoundingClientRect(), thumb = shelf.querySelector('.backThumb'), tr = thumb && thumb.getBoundingClientRect();
      return { attr: shelf.getAttribute('data-eng'), h: Math.round(r.height), vis: getComputedStyle(shelf).visibility, thumbs: shelf.querySelectorAll('.backPieces figure').length, thumbH: tr ? Math.round(tr.height) : 0,
        control: !host.hidden && !!btn && host.getBoundingClientRect().width > 0, expanded: btn && btn.getAttribute('aria-expanded'), pressed: btn && btn.getAttribute('aria-pressed'), count: btn && btn.textContent.trim(), controlsH: card.querySelector('.shControls').getBoundingClientRect().height };
    }, metal);
    let pressHits = 0;   // picture requests made while a press (or a key) was being handled and its shelf eased: there must be none
    const counted = async fn => { const b = backHits; await fn(); await tick(120); pressHits += backHits - b; };
    const press = (pg, metal) => counted(() => pg.click(`.sheetCard[data-m="${metal}"] [data-r="eng"] button`));
    const settle = pg => pg.waitForFunction(() => !document.getAnimations().some(a => a.playState === 'running' && a.effect && a.effect.target && a.effect.target.matches && a.effect.target.matches('[data-r="backs"]')));

    // ── before: the shelf as it was (the control left out)
    if (shots) {
      const o = await open({ noToggle: true }); await seed(o.pg); await tick(600);
      const card = await o.pg.$('.sheetCard[data-m="gold14k"]'); await card.screenshot({ path: path.join(shots, 'engraving-toggle-nest-before.png') });
      await o.context.close();
    }

    const { context, pg } = await open();
    await seed(pg); await tick(500);
    // 1 · every card starts collapsed
    let a = await look(pg, 'gold14k'), g = await look(pg, 'gold'), s = await look(pg, 'silver');
    assert.equal(a.thumbs, 3); assert.equal(a.attr, 'closed'); assert.equal(a.h, 0, 'collapsed: the shelf takes no height'); assert.equal(a.vis, 'hidden'); assert.equal(a.control, true); assert.equal(a.expanded, 'false'); assert.equal(a.pressed, 'false'); assert.equal(a.count, '3');
    assert.equal(s.attr, 'closed'); assert.equal(s.h, 0); assert.equal(s.count, '4');
    // no engraving: no control (and no shelf)
    assert.equal(g.thumbs, 0); assert.equal(g.control, false, 'a sheet without engraving has no control'); assert.equal(g.h, 0);
    console.log('  ✓ page: every sheet loads collapsed; a sheet with no engraving has no control');
    // 2 · open one: only that sheet's shelf
    await press(pg, 'gold14k'); await settle(pg);
    a = await look(pg, 'gold14k'); s = await look(pg, 'silver');
    assert.equal(a.attr, 'open'); assert(a.h > 20, `open: the shelf has height (${a.h})`); assert.equal(a.vis, 'visible'); assert(a.thumbH > 10, 'its thumbnails are drawn'); assert.equal(a.expanded, 'true'); assert.equal(a.pressed, 'true');
    assert.equal(s.attr, 'closed'); assert.equal(s.h, 0, 'the other card stays collapsed'); assert.equal(s.expanded, 'false');
    // 3 · a redraw (the card's repaint, the live refresh) replaces the shelf's children: the choice stays
    await pg.evaluate(() => { document.querySelector('.sheetCard[data-m="gold14k"] [data-r="backs"] .backPieces').__mark = 1; });
    await pg.evaluate(() => { renderCard(activePage('gold14k')); refreshAllCards(); Engrave.refreshBacks(); });
    await pg.waitForFunction(() => !document.querySelector('.sheetCard[data-m="gold14k"] [data-r="backs"] .backPieces').__mark);   // (really drawn again)
    a = await look(pg, 'gold14k'); assert.equal(a.attr, 'open'); assert(a.h > 20); assert.equal(a.expanded, 'true'); assert.equal(a.count, '3');
    s = await look(pg, 'silver'); assert.equal(s.attr, 'closed');
    // 4 · the tabs: sheet 2 of the same card is its own sheet (collapsed), and sheet 1 is still open when it comes back
    await pg.click('.sheetCard[data-m="gold14k"] .shTabs button[data-i="1"]'); await tick(150);
    a = await look(pg, 'gold14k'); assert.equal(a.count, '2'); assert.equal(a.attr, 'closed', 'sheet 2 has its own state: collapsed'); assert.equal(a.h, 0); assert.equal(a.expanded, 'false');
    await press(pg, 'gold14k'); await settle(pg); a = await look(pg, 'gold14k'); assert.equal(a.attr, 'open'); assert(a.h > 20);
    await pg.click('.sheetCard[data-m="gold14k"] .shTabs button[data-i="0"]'); await tick(150);
    a = await look(pg, 'gold14k'); assert.equal(a.count, '3'); assert.equal(a.attr, 'open', 'sheet 1 kept its choice across the tab round trip');
    // 5 · press again: hidden; the keyboard does the same
    await press(pg, 'gold14k'); await settle(pg); a = await look(pg, 'gold14k'); assert.equal(a.attr, 'closed'); assert.equal(a.h, 0); assert.equal(a.expanded, 'false');
    await pg.focus('.sheetCard[data-m="silver"] [data-r="eng"] button'); await counted(() => pg.keyboard.press('Enter')); await settle(pg);
    s = await look(pg, 'silver'); assert.equal(s.attr, 'open', 'Enter opens'); assert(s.h > 20);
    await counted(() => pg.keyboard.press('Space')); await settle(pg); s = await look(pg, 'silver'); assert.equal(s.attr, 'closed', 'Space closes'); assert.equal(s.h, 0);
    // 6 · the toggles asked the network for nothing, and the shelf is only looked at: the sheet's picture and its back view are untouched
    assert.equal(pressHits, 0, `toggling made ${pressHits} request(s) for back pictures`);
    const face = await pg.evaluate(() => { const cv = document.querySelector('.sheetCard[data-m="gold14k"] [data-r="canvas"]'); return { face: cv._face || 'front', w: cv.width }; });
    assert.equal(face.face, 'front');
    console.log(`  ✓ page: a press opens that sheet's shelf only, again hides it; kept across a redraw and the tabs; Enter/Space work; ${pressHits} picture requests while toggling`);
    // 7 · the controls row: as tall with the control as without it, at 320, 480 and 900 px
    for (const width of [320, 480, 900]) {
      await pg.setViewportSize({ width, height: 900 }); await tick(250);
      const r = await pg.evaluate(() => {
        const out = {};
        for (const m of ['gold14k', 'silver']) {
          const card = document.querySelector(`.sheetCard[data-m="${m}"]`), row = card.querySelector('.shControls'), host = card.querySelector('[data-r="eng"]'), face = card.querySelector('.shFace'), btn = host.querySelector('button');
          const withIt = row.getBoundingClientRect().height; host.hidden = true; const without = row.getBoundingClientRect().height; host.hidden = false;
          const hr = btn.getBoundingClientRect(), fr = face.hidden ? null : face.getBoundingClientRect(), rr = row.getBoundingClientRect();
          out[m] = { withIt, without, btnH: hr.height, btnW: Math.round(hr.width), inside: hr.left >= rr.left - 1 && hr.right <= rr.right + 1, noOverlap: !fr || hr.right <= fr.left + 1 || hr.left >= fr.right - 1, faceFits: !!fr && fr.left >= rr.left - 1 && fr.right <= rr.right + 1, shown: !host.hidden, at: `btn ${Math.round(hr.left)}-${Math.round(hr.right)} row ${Math.round(rr.left)}-${Math.round(rr.right)} face ${fr ? Math.round(fr.left) + '-' + Math.round(fr.right) : 'hidden'}` };
        }
        out.docOverflow = document.documentElement.scrollWidth - document.documentElement.clientWidth;
        return out;
      });
      for (const m of ['gold14k', 'silver']) {
        assert.equal(r[m].withIt, r[m].without, `${m} @${width}px: the controls row is ${r[m].withIt}px with the control and ${r[m].without}px without`);
        assert(r[m].btnH <= r[m].withIt, `${m} @${width}px: the control fits the row (${r[m].btnH} in ${r[m].withIt})`); assert(!r[m].faceFits || (r[m].inside && r[m].noOverlap), `${m} @${width}px: inside the row and clear of Front | Back wherever Front | Back itself fits (${r[m].at})`);
      }
      console.log(`  ✓ page @${width}px: controls row ${r.gold14k.withIt}px with and without; the control is ${r.gold14k.btnW}×${Math.round(r.gold14k.btnH)}px (${r.gold14k.at}; Front | Back ${r.gold14k.faceFits ? 'fits' : 'is already wider than the row at this width'})`);
    }
    await pg.setViewportSize({ width: 1440, height: 900 }); await tick(250);
    // pictures: collapsed (as every load starts), then opened
    if (shots) {
      const card = await pg.$('.sheetCard[data-m="gold14k"]');
      await card.screenshot({ path: path.join(shots, 'engraving-toggle-nest-after-collapsed.png') });
      await press(pg, 'gold14k'); await settle(pg); await tick(200); await card.screenshot({ path: path.join(shots, 'engraving-toggle-nest-after-open.png') });
      await press(pg, 'gold14k'); await settle(pg);
    }
    // 8 · open one, then reload: not remembered, every load starts collapsed
    await press(pg, 'silver'); await settle(pg); assert.equal((await look(pg, 'silver')).attr, 'open');
    assert.equal(await pg.evaluate(() => { let n = 0; for (const st of [localStorage, sessionStorage]) for (let i = 0; i < st.length; i++) if (/eng/i.test(st.key(i))) n++; return n; }), 0, 'nothing about it in the browser storage');
    await pg.reload({ waitUntil: 'load' }); await pg.waitForFunction(() => window.EngravingToggle && window.CN && CN.S.cloud.ok === true, null, { timeout: 60000 });
    assert.equal(await pg.evaluate(() => EngravingToggle.isOpen('eng-ss')), false, 'a new load remembers nothing');
    await seed(pg); await tick(500); s = await look(pg, 'silver'); assert.equal(s.attr, 'closed'); assert.equal(s.h, 0); assert.equal(s.expanded, 'false');
    console.log('  ✓ page: not remembered after a reload; every load starts collapsed');
    assert.deepEqual(errors, [], 'no page errors');
    await context.close();
  } finally { await browser.close(); await srv.close(); }
}

(async () => { await component(); await page(); console.log('PASS: engraving-toggle'); })().catch(err => { console.error(err); process.exitCode = 1; });
