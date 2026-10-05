/* The Library's show/hide of each sheet's back-engraving shelf (charm-nest-library-engraving.js + the shared charm-nest-engraving-toggle.js).
   Paul, 5 Oct 2026, 03:33 UTC: "Add a colapsable button functionality to hide/show all of the back engraving on top of a given sheet.
   Add this new feature to both the Nest tab sheets and the Library Tab Sheets. Make the UI small and elegant so not to crowd the
   existing UI. By default keep it collapsed."
   What "the back engraving on top of a sheet" is on a Library card: the shelf of back-engraving thumbnails above the sheet's picture
   ([data-back-sheet] > .sheetBacks > .backPieces, one figure per piece; CharmNestBacks.markup), as in his screenshot.
   The page is the app's own shell and CSS (charm-nest-1.html with its scripts taken out) around the REAL Library code: LaserReview, the
   Sets view's card code and renderLibrary, charm-nest-library.js (which calls the hook), charm-nest-library-dnd.js (the grip), the REAL
   CharmNestBacks.markup for the shelf, the real Engrave.refreshBacks text from the bridge, and the shared toggle component itself.
   Offline: every request that is not about:/data: is refused and counted. What it holds to:
     1  every card with engraving starts collapsed (the shelf has no height, the control says how many pieces), a card without
        engraving has no control and no change at all
     2  a press shows the shelf (the picture is untouched), another hides it, and neither opens the sheet window behind it
     3  one sheet at a time: opening one leaves the others as they were
     4  the choice survives a live repaint (LaserReview.changed), the cards drawn again (the Library's next read) and Engrave.refreshBacks
        writing a shelf anew (a piece added: the count follows); a closed card is not touched by a repaint (no mutation inside it)
     5  nothing is stored: no localStorage / sessionStorage / cookie; a fresh page starts collapsed again
     6  a sheet open on the Nest tab and in the Library is one choice (EngravingToggle.set / isOpen), both ways
     7  no added height: the header is as tall with the control as without, at 1440, 900, 390 and 320 px wide; a collapsed card is as tall as
        a card with no engraving; "pieces", never "lines"
     8  the grip still drags the card (LibraryDnd); a press and a move on the control starts no drag
     9  300 sheets: no request, one read of each list, drawn within the time budget of the page without the control
   10  without the component on the page nothing changes: no control, the shelf shows as it always did
     node tests/charm-nest/library-engraving-toggle.cjs [playwright-core dir]     (PW_DIR=…, CHROMIUM=…, TOGGLE_JS=<path to the component while it is not in the repo>; SHOTS=<dir> saves screenshots) */
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const F = require('./library-issues-fixture.cjs');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const pwDir = process.argv[2] || process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules', '/opt/node22/lib/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core'))) || path.join(root, 'node_modules');
let chromium;
try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '';
const TOGGLE = fs.existsSync(path.join(root, 'charm-nest-engraving-toggle.js')) ? read('charm-nest-engraving-toggle.js') : process.env.TOGGLE_JS && fs.existsSync(process.env.TOGGLE_JS) ? fs.readFileSync(process.env.TOGGLE_JS, 'utf8') : null;
if (!TOGGLE) { console.error('charm-nest-engraving-toggle.js is not in the repo yet (set TOGGLE_JS=<path> to test against a copy)'); process.exit(1); }

const WORDS = ['Jessica', 'I dissent', 'AY', 'Charlie', 'Lucky', '143', 'KMB SMH', 'Dr. Lara', 'Daddio', 'Canada', 'Sonia', 'Jesus', 'Päivi', 'JLP', 'S', 'Finn', 'C', 'T', 'R', '2030', 'Go Birds'];
const STUBS = `
window.__calls = []; window.__api = []; window.__requests = [];
window.matchMedia = window.matchMedia || (() => ({ matches: false }));
Object.assign(window, {
  S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: true } },
  api: async (name, b) => { window.__api.push(b.op); return b.op === 'setList' ? { sets: window.__rawSets || [], sheets: [] } : b.op === 'listSheets' ? { sheets: window.__sheets || [] } : { sheets: [], sets: [], counts: {} }; },
  allSheets: () => [], Orders: { rows: () => window.__rows || [] },
  esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
  el: (tag, cls) => { const e = document.createElement(tag); e.className = cls; return e; }, Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' },
  toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, pvRatio: () => '', dayShort: x => x, metalOf: r => ({ label: r.metal }),
  sheetHead: r => '<div class="h"><span class="sw" style="--c:#c8a24e">GF</span><span class="nm">Sheet ' + r.sheetIndex + '</span><span class="tm">Oct 2</span></div>',
  openLibrarySheet: id => { window.__calls.push(['sheet', id]); return true; }, openOrderFrom: () => true, setMode: () => {}, showLibrary: () => {}, loadLibrary: () => Promise.resolve()
});
// the thumbnails of the shelf: small drawn pictures (a charm's outline and its words), a handful shared by every piece
(() => { const out = [], words = ${JSON.stringify(WORDS)}; for (let i = 0; i < words.length; i++) { const c = document.createElement('canvas'); c.width = 120; c.height = 100; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 120, 100); x.strokeStyle = '#2a2724'; x.lineWidth = 2; x.beginPath(); x.ellipse(60, 54, 44 - (i % 3) * 4, 32 + (i % 4) * 3, 0, 0, 7); x.stroke(); x.beginPath(); x.arc(60, 12, 6, 0, 7); x.stroke(); x.fillStyle = '#2f2512'; x.font = '700 15px Georgia, serif'; x.textAlign = 'center'; x.fillText(words[i], 60, 60, 74); out.push(c.toDataURL('image/png')); } window.__thumbs = out; })();
window.Engrave = {
  items: () => new Map(), loadBackPreview: () => Promise.reject(new Error('no network')),
  backsMarkup: s => CharmNestBacks.markup((s.backPool || []).map((b, i) => Object.assign({}, b, { sheetId: s.id, setId: s.setId, order: String(b.poolId).split('_')[0], sku: 'MIDDLE_9935', copy: 1, text: 'Jessica', preview: window.__thumbs[i % window.__thumbs.length], previewWPt: 64, previewHPt: 56 })), { wPt: 283.46, hPt: 141.73 })
};`;

/* sheets: ready ones carry `n` engraved pieces, plain ones none (no engraving: no control) */
const mkSheet = (id, k, i, n) => {
  const pool = [...Array(Math.max(n, 6))].map((_, j) => `${4100000000 + (k * 3 + i) * 100 + j}_t${j}_1`), orders = pool.map(p => p.split('_')[0]);
  return { id, metal: 'gold', metalLabel: 'GF 14/20', setId: 'set-' + k, setSeq: k + 1, runId: 'run1', sheetIndex: i + 1, status: 'complete', poolIds: pool, placedCount: pool.length, charmCount: pool.length, density: .73, verification: { ok: true }, preview: F.sheetPicture(), outputs: { ai: 'https://example.com/f.ai' },
    orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] }, backPool: pool.slice(0, n).map(p => F.back(p, id)), engraving: {}, orderReadiness: Object.fromEntries(orders.map(o => [o, { ready: true }])), updatedAt: 1, day: '2026-10-03', folder: `GF_Oct.03.26_Set-${k + 1}_Sheet-${i + 1}` };
};
/* a Library of `sets` sets of `per` sheets: sheet 1 of each set has 29 pieces, sheet 2 has 3, sheet 3 (when there is one) none */
function records(sets, per, mix = [29, 3, 0]) {
  const sheets = [], rawSets = [];
  for (let k = 0; k < sets; k++) {
    const mine = [...Array(per)].map((_, i) => mkSheet(`s${k}x${i}`, k, i, mix[i % mix.length]));
    sheets.push(...mine); rawSets.push({ setId: 'set-' + k, seq: k + 1, day: '2026-10-03', runId: 'run1', sheetIds: mine.map(s => s.id), materials: ['gold'], status: 'labelled', updatedAt: 1 });
  }
  const rows = []; for (const s of sheets) for (const p of s.poolIds) rows.push({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: 'Buyer' } }, state: 'written', poolIds: [p], engrave: { needed: true, state: 'approved', approved: true } });
  return { sheets, rawSets, rows };
}

async function open(browser, { width = 1440, height = 900, sets = 1, per = 3, mix, component = true, dnd = false, withSet = false, errors = [], requests = [] } = {}) {
  const context = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2 });
  const page = await context.newPage();
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  page.on('console', m => { if (m.type() === 'warning' || m.type() === 'error') { const t = m.text(); if (!/Failed to load resource/.test(t)) errors.push('console: ' + t); } });
  await page.addInitScript(() => { window.__stored = []; try { Storage.prototype.setItem = function (k) { window.__stored.push('storage:' + k); }; const real = indexedDB.open.bind(indexedDB); indexedDB.open = function (...a) { window.__stored.push('indexedDB:' + a[0]); return real(...a); }; Object.defineProperty(document, 'cookie', { configurable: true, get: () => '', set: v => { window.__stored.push('cookie:' + v); } }); } catch (_) {} });
  await page.route(u => !/^about:|^data:/.test(u.href), r => { requests.push(r.request().url()); r.abort(); });
  await page.setContent(F.shell(), { waitUntil: 'domcontentloaded' });
  await page.addScriptTag({ content: STUBS });
  if (withSet) await page.evaluate(() => { window.sheetHead = r => '<div class="h"><span class="sw" style="--c:#c8a24e">GF</span><span class="nm">Sheet ' + r.sheetIndex + '</span><span class="set">Set ' + r.setSeq + '</span><span class="tm" title="Sheet started">Sep 30</span></div>'; });
  const { sheets, rawSets, rows } = records(sets, per, mix);
  await page.evaluate(({ sheets, rawSets, rows }) => { window.__sheets = sheets; window.__rawSets = rawSets; window.__rows = rows; }, { sheets, rawSets, rows });
  for (const f of ['charm-nest-backs.js', 'charm-nest-orders.js']) await page.addScriptTag({ content: read(f) });
  await page.evaluate(() => { window.O = window.CharmNestOrders; });
  for (const f of ['charm-nest-readiness.js', 'charm-nest-activity.js', 'charm-nest-motion.js']) await page.addScriptTag({ content: read(f) });
  const bridge = read('charm-nest-bridge.js');
  await page.addScriptTag({ content: bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')) });
  const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
  await page.addScriptTag({ content: 'window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {renderLibrary,libraryCard};})();' });
  // the bridge's own Engrave.refreshBacks, verbatim, over the page's stand-ins for what it reads
  const r0 = bridge.indexOf('  function refreshBacks() {'), r1 = bridge.indexOf('  function reconcileSheet', r0);
  await page.addScriptTag({ content: '(()=>{const refreshAllCards=()=>{},Session={schedule(){}},backsMarkup=s=>Engrave.backsMarkup(s);' + bridge.slice(r0, r1) + ';Engrave.refreshBacks=refreshBacks;})();' });
  if (component) await page.addScriptTag({ content: TOGGLE });
  await page.addScriptTag({ content: read('charm-nest-library.js') });
  if (dnd) {
    await page.evaluate(() => { window.LibraryFlow = { targets: () => [{ area: 'progress' }, { area: 'laser' }, { area: 'completed' }], plan: async q => ({ ok: true, kind: q.kind, id: q.id, move: q, from: {}, to: {}, auto: [], needs: [], confirm: [], notes: [] }), commit: async () => ({ ok: true, applied: [] }) }; });
    await page.addScriptTag({ content: read('charm-nest-library-dnd.js') });
  }
  await page.addScriptTag({ content: read('charm-nest-library-engraving.js') });
  const t0 = Date.now();
  await page.evaluate(async () => { const body = document.getElementById('libBody'); const t = performance.now(); await window.Sets.renderLibrary(body); window.__renderMs = performance.now() - t; });
  await page.waitForSelector('.libCard'); await page.waitForTimeout(300);
  return { page, context, ms: Date.now() - t0 };
}
const shot = async (page, name, sel) => { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); const el = sel && await page.$(sel); await (el ? el.screenshot({ path: path.join(SHOTS, name + '.png') }) : page.screenshot({ path: path.join(SHOTS, name + '.png') })); };
const info = (page, id) => page.evaluate(id => {
  const card = document.querySelector(`.libCard[data-id="${id}"]`); if (!card) return null;
  const sh = card.querySelector(':scope > [data-back-sheet]'), h = card.querySelector(':scope > .h'), tog = h && h.querySelector('.engTog'), btn = tog && tog.querySelector('button'), r = e => e && e.getBoundingClientRect();
  return { has: !!tog, hidden: !!(tog && tog.parentElement.hidden), eng: sh && sh.getAttribute('data-eng'), shelfH: sh ? r(sh).height : 0, pieces: sh ? sh.querySelectorAll('.backPieces figure').length : 0, headH: r(h).height, cardH: r(card).height, cardW: r(card).width, pressed: btn && btn.getAttribute('aria-pressed'), expanded: btn && btn.getAttribute('aria-expanded'), n: btn && btn.querySelector('.engN') && btn.querySelector('.engN').textContent, label: btn && (btn.getAttribute('aria-label') + ' | ' + btn.title), btnBox: btn && (b => ({ l: b.left, r: b.right, t: b.top, b: b.bottom }))(r(btn)), headBox: (b => ({ l: b.left, r: b.right, t: b.top, b: b.bottom }))(r(h)), picH: r(card.querySelector('img.pv')).height, picT: r(card.querySelector('img.pv')).top };
}, id);

const sleep = ms => new Promise(r => setTimeout(r, ms));
const press = async (page, id) => { await page.click(`.libCard[data-id="${id}"] .engTog button`); await sleep(520); };   // (the shelf eases for 200 ms)
const rerender = page => page.evaluate(async () => { await window.Sets.renderLibrary(document.getElementById('libBody')); await new Promise(r => setTimeout(r, 120)); });

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    /* 1–5, 7: one Library of two sets, three sheets each: 29 pieces · 3 pieces · none */
    { const errors = [], requests = [];
      const { page, context } = await open(browser, { sets: 2, dnd: true, errors, requests });
      // 1. collapsed by default; a sheet with no engraving has no control and no change
      const a = await info(page, 's0x0'), b = await info(page, 's0x1'), none = await info(page, 's0x2');
      for (const [x, n] of [[a, 29], [b, 3]]) { assert.equal(x.has, true, 'a sheet with engraving carries the control'); assert.equal(x.eng, 'closed', 'collapsed by default'); assert.equal(x.shelfH, 0, 'the shelf has no height'); assert.equal(x.pressed, 'false'); assert.equal(x.n, String(n), 'the control says how many pieces'); assert.match(x.label, new RegExp(`\\b${n} pieces\\b`)); assert.doesNotMatch(x.label, /\blines?\b/i, 'pieces, never lines'); assert.equal(x.pieces, n); }
      assert.equal(none.has, false, 'no engraving: no control'); assert.equal(none.eng, null, 'and its shelf host is left alone'); assert.equal(await page.evaluate(() => document.querySelectorAll('.libCard[data-id="s0x2"] .engLibHost, .libCard[data-id="s0x2"] .engTog').length), 0);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.libCard .engTog').length), 4, 'one control per sheet that has engraving (2 sets × 2 sheets)');
      // 7 (desktop): no added height, the sheet's picture drawn clean and where it would be without engraving
      assert.equal(a.headH, none.headH, `the header is as tall with the control as without (${a.headH} vs ${none.headH})`); assert.equal(b.headH, none.headH);
      assert(Math.abs(a.cardH - none.cardH) < .6, `a collapsed card is as tall as a card with no engraving (${a.cardH} vs ${none.cardH})`);
      assert(Math.abs((a.picT - a.headBox.t) - (none.picT - none.headBox.t)) < .6, 'the picture sits just where it does on a card with no engraving');
      assert(a.btnBox.l >= a.headBox.l && a.btnBox.r <= a.headBox.r && a.btnBox.t >= a.headBox.t - 4 && a.btnBox.b <= a.headBox.b + 4, 'the control is inside the header');
      await shot(page, 'collapsed', '.setCard');

      // 2. a press shows the shelf, another hides it; the picture is untouched; nothing opens behind the control
      await press(page, 's0x0'); let x = await info(page, 's0x0');
      assert.equal(x.eng, 'open'); assert(x.shelfH > 100, `the shelf shows (${x.shelfH} px)`); assert.equal(x.pressed, 'true'); assert.equal(x.expanded, 'true'); assert.equal(x.picH, a.picH, 'the sheet\'s picture is untouched');
      assert.equal(await page.evaluate(() => document.querySelectorAll('.libCard[data-id="s0x0"] .sheetBacks .backPieces figure').length), 29, 'every piece is in the shelf');
      assert.deepEqual(await page.evaluate(() => window.__calls), [], 'the sheet window did not open behind the press');
      await shot(page, 'open', '.setCard');
      await press(page, 's0x0'); x = await info(page, 's0x0'); assert.equal(x.eng, 'closed'); assert.equal(x.shelfH, 0); assert.equal(x.pressed, 'false'); assert(Math.abs(x.cardH - none.cardH) < .6, 'and the card is as short as before');
      // keyboard: the control is a button
      await page.focus('.libCard[data-id="s0x1"] .engTog button'); await page.keyboard.press('Enter'); await sleep(450); assert.equal((await info(page, 's0x1')).eng, 'open', 'Enter opens'); await page.keyboard.press('Space'); await sleep(450); assert.equal((await info(page, 's0x1')).eng, 'closed', 'Space closes');
      assert.deepEqual(await page.evaluate(() => window.__calls), []);

      // 3. one sheet at a time
      await press(page, 's0x0'); assert.deepEqual([await info(page, 's0x0'), await info(page, 's0x1'), await info(page, 's1x0')].map(i => i.eng), ['open', 'closed', 'closed'], 'opening one sheet leaves the others as they were');
      await press(page, 's0x1'); await press(page, 's0x0'); assert.deepEqual([await info(page, 's0x0'), await info(page, 's0x1')].map(i => i.eng), ['closed', 'open'], 'closing one leaves the other open');
      await press(page, 's0x0'); await press(page, 's1x1');   // open now: s0x0, s0x1, s1x1

      // 4. survives a live repaint, the cards drawn again, and a shelf written anew; a closed card is not touched by a repaint
      const state = async () => (await Promise.all(['s0x0', 's0x1', 's0x2', 's1x0', 's1x1'].map(i => info(page, i)))).map(i => i.eng);
      assert.deepEqual(await state(), ['open', 'open', null, 'closed', 'open']);
      await page.evaluate(() => { window.__mut = []; const mo = new MutationObserver(l => { for (const m of l) { const t = m.target.nodeType === 1 ? m.target : m.target.parentElement; if (t && t.closest && t.closest('.libCard > [data-back-sheet], .libCard > .h')) window.__mut.push(m.type + ':' + (t.className || t.tagName) + ':' + (m.attributeName || '')); } }); mo.observe(document.getElementById('libBody'), { subtree: true, childList: true, attributes: true, characterData: true }); window.__mo = mo; });
      await page.evaluate(() => window.LaserReview.changed()); await sleep(700);
      assert.deepEqual(await page.evaluate(() => window.__mut), [], 'a live repaint writes nothing into any shelf or header (closed cards are not redrawn)'); assert.deepEqual(await state(), ['open', 'open', null, 'closed', 'open'], 'the choices survive the live repaint');
      await page.evaluate(() => { window.__mo.disconnect(); window.__old = document.querySelector('.libCard[data-id="s0x0"]'); });
      await rerender(page);
      assert.equal(await page.evaluate(() => window.__old !== document.querySelector('.libCard[data-id="s0x0"]')), true, 'the cards are new elements (the Library read its list again)');
      assert.deepEqual(await state(), ['open', 'open', null, 'closed', 'open'], 'each sheet is drawn again in its own state'); assert.equal(await page.evaluate(() => document.querySelectorAll('.libCard .engTog').length), 4, 'one control each, none doubled');
      assert.equal(await page.evaluate(() => window.LibraryEngraving.size()), 4, 'the old cards\' controls were let go');
      // the shelf written anew (Engrave.refreshBacks, the bridge's own text): the host keeps its state, the count follows; a first piece brings the control, the last one takes it away
      await page.evaluate(() => { const rec = id => window.__sheets.find(s => s.id === id); rec('s0x0').poolIds.push('x_29_1'); rec('s0x0').backPool.push({ poolId: 'x_29_1', sheetId: 's0x0', approvedAt: 10 }); rec('s0x2').backPool = [{ poolId: rec('s0x2').poolIds[0], sheetId: 's0x2', approvedAt: 10 }]; rec('s0x1').backPool = []; window.S.library.rows = window.__sheets; Engrave.refreshBacks(); });
      x = await info(page, 's0x0'); assert.equal(x.eng, 'open', 'a shelf written anew keeps its state'); assert.equal(x.n, '30', 'and the control counts its new piece'); assert.equal(x.pieces, 30);
      x = await info(page, 's0x2'); assert.equal(x.has, true, 'a sheet that gets its first engraving gets the control'); assert.equal(x.eng, 'closed', 'collapsed'); assert.equal(x.n, '1'); assert.match(x.label, /\b1 piece\b/, '1 piece, singular');
      x = await info(page, 's0x1'); assert.equal(x.hidden, true, 'a sheet whose engraving is gone has no control to see'); assert.equal(x.pieces, 0);
      await page.evaluate(() => { const rec = id => window.__sheets.find(s => s.id === id); rec('s0x1').backPool = rec('s0x1').poolIds.slice(0, 3).map(p => ({ poolId: p, sheetId: 's0x1', approvedAt: 10 })); Engrave.refreshBacks(); });
      x = await info(page, 's0x1'); assert.equal(x.hidden, false, 'and it is back when the pieces are'); assert.equal(x.eng, 'open', 'in the state the sheet had'); assert.equal(x.n, '3');

      // 5. nothing is stored
      assert.deepEqual(await page.evaluate(() => window.__stored), [], 'nothing was written to the browser\'s storage, cookies or IndexedDB');
      assert(!/localStorage|sessionStorage|indexedDB|document\.cookie/.test((TOGGLE + read('charm-nest-library-engraving.js')).replace(/\/\*[\s\S]*?\*\//g, '')), 'the code does not use storage');
      assert.deepEqual(errors, []); await context.close();
      const fresh = await open(browser, { sets: 2, errors, requests });
      assert.deepEqual(await Promise.all(['s0x0', 's0x1', 's0x2', 's1x0', 's1x1'].map(async i => (await info(fresh.page, i)).eng)), ['closed', 'closed', null, 'closed', 'closed'], 'a fresh page load starts collapsed again');
      assert.deepEqual(errors, []); await fresh.context.close();
      assert.deepEqual(requests, [], 'no request went anywhere');
    }

    /* 6. one choice with the Nest tab: the component's isOpen/set by sheet id, and its event */
    { const errors = [], { page, context } = await open(browser, { sets: 1, errors });
      await page.evaluate(() => { window.__ev = []; window.addEventListener('engravingtoggle', e => window.__ev.push([e.detail.sheetKey, e.detail.open])); });
      await page.evaluate(() => EngravingToggle.set('s0x1', true)); await sleep(450);   // (the Nest tab's card for this sheet was opened)
      let x = await info(page, 's0x1'); assert.equal(x.eng, 'open', 'a sheet opened on the Nest tab is open in the Library'); assert.equal(x.pressed, 'true'); assert.equal((await info(page, 's0x0')).eng, 'closed', 'and no other sheet is');
      await press(page, 's0x1'); assert.equal(await page.evaluate(() => EngravingToggle.isOpen('s0x1')), false, 'a press in the Library is the sheet\'s choice for the Nest tab too');
      await press(page, 's0x0'); assert.equal(await page.evaluate(() => EngravingToggle.isOpen('s0x0')), true);
      assert.deepEqual(await page.evaluate(() => window.__ev), [['s0x1', true], ['s0x1', false], ['s0x0', true]], 'each choice is announced once, by sheet id');
      // a card drawn when the sheet is already open on the Nest tab comes up open
      await page.evaluate(() => EngravingToggle.set('s0x1', true)); await sleep(300); await rerender(page); assert.equal((await info(page, 's0x1')).eng, 'open');
      assert.deepEqual(errors, []); await context.close(); }

    /* 7. no added height, at every width; one-line headers stay one line (also with the Set tag the sheet list shows) */
    for (const withSet of [false, true]) for (const width of [1440, 900, 390, 320]) {
      const errors = [], { page, context } = await open(browser, { sets: 1, width, dnd: true, withSet, errors });
      const a = await info(page, 's0x0'), b = await info(page, 's0x1'), none = await info(page, 's0x2');
      const head = await page.evaluate(() => { const g = id => document.querySelector(`.libCard[data-id="${id}"] > .h`); const m = id => [...g(id).children].map(c => ({ c: c.className, w: Math.round(c.getBoundingClientRect().width), t: Math.round(c.getBoundingClientRect().top) })); return { a: m('s0x0'), none: m('s0x2') }; });
      assert.equal(a.headH, none.headH, `${withSet ? 'with the Set tag, ' : ''}${width} px: the header is as tall with the control as without (${a.headH} vs ${none.headH}): ${JSON.stringify(head)}`); assert.equal(b.headH, none.headH);
      assert(Math.abs(a.cardH - none.cardH) < .6, `${width} px: a collapsed card is as tall as one with no engraving (${a.cardH} vs ${none.cardH})`);
      const tops = await page.evaluate(() => [...document.querySelectorAll('.libCard[data-id="s0x0"] > .h > *')].filter(c => !c.hidden).map(c => Math.round(c.getBoundingClientRect().top))); assert(Math.max(...tops) - Math.min(...tops) <= 5, `${width} px: the header is one line (${tops})`);
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${width} px: no sideways page scroll`);
      assert(a.btnBox.r <= a.headBox.r + .5 && a.btnBox.l >= a.headBox.l - .5, `${width} px: the control is inside its card`);
      const clear = await page.evaluate(() => { const h = document.querySelector('.libCard[data-id="s0x0"] > .h'), b = h.querySelector('.engTog').getBoundingClientRect(), o = [...h.children].filter(c => !c.contains(h.querySelector('.engTog')) && !c.hidden).map(c => c.getBoundingClientRect()); return o.every(r => r.right <= b.left + .5 || r.left >= b.right - .5 || r.width === 0); });
      assert(clear, `${width} px: the control overlaps nothing in the header`);
      if (!withSet && width === 390) { await shot(page, 'narrow-collapsed', '.setCard'); await press(page, 's0x0'); await shot(page, 'narrow-open', '.setCard'); assert.equal((await info(page, 's0x0')).eng, 'open'); }
      assert.deepEqual(errors, []); await context.close(); }

    /* 8. the grip still drags; the control starts no drag */
    { const errors = [], { page, context } = await open(browser, { sets: 1, dnd: true, errors });
      const st = () => page.evaluate(() => ({ s: LibraryDnd.state(), attr: document.documentElement.hasAttribute('data-library-drag') }));
      const centre = sel => page.evaluate(sel => { const r = document.querySelector(sel).getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, sel);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.libCard[data-id="s0x0"] > .h > .dndGrip').length), 1, 'the grip is on the card');
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('.libCard[data-id="s0x0"] > .h > *')].map(c => c.className.split(' ')[0])), ['sw', 'nm', 'engLibHost', 'tm', 'dndGrip'], 'the control sits between the name and the date, the grip stays last');
      // the control: press down, move well past the drag distance
      let c = await centre('.libCard[data-id="s0x0"] .engTog button');
      await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.move(c.x + 12, c.y + 6, { steps: 3 }); await page.mouse.move(c.x + 60, c.y + 40, { steps: 6 });
      let s = await st(); assert.equal(s.s.dragging, false, 'a press and a move on the control starts no drag'); assert.equal(s.attr, false); assert.equal(await page.evaluate(() => !!document.querySelector('.dndLift, .mGhost')), false, 'no card is lifted');
      await page.mouse.up(); await sleep(100); assert.equal((await st()).s.dragging, false);
      // the grip: press down, move 60 px
      await page.evaluate(() => { window.__calls.length = 0; });
      await page.mouse.move(c.x - 100, c.y); c = await centre('.libCard[data-id="s0x1"] .dndGrip'); await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.move(c.x + 14, c.y + 8, { steps: 3 }); await page.mouse.move(c.x + 60, c.y + 50, { steps: 6 }); await sleep(120);
      s = await st(); assert.equal(s.s.dragging, true, 'the grip still starts the drag'); assert.equal(s.attr, true, 'and says so (data-library-drag)');
      await page.keyboard.press('Escape'); await page.mouse.up(); await sleep(500); assert.equal((await st()).s.dragging, false, 'Esc puts the card back');
      assert.deepEqual(await page.evaluate(() => window.__calls.filter(x => x[0] === 'sheet')), [], 'neither opened the sheet window');
      assert.deepEqual(errors, []); await context.close(); }

    /* 9. 300 sheets: no request, one read of each list, within the time budget of the page without the control */
    { const run = async component => { const errors = [], requests = [], { page, context } = await open(browser, { sets: 100, per: 3, mix: [28], component, dnd: true, errors, requests });
        const r = await page.evaluate(() => ({ ms: window.__renderMs, api: window.__api.slice(), cards: document.querySelectorAll('.libCard').length, ctl: document.querySelectorAll('.engTog').length, size: window.LibraryEngraving.size(), closed: document.querySelectorAll('.libCard > [data-back-sheet][data-eng="closed"]').length }));
        const again = await page.evaluate(() => { const t0 = performance.now(); LibraryEngraving.decorate(document); return performance.now() - t0; });   // (the Library drawn once more: nothing to do for the cards that have their control)
        assert.deepEqual(errors, []); assert.deepEqual(requests, [], 'no request: nothing per card goes to the network'); assert.deepEqual(r.api, ['setList', 'listSheets'], 'one read of each list');
        await context.close(); return { ...r, again }; };
      const withMs = [], withoutMs = []; let w, wo;
      for (let i = 0; i < 2; i++) { w = await run(true); withMs.push(w.ms); wo = await run(false); withoutMs.push(wo.ms); }
      assert.equal(w.cards, 300, '300 sheets are drawn'); assert.equal(w.ctl, 300, 'each carries its control'); assert.equal(w.size, 300); assert.equal(w.closed, 300, 'every one collapsed'); assert.equal(wo.ctl, 0); assert.equal(wo.closed, 0);
      const a = Math.min(...withMs), b = Math.min(...withoutMs);
      console.log(`  300 sheets: drawn in ${a.toFixed(0)} ms with the control, ${b.toFixed(0)} ms without; looking again at all 300 ${w.again.toFixed(1)} ms`);
      assert(a <= b * 1.35 + 150, `the Library draws 300 sheets within the time budget of the page without the control (${a.toFixed(0)} vs ${b.toFixed(0)} ms)`);
      assert(w.again < 80, `looking again at 300 cards that have their control costs ${w.again.toFixed(1)} ms (a few)`); }

    /* 10. without the component: no control, the shelf shows as it always did */
    { const errors = [], { page, context } = await open(browser, { sets: 1, component: false, errors });
      const x = await info(page, 's0x0'); assert.equal(x.has, false); assert.equal(x.eng, null); assert(x.shelfH > 100, 'the shelf shows'); assert.equal(await page.evaluate(() => window.LibraryEngraving.size() + document.querySelectorAll('.engLibHost').length), 0);
      await shot(page, 'before', '.setCard');
      assert.deepEqual(errors, []); await context.close(); }

    /* the hooks are where the Library draws and where the shelf is written */
    { const lib = read('charm-nest-library.js'), br = read('charm-nest-bridge.js'), html = read('charm-nest-1.html'), build = read('scripts/build-public.cjs');
      assert(/function cards\(root, mode\)[\s\S]*?LibraryEngraving\.decorate\(root\)[\s\S]*?function act\(/.test(lib), 'charm-nest-library.js calls the hook for every list it draws');
      assert(/function refreshBacks\(\)[\s\S]*?LibraryEngraving\?\.sync\?\.\(el\)[\s\S]*?function reconcileSheet/.test(br), 'Engrave.refreshBacks calls the hook after it writes a shelf');
      assert(/<script src="charm-nest-library-engraving\.js\?v=[^"]+"><\/script>/.test(html) && build.includes('"charm-nest-library-engraving.js"'), 'the file is on the page and in the build');
      if (/charm-nest-engraving-toggle\.js/.test(html)) assert(html.indexOf('charm-nest-engraving-toggle.js') < html.indexOf('charm-nest-library-engraving.js'), 'the component loads before the Library hook'); }
  } finally { await browser.close(); }
  console.log('Library engraving toggle OK: collapsed by default, a press shows and hides the shelf, one sheet at a time, survives live repaints, redraws and a shelf written anew, nothing stored, one choice with the Nest tab, no added height at 1440/900/390/320 px, the grip still drags, 300 sheets within budget, no request');
})().catch(e => { console.error(e); process.exitCode = 1; });
