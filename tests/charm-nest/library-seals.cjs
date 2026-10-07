// The Library's seals (Paul, 7 Oct 2026 00:01 UTC: "there are too many seals visible here. There should only be one seal per each sheet and only visible
// when a given sheet has completed the laser cutting process, no interim seals no duplicates ... make the seals a bit bigger and zoom a bit bigger so they
// are easily legible"). A DISPLAY rule over data the page already holds: the stamps stay in the records (and on the order timelines), nothing here deletes one.
//
// Real page (charm-nest-1.html, real modules) in headless Chromium over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary
// function over an in-memory Firestore). Nothing live is ever called. Fixture:
//   Set-1 (laser cutting): GF  completed after an undo and a second completion (FOUR stamps: ready, cut, ready, cut)  -> exactly ONE seal, the latest LASER CUT
//                          SS  approved (LASER READY) but not cut                                                       -> none
//                          RG  Rose Gold, approved, with a PARTIAL Cut Sheet press (roseCutAt, no completion)           -> none
//   a loose sheet completed twice (Completed, Sheets) and Set-2 completed (Completed, Sets: its row, then its two sheets)
// Checked at 1280, 700 and 390 px: one seal on the completed sheet only, none on the set header, 72 px, whole inside its card, covering neither the counts nor the
// "N / N" counter, zoom x1.8 in place (500 ms rest, a click or Enter at once, Esc and leaving put it back) and inside the window, Reopen takes the seal away and
// a second completion shows only the newest, the stamps in the data are byte-for-byte what they were, Completed rows and the sheet window show the same single seal.
//   SHOTS=<dir> node tests/charm-nest/library-seals.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets';
const T = (d, h, m) => Date.UTC(2026, 9, d, h, m);   // 6 Oct 23:55 UTC is 7:55 PM in Toronto
const AT = { gfReady1: T(5, 13, 58), gfCut1: T(5, 18, 14), gfReady2: T(6, 23, 52), gfCut2: T(6, 23, 55) };
const fails = [], ok = [];
const check = (c, m) => { if (c) ok.push(m); else fails.push(m); };

function seed(st, blobUrl) {
  st.docs.clear();
  const day = '2026-10-03', now = Date.now(); let n = 0; const lines = {};
  const mk = (id, metal, setId, setSeq, extra) => {
    n++;
    const orders = Array.from({ length: 3 + (n % 3) }, (_, i) => String(3700000000 + n * 100 + i)), p = `charmnest/sets/${day}/Set-${setSeq || 0}/${id}/preview.png`;
    const poolIds = orders.map(o => `${o}_1_1`);
    // every charm has an approved, saved back (so the card shows its "N / N" counter, bottom right, as in Paul's picture)
    const backPool = poolIds.map(poolId => ({ poolId, approvedAt: now - 3 * 86400000, approvedBy: 'Anna', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: `${poolId}.ai`, url: blobUrl(`${poolId}.ai`) } } }));
    const engraving = Object.fromEntries(poolIds.map(k => [k, { needed: true, state: 'approved', approved: true }]));
    const rec = { id, metal, metalLabel: metal, day, setId: setId || null, setSeq: setSeq || null, sheetIndex: 1, fileBase: `${metal.slice(0, 2).toUpperCase()}_${day}_Set-${setSeq || 0}_Sheet-1`, folder: `${metal}_${id}`, orders, poolIds, listings: orders.map((o, i) => String(1718000 + (i % 4))), placedCount: orders.length, charmCount: orders.length, density: 0.7, freePt2: 2000, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url: blobUrl(p) }, ai: { path: `${id}.ai`, url: blobUrl(`${id}.ai`) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: 'run-ls', backPool, engraving, label: { files: [{ path: `${id}-qr.png`, url: blobUrl(p), payload: orders[0], orders }] } };
    orders.forEach(o => { lines[`${o}_1`] = { orderId: o, state: 'written', quantity: 1, poolIds: [`${o}_1_1`], engraveCandidate: true }; });
    st.put(SHEETS, id, Object.assign(rec, extra));
  };
  // Set-1: GF completed twice (undo between), SS approved, RG partly cut
  mk('ls-gf', 'gold', 'set-ls-1', 1, { processReady: false, laserDoneAt: AT.gfCut2, laserDoneBy: 'Seth', processSeals: [
    { id: 'gf-r1', how: 'laserReady', at: AT.gfReady1, by: 'Anna' }, { id: 'gf-d1', how: 'laserDone', at: AT.gfCut1, by: 'Seth' },
    { id: 'gf-r2', how: 'laserReady', at: AT.gfReady2, by: 'Anna' }, { id: 'gf-d2', how: 'laserDone', at: AT.gfCut2, by: 'Seth' }] });
  mk('ls-ss', 'silver', 'set-ls-1', 1, { processReady: true, processSeals: [{ id: 'ss-r1', how: 'laserReady', at: T(6, 23, 53), by: 'Anna' }] });
  mk('ls-rg', 'rose', 'set-ls-1', 1, { processReady: true, roseCutAt: T(6, 23, 50), roseStockId: 'stock-ls', rosePlanHash: 'h', processSeals: [{ id: 'rg-r1', how: 'laserReady', at: T(6, 23, 52), by: 'Anna' }] });
  st.put(SETS, 'set-ls-1', { setId: 'set-ls-1', seq: 1, day, runId: 'run-ls', name: 'Set-1', sheetIds: ['ls-gf', 'ls-ss', 'ls-rg'], materials: ['gold', 'silver', 'rose'], orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now), createdAt: Timestamp.fromMillis(now - 86400000), processReady: true, processSeals: [{ id: 'set1-r', how: 'laserReady', at: T(6, 23, 53), by: 'Anna' }] });
  // a loose sheet, completed twice (Completed › Sheets)
  mk('ls-loose', 'gold', null, null, { processReady: false, laserDoneAt: T(6, 20, 5), laserDoneBy: 'Seth', processSeals: [
    { id: 'lo-r1', how: 'laserReady', at: T(5, 12, 0), by: 'Anna' }, { id: 'lo-d1', how: 'laserDone', at: T(5, 15, 0), by: 'Seth' },
    { id: 'lo-r2', how: 'laserReady', at: T(6, 19, 0), by: 'Anna' }, { id: 'lo-d2', how: 'laserDone', at: T(6, 20, 5), by: 'Seth' }] });
  // Set-2, completed whole (Completed › Sets)
  for (const [id, metal, k] of [['ls2-a', 'gold', 'a'], ['ls2-b', 'silver', 'b']])
    mk(id, metal, 'set-ls-2', 2, { processReady: false, laserDoneAt: T(6, 21, k === 'a' ? 10 : 12), laserDoneBy: 'Seth', processSeals: [{ id: `${id}-r`, how: 'laserReady', at: T(6, 20, 50), by: 'Anna' }, { id: `${id}-d`, how: 'laserDone', at: T(6, 21, k === 'a' ? 10 : 12), by: 'Seth' }] });
  st.put(SETS, 'set-ls-2', { setId: 'set-ls-2', seq: 2, day, runId: 'run-ls', name: 'Set-2', sheetIds: ['ls2-a', 'ls2-b'], materials: ['gold', 'silver'], orders: {}, labels: null, labelFiles: [], status: 'complete', updatedAt: Timestamp.fromMillis(now - 5000), createdAt: Timestamp.fromMillis(now - 86400000), laserDoneAt: T(6, 21, 12), laserDoneBy: 'Seth', processReady: false, processSeals: [{ id: 'set2-r', how: 'laserReady', at: T(6, 20, 50), by: 'Anna' }, { id: 'set2-d', how: 'laserDone', at: T(6, 21, 12), by: 'Seth' }] });
  st.put('Charm_Nest_Runs', 'run-ls', { runId: 'run-ls', lines });
}

const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 60)); } };
const clone = x => JSON.parse(JSON.stringify(x));

(async () => {
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const stamps = id => clone(st.doc(SHEETS, id).processSeals);
  const stampsOfSet = id => clone(st.doc(SETS, id).processSeals);

  async function open(width, height) {
    seed(st, srv.blobUrl);
    const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 1 });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
    await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push('page: ' + e.message));
    page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|CORS policy|net::ERR/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone && window.Seal, null, { timeout: 60000 });
    await page.waitForSelector('#libBody .setCard .libCard', { timeout: 30000 });
    await page.evaluate(() => document.getElementById('app').classList.add('railOff'));   // (the nesting rail is folded away as it is on every other tab; on a phone it would cover the Library)
    await until(() => page.evaluate(() => document.querySelectorAll('#libBody .libCard').length >= 3 && [...document.querySelectorAll('#libBody .libCard [data-sheet-status] .backSavedCount')].length >= 3), 20000, 'cards drawn with their counters');
    await page.waitForTimeout(700);
    return { ctx, page, errors };
  }
  /** what the Library cards show, measured in the page */
  const readCards = page => page.evaluate(() => {
    const rect = e => { const r = e.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height }; };
    const out = { cards: {}, header: [], page: { sw: document.documentElement.scrollWidth, iw: innerWidth } };
    for (const c of document.querySelectorAll('#libBody .libCard')) {
      const seals = [...c.querySelectorAll('.seal')].map(s => ({ cls: [...s.classList].filter(x => /^seal-/.test(x)).join(' '), at: +s.dataset.at, id: s.dataset.processSeal, size: s.offsetWidth, rect: rect(s), text: (() => { try { const j = JSON.parse(s.querySelector('svg[data-seal-model]').getAttribute('data-seal-model')); return j.action + ' ' + j.date + ' ' + j.time; } catch (_) { return ''; } })(), visible: !!s.offsetParent }));
      const others = [...c.querySelectorAll('.m > span:not(.sheetCutRow), .m .backSavedCount, .h button, .h .dndGrip, .ldMark, a, button')].filter(e => e.offsetParent && !e.closest('.sheetCutRow')).map(e => ({ what: (e.className || e.tagName) + ':' + (e.textContent || '').trim().slice(0, 14), rect: rect(e) }));
      out.cards[c.dataset.id] = { seals, others, card: rect(c), counter: (c.querySelector('.backSavedCount') || {}).textContent || '', foot: rect(c.querySelector('.m')) };
    }
    out.header = [...document.querySelectorAll('#libBody .setCard > .sh .seal, #libBody .setCard > .sh .sealRow')].map(e => e.className);
    out.allSeals = document.querySelectorAll('#libBody .seal').length;
    out.sealRows = [...document.querySelectorAll('#libBody .sealRow')].map(e => e.className);
    out.oldHangers = document.querySelectorAll('#libBody .processSealRow, #libBody .setProcessSeals, #libBody .ldProcessSeals').length;
    return out;
  });
  /** the toasts of the marks above and the station's "Laser or Design?" bar float over the page: not what is measured */
  const clearFloaters = page => page.evaluate(() => document.querySelectorAll('[data-kind="role"], .toast').forEach(e => e.remove()));
  const hit = (a, b) => a.l < b.r - 0.5 && a.r > b.l + 0.5 && a.t < b.b - 0.5 && a.b > b.t + 0.5;
  const shot = async (page, name, sel) => { if (!SHOTS) return; await page.evaluate(() => document.querySelectorAll('[data-kind="role"], .toast').forEach(e => e.remove())).catch(() => {});   /* (a toast or the station's "Laser or Design?" bar must not hide what the picture is for) */ const el = sel && await page.$(sel); if (el) await el.screenshot({ path: path.join(SHOTS, name + '.png') }); else await page.screenshot({ path: path.join(SHOTS, name + '.png') }); };

  for (const [width, height] of [[1280, 900], [700, 900], [390, 844]]) {
    const W = `${width}px`;
    const { ctx, page, errors } = await open(width, height);
    const before = { gf: stamps('ls-gf'), ss: stamps('ls-ss'), rg: stamps('ls-rg'), set: stampsOfSet('set-ls-1'), loose: stamps('ls-loose') };
    try {
      /* ══ 1 · Laser cutting: the set card of Set-1 ══ */
      const m = await readCards(page);
      const ids = Object.keys(m.cards).sort();
      check(ids.join() === 'ls-gf,ls-rg,ls-ss', `${W}: the three sheets of Set-1 are on screen (${ids})`);
      check(m.cards['ls-gf'].seals.length === 1, `${W}: the sheet completed after an undo (4 stamps in its record) shows exactly ONE seal: ${m.cards['ls-gf'].seals.length}`);
      const s = m.cards['ls-gf'].seals[0] || {};
      check(s.cls === 'seal-laserDone' && s.at === AT.gfCut2 && s.id === 'gf-d2', `${W}: it is the LATEST LASER CUT (the 6 Oct one), not an older cut and never a LASER READY: ${JSON.stringify([s.cls, s.at, s.id])}`);
      check(/LASER CUT/.test(s.text || '') && /06 OCT 2026/.test(s.text) && /7:55 PM/.test(s.text), `${W}: its face reads LASER CUT, 06 OCT 2026, 7:55 PM: ${s.text}`);
      check(m.cards['ls-ss'].seals.length === 0, `${W}: the sheet that is approved but not cut shows no seal (no LASER READY on a sheet): ${m.cards['ls-ss'].seals.length}`);
      check(m.cards['ls-rg'].seals.length === 0, `${W}: the Rose Gold sheet with only a partial Cut Sheet press shows no seal: ${m.cards['ls-rg'].seals.length}`);
      check(m.header.length === 0, `${W}: the set header carries no seal: ${JSON.stringify(m.header)}`);
      check(m.allSeals === 1 && m.sealRows.length === 1, `${W}: one seal in the whole Laser cutting list (${m.allSeals} seals, ${m.sealRows.length} rows)`);
      check(m.oldHangers === 0, `${W}: nothing of the old drawing (stamp rows hanging under the cards, set seals) is left: ${m.oldHangers}`);
      check(s.size >= 64 && s.size === 72, `${W}: the seal is 72 px across (at least 64): ${s.size}`);
      const g = m.cards['ls-gf'];
      check(/\d+ \/ \d+/.test(g.counter), `${W}: the sheet card shows its "N / N" counter (the fixture is like the picture): ${JSON.stringify(g.counter)}`);
      const sr = s.rect || {};
      const inside = sr.l >= g.card.l - 0.5 && sr.r <= g.card.r + 0.5 && sr.t >= g.card.t - 0.5 && sr.b <= g.card.b + 0.5;
      check(inside, `${W}: the seal lies wholly inside its card (no clipping at this width): seal ${Math.round(sr.l)}-${Math.round(sr.r)} x ${Math.round(sr.t)}-${Math.round(sr.b)}, card ${Math.round(g.card.l)}-${Math.round(g.card.r)} x ${Math.round(g.card.t)}-${Math.round(g.card.b)}`);
      const covered = g.others.filter(o => hit(sr, o.rect));
      check(covered.length === 0, `${W}: the seal covers neither the counts, the counter nor any button of the card: ${JSON.stringify(covered.map(o => o.what))}`);
      check(m.page.sw <= m.page.iw + 1, `${W}: the page does not scroll sideways (${m.page.sw} > ${m.page.iw})`);
      await shot(page, `w${width}-set-1-laser-cutting`, '#libBody .setCard');

      /* ══ 2 · the zoom: x1.8 of the resting size, in place, 500 ms, a click or Enter at once, Esc and leaving put it back ══ */
      const sealSel = '#libBody .libCard[data-id="ls-gf"] .seal';
      await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), sealSel);
      await page.waitForTimeout(300);
      const zoomState = () => page.evaluate(sel => { const e = document.querySelector(sel), r = e.getBoundingClientRect(); return { zoom: e.dataset.sealZoom || '', w: r.width, h: r.height, cx: r.left + r.width / 2, cy: r.top + r.height / 2, l: r.left, t: r.top, r: r.right, b: r.bottom, vw: innerWidth, vh: innerHeight }; }, sealSel);
      const rest = await zoomState(), restSize = await page.evaluate(sel => document.querySelector(sel).offsetWidth, sealSel);
      const c0 = { x: rest.cx, y: rest.cy };
      await page.mouse.move(2, 2); await page.mouse.move(c0.x, c0.y, { steps: 4 });
      await page.waitForTimeout(250);
      check((await zoomState()).zoom === '', `${W}: a pointer that has only just arrived does not zoom yet (500 ms rest)`);
      await page.waitForTimeout(450);   // 700 ms in
      await until(async () => (await zoomState()).zoom !== '', 3000, 'zoom after rest');
      await page.waitForTimeout(450);   // let the grow finish
      let z = await zoomState();
      check(Math.abs(parseFloat(z.zoom) - 1.8) < 0.011, `${W}: the zoom is x1.8 of its resting size: ${z.zoom}`);
      check(Math.abs(z.w - restSize * 1.8) < 1.5 && Math.abs(z.h - restSize * 1.8) < 1.5, `${W}: the grown seal is ${Math.round(restSize * 1.8)} px across (${Math.round(z.w)} x ${Math.round(z.h)})`);
      check(z.l >= 0 && z.t >= 0 && z.r <= z.vw && z.b <= z.vh, `${W}: the grown seal stays inside the window: ${Math.round(z.l)}-${Math.round(z.r)} x ${Math.round(z.t)}-${Math.round(z.b)} in ${z.vw} x ${z.vh}`);
      check(Math.hypot(z.cx - rest.cx, z.cy - rest.cy) < restSize * 0.45, `${W}: it grows where it stands (centre moved ${Math.round(Math.hypot(z.cx - rest.cx, z.cy - rest.cy))} px)`);
      check((await page.evaluate(() => document.querySelectorAll('.sealLens,.tlLoupe,[role=tooltip]').length)) === 0 && !(await page.evaluate(sel => document.querySelector(sel).hasAttribute('title'), sealSel)), `${W}: no second seal, no tooltip`);
      await shot(page, `w${width}-set-1-zoomed`);
      await page.mouse.move(2, 2); await page.waitForTimeout(600);
      check((await zoomState()).zoom === '', `${W}: leaving puts the seal back`);
      // a click zooms at once
      await page.mouse.move(2, 2); await page.mouse.click(c0.x, c0.y); await page.waitForTimeout(120);
      check((await zoomState()).zoom !== '', `${W}: a click zooms it at once, no waiting`);
      await page.keyboard.press('Escape'); await page.waitForTimeout(500);
      check((await zoomState()).zoom === '', `${W}: Esc puts it back`);
      // the keyboard: Enter on the focused seal
      await page.mouse.move(2, 2); await page.waitForTimeout(300);
      await page.evaluate(sel => document.querySelector(sel).focus(), sealSel); await page.waitForTimeout(150);
      if ((await zoomState()).zoom === '') await page.keyboard.press('Enter');
      await page.waitForTimeout(150);
      check((await zoomState()).zoom !== '', `${W}: Enter on the focused seal zooms it at once`);
      await page.keyboard.press('Escape'); await page.waitForTimeout(500);
      check((await zoomState()).zoom === '', `${W}: Esc puts it back again`);
      await page.evaluate(sel => document.querySelector(sel).blur(), sealSel);

      /* ══ 3 · permanence: nothing was written or lost ══ */
      check(JSON.stringify(stamps('ls-gf')) === JSON.stringify(before.gf) && before.gf.length === 4, `${W}: the sheet's record still holds all four stamps, unchanged`);
      check(JSON.stringify(stamps('ls-ss')) === JSON.stringify(before.ss) && JSON.stringify(stamps('ls-rg')) === JSON.stringify(before.rg) && JSON.stringify(stampsOfSet('set-ls-1')) === JSON.stringify(before.set), `${W}: the other sheets' and the set's stamps are untouched (the set keeps its LASER READY in the data)`);
      const pageStamps = await page.evaluate(() => ({ gf: (LibraryDone.recordOf('ls-gf').processSeals || []).map(x => x.id), roseCutAt: LibraryDone.recordOf('ls-rg').roseCutAt || 0 }));
      check(pageStamps.gf.join() === 'gf-r1,gf-d1,gf-r2,gf-d2' && pageStamps.roseCutAt > 0, `${W}: the page's own record of the sheet holds all four stamps too: ${pageStamps.gf}`);

      /* ══ 4 · Reopen takes the seal off the card (the stamps stay), the second completion shows only the newest ══ */
      const readCount = () => page.evaluate(() => ({ gf: document.querySelectorAll('#libBody .libCard[data-id="ls-gf"] .seal').length, all: document.querySelectorAll('#libBody .seal').length, ats: [...document.querySelectorAll('#libBody .libCard[data-id="ls-gf"] .seal')].map(s => +s.dataset.at) }));
      await page.evaluate(() => LibraryDone.mark('sheet', 'ls-gf', false, { by: 'Tester', undo: true }));
      await until(async () => (await readCount()).all === 0, 8000, 'seal gone after reopen');
      check(true, `${W}: Reopen (Undo) takes the seal off the card: none shown while the sheet is not completed`);
      check(stamps('ls-gf').length === 4 && JSON.stringify(stamps('ls-gf')) === JSON.stringify(before.gf), `${W}: ...and the four stamps are still in the data after the Reopen`);
      await page.evaluate(() => LibraryDone.mark('sheet', 'ls-gf', true, { by: 'Tester' }));
      await until(async () => (await readCount()).gf === 1, 10000, 'one seal after the new completion');
      await page.waitForTimeout(1800);   // the wooden press lands
      const again = await readCount(), now = stamps('ls-gf');
      check(again.gf === 1 && again.all === 1, `${W}: completed again: still exactly ONE seal on the card (${again.gf}), one in the list (${again.all})`);
      check(now.length > 4 && JSON.stringify(now.slice(0, 4)) === JSON.stringify(before.gf), `${W}: the earlier four stamps stay first and unchanged; the new completion was appended (${now.length} stamps)`);
      const newest = now.filter(x => x.how === 'laserDone').sort((a, b) => b.at - a.at)[0];
      check(again.ats[0] === newest.at && newest.at > AT.gfCut2, `${W}: the one seal is the newest completion (${again.ats[0]} = ${newest.at})`);
      await shot(page, `w${width}-set-1-completed-again`, '#libBody .setCard');

      /* ══ 5 · other Library views: the sheet window, and Completed ══ */
      await page.evaluate(() => SheetWin.open('ls-gf'));
      await page.waitForFunction(() => document.querySelector('.swState') && document.querySelector('.swStateSeal .seal'), null, { timeout: 20000 });
      await page.waitForTimeout(1400);
      const head = () => page.evaluate(() => { const seals = [...document.querySelectorAll('.swStateSeal .seal')].filter(e => e.offsetParent), r = e => { const b = e.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom }; }; const hd = document.querySelector('.swHead'); return { n: seals.length, size: seals[0] ? seals[0].offsetWidth : 0, text: seals[0] ? (() => { try { const j = JSON.parse(seals[0].querySelector('svg[data-seal-model]').getAttribute('data-seal-model')); return j.action; } catch (_) { return ''; } })() : '', rect: seals[0] ? r(seals[0]) : null, head: hd ? r(hd) : null, state: (document.querySelector('.swState') || {}).textContent.trim(), plus: document.querySelectorAll('.swStateSeal .sealPlus').length, vw: innerWidth, over: hd ? hd.scrollWidth - hd.clientWidth : 0, btn: document.querySelector('[data-r=done]') && document.querySelector('[data-r=done]').getBoundingClientRect().right }; });
      let h = await head();
      check(h.n === 1 && h.size === 72 && h.text === 'LASER CUT' && h.plus === 0, `${W}: the sheet window's header shows the same single seal, LASER CUT, 72 px, no "+N" beside it: ${JSON.stringify([h.n, h.size, h.text, h.plus])}`);
      check(h.rect && h.rect.l >= 0 && h.rect.r <= h.vw && h.over <= 1, `${W}: the header seal is on the screen and the header does not scroll sideways: ${JSON.stringify([h.rect && [h.rect.l, h.rect.r], h.vw, h.over])}`);
      await shot(page, `w${width}-sheet-window`);
      // its zoom is the same: x1.8, inside the window
      const hs = await page.evaluate(() => { const e = document.querySelector('.swStateSeal .seal'); const r = e.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; });
      await page.evaluate(() => document.querySelectorAll('[data-kind="role"]').forEach(e => e.remove()));   // (the station's "Laser or Design?" bar floats over the window on a phone: not what is measured here)
      await page.mouse.click(hs.x, hs.y); await page.waitForTimeout(500);
      const hz = await page.evaluate(() => { const e = document.querySelector('.swStateSeal .seal'), r = e.getBoundingClientRect(); return { zoom: e.dataset.sealZoom || '', w: r.width, l: r.left, t: r.top, r: r.right, b: r.bottom, vw: innerWidth, vh: innerHeight }; });
      check(Math.abs(parseFloat(hz.zoom) - 1.8) < 0.011 && hz.l >= 0 && hz.t >= 0 && hz.r <= hz.vw && hz.b <= hz.vh, `${W}: the header seal zooms x1.8 and stays inside the window: ${JSON.stringify(hz)}`);
      await page.keyboard.press('Escape'); await page.waitForTimeout(500);
      await page.evaluate(() => { const b = document.querySelector('[data-r=done]'); b.click(); });   // Move back to current
      await until(async () => (await head()).n === 0, 8000, 'header seal gone after Move back');
      h = await head();
      check(h.n === 0, `${W}: the sheet window after "Move back to current": no seal (the sheet is not completed); the stamps stay in the record: ${stamps('ls-gf').length} stamps`);
      await page.waitForFunction(() => !document.querySelector('[data-r=done]').disabled, null, { timeout: 8000 });
      await page.evaluate(() => document.querySelector('[data-r=done]').click());
      await until(async () => (await head()).n === 1, 8000, 'header seal back after Mark completed');
      check(true, `${W}: Mark completed again: the header shows its one seal again`);
      await page.keyboard.press('Escape'); await page.waitForTimeout(300);
      await page.evaluate(() => { try { SheetWin.close(true); } catch (_) {} });
      await page.waitForFunction(() => !document.querySelector('dialog.sheetWin[open]') && !(window.SheetWin && SheetWin.isOpen && SheetWin.isOpen()), null, { timeout: 15000 });   // (an open window makes the page behind it inert: nothing there can be pressed)
      await page.waitForTimeout(300);

      // Completed › Sheets
      await page.evaluate(() => { LibraryDone.setTab('done'); });
      await clearFloaters(page); await page.evaluate(() => document.querySelector('#libKind button[data-k="sheets"]').click());
      if (process.env.DEBUG) console.log('after click', await page.evaluate(() => ({ kind: CN.S.library.kind, on: [...document.querySelectorAll('#libKind button')].map(b => b.dataset.k + ':' + b.className), tab: LibraryDone.tab() })));
      await until(() => page.evaluate(() => document.querySelectorAll('#libDone .ldItem[data-kind="sheet"]').length >= 1), 20000, 'completed sheets listed').catch(async e => { if (process.env.DEBUG) console.log(await page.evaluate(() => ({ tab: LibraryDone.tab(), kind: CN.S.library.kind, html: document.getElementById('libDone').innerHTML.slice(0, 700), hidden: document.getElementById('libDone').hidden, cls: document.getElementById('libDone').className }))); throw e; });
      await page.waitForTimeout(900);
      const done1 = await page.evaluate(() => [...document.querySelectorAll('#libDone .ldItem[data-kind="sheet"]')].map(it => ({ id: it.dataset.id, seals: [...it.querySelectorAll('.seal')].map(s => ({ cls: [...s.classList].filter(x => /^seal-/.test(x)).join(' '), at: +s.dataset.at, size: s.offsetWidth })), rect: (() => { const r = it.getBoundingClientRect(); return { l: r.left, r: r.right, t: r.top, b: r.bottom }; })(), sealRect: (() => { const s = it.querySelector('.seal'); if (!s) return null; const r = s.getBoundingClientRect(); return { l: r.left, r: r.right, t: r.top, b: r.bottom }; })(), over: document.documentElement.scrollWidth - innerWidth })));
      const loose = done1.find(x => x.id === 'ls-loose');
      check(!!loose && loose.seals.length === 1 && loose.seals[0].cls === 'seal-laserDone' && loose.seals[0].at === T(6, 20, 5) && loose.seals[0].size === 72, `${W}: Completed › Sheets: the sheet completed twice shows one seal, its latest LASER CUT, 72 px: ${JSON.stringify(loose && loose.seals)}`);
      check(done1.every(x => x.seals.length <= 1) && done1.every(x => x.over <= 1), `${W}: every Completed sheet row has at most one seal and the page does not scroll sideways`);
      check(loose && loose.sealRect && loose.sealRect.l >= loose.rect.l - 1 && loose.sealRect.r <= loose.rect.r + 1, `${W}: the Completed row's seal is inside its row`);
      await shot(page, `w${width}-completed-sheets`, '#libDone');

      // Completed › Sets: the set's row has no seal; opened, each of its sheets shows its one
      await clearFloaters(page); await page.evaluate(() => document.querySelector('#libKind button[data-k="sets"]').click());
      await until(() => page.evaluate(() => document.querySelectorAll('#libDone .ldItem[data-kind="set"]').length >= 1), 20000, 'completed sets listed');
      await page.waitForTimeout(700);
      const setRow = await page.evaluate(() => { const it = document.querySelector('#libDone .ldItem[data-kind="set"]'); return { seals: it.querySelectorAll('.seal').length, rows: it.querySelectorAll('.sealRow').length }; });
      check(setRow.seals === 0 && setRow.rows === 0, `${W}: Completed › Sets: the set's own row carries no seal: ${JSON.stringify(setRow)}`);
      await page.evaluate(() => document.querySelector('#libDone .ldItem[data-kind="set"] .ldLine').click());
      await until(() => page.evaluate(() => document.querySelectorAll('#libDone .ldItem[data-kind="set"] .libCard').length >= 2), 20000, 'the opened set shows its sheets');
      await page.waitForTimeout(900);
      const opened = await page.evaluate(() => [...document.querySelectorAll('#libDone .ldItem[data-kind="set"] .libCard')].map(c => ({ id: c.dataset.id, seals: [...c.querySelectorAll('.seal')].map(s => ({ cls: [...s.classList].filter(x => /^seal-/.test(x)).join(' '), size: s.offsetWidth })), head: c.closest('.setCard') ? c.closest('.setCard').querySelectorAll(':scope > .sh .seal').length : 0 })));
      check(opened.length === 2 && opened.every(c => c.seals.length === 1 && c.seals[0].cls === 'seal-laserDone' && c.seals[0].size === 72 && c.head === 0), `${W}: the opened completed set: each sheet shows its one LASER CUT seal (72 px), the set header none: ${JSON.stringify(opened)}`);
      await shot(page, `w${width}-completed-sets-open`, '#libDone');
      check(errors.length === 0, `${W}: no page errors: ${errors.join(' | ')}`);
    } finally { await ctx.close(); }
  }
  await browser.close(); srv.close();
  console.log(ok.map(x => '  ok  ' + x).join('\n'));
  if (fails.length) { console.error('\n' + fails.length + ' FAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log(`\nlibrary-seals: ${ok.length} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
