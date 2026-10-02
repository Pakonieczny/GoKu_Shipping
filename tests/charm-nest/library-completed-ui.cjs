// Browser test of the Library's Current | Completed tabs (charm-nest-library.js) over the local stand-in for the site
// (bridge-server.cjs: the real charmNestLibrary function over an in-memory Firestore): marking a sheet and a set
// completed and undoing it, the Completed list (by day, paged as it scrolls, a set opened in place), Move back, the
// order / listing search in both tabs, the tab in the address and one line of bar at two widths.
//   node tests/charm-nest/library-completed-ui.cjs [playwright-core dir]      (SHOTS=<dir> also saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
// (a real Timestamp: the Completed list pages by `updatedAt <= cursor`, which a stand-in object with only toMillis does not compare with)
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';

function crc32(buf) { let c, crc = 0xffffffff; for (let n = 0; n < buf.length; n++) { c = (crc ^ buf[n]) & 0xff; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; crc = (crc >>> 8) ^ c; } return (crc ^ 0xffffffff) >>> 0; }
function png(w, h, fn) {
  const raw = Buffer.alloc((w * 3 + 1) * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const [r, g, b] = fn(x, y), o = y * (w * 3 + 1) + 1 + x * 3; raw[o] = r; raw[o + 1] = g; raw[o + 2] = b; }
  const chunk = (t, d) => { const len = Buffer.alloc(4); len.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]), crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, crc]); };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}
const COLORS = { gold: [201, 166, 92], silver: [160, 166, 175], rose: [205, 140, 130] };
const preview = (metal, seed) => { const [r, g, b] = COLORS[metal]; return png(120, 100, (x, y) => ((x + seed * 13) % 24 - 10) ** 2 + ((y + seed * 7) % 22 - 9) ** 2 < 40 ? [r, g, b] : [252, 250, 246]); };
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets';

/* Current: 6 days × 2 sets of 1-3 sheets. Completed already: 40 sets of 3 sheets, one a day, marked by Anna. */
function seed(st, blobUrl) {
  const now = Date.now(), day = i => new Date(now - i * 86400000).toISOString().slice(0, 10), metals = ['gold', 'silver', 'rose'];
  let n = 0;
  const sheet = (id, metal, d, setId, s, k, extra) => {
    const p = `charmnest/sets/${day(d)}/Set-${s}/${id}/preview.png`; n++;
    st.blobs.set(p, { buf: preview(metal, n), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
    const orders = Array.from({ length: 3 + (n % 4) }, (_, i) => String(3700000000 + n * 10 + i));
    st.put(SHEETS, id, Object.assign({ id, metal, metalLabel: metal, day: day(d), setId, setSeq: s, sheetIndex: k + 1, fileBase: `${metal.slice(0, 2).toUpperCase()}_${day(d)}_Set-${s}_Sheet-${k + 1}`, folder: `${metal}_${id}`, orders, poolIds: orders.map(o => `${o}_1_1`), listings: orders.map((o, i) => String(1718000 + (i % 4))), placedCount: 10 + n % 7, charmCount: 10 + n % 7, density: 0.6 + (n % 30) / 100, freePt2: 2000, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url: blobUrl(p) } }, verification: { ok: true }, names: orders.map(o => `${o} · BR-TST-0${n % 6}`).join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - d * 86400000 - s * 1000 - k), createdAt: Timestamp.fromMillis(now - d * 86400000), archived: false, runId: `run-${day(d)}` }, extra));
  };
  for (let d = 0; d < 6; d++) for (let s = 1; s <= 2; s++) {
    const setId = `set-${day(d)}-${s}`, ids = [];
    metals.slice(0, 1 + ((d + s) % 3)).forEach((metal, k) => { const id = `sh${d}x${s}x${k}`; ids.push(id); sheet(id, metal, d, setId, s, k); });
    st.put(SETS, setId, { setId, seq: s, day: day(d), runId: `run-${day(d)}`, name: `Set-${s}`, sheetIds: ids, materials: metals.slice(0, ids.length), orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now - d * 86400000 - s * 1000), createdAt: Timestamp.fromMillis(now - d * 86400000) });
  }
  // Since 30 Sep a sheet or set is marked completed only once it is ready for the laser: its cutting file, its QR label, its engraving
  // decisions (none to make here), a charm for each of its pieces and every one of its orders written (the server checks the run's lines).
  // The current sheets are made so.
  const lines = {};
  for (const r of st.list(SHEETS)) {
    r.orders.forEach(o => { (lines[r.runId] = lines[r.runId] || {})[`${o}_1`] = { orderId: o, state: 'written', quantity: 1, poolIds: [`${o}_1_1`], engraveCandidate: false }; });
    st.put(SHEETS, r.id, { processReady: true, processSeals: [{ id: `laserReady-${now}-0`, how: 'laserReady', at: now - 3600000, by: 'Anna' }], placedCount: r.orders.length, charmCount: r.orders.length, outputs: Object.assign({}, r.outputs, { ai: { path: `${r.id}.ai`, url: blobUrl(`${r.id}.ai`) } }), label: { files: [{ path: `${r.id}-qr.png`, url: r.outputs.preview.url, payload: r.orders[0], orders: r.orders }] } });
  }
  for (const [runId, ls] of Object.entries(lines)) st.put('Charm_Nest_Runs', runId, { runId, lines: ls });
  // (their Laser Ready seals are already on record, as the server leaves them: the page presses only a seal that is new)
  for (const x of st.list(SETS)) st.put(SETS, x.setId, { processReady: true, processSeals: [{ id: `laserReady-${now}-1`, how: 'laserReady', at: now - 3600000, by: 'Anna' }] });
  for (let d = 0; d < 40; d++) {
    const setId = `old-${day(d + 7)}`, ids = [], at = now - (d + 1) * 86400000 + 3600000;
    metals.forEach((metal, k) => { const id = `old${d}x${k}`; ids.push(id); sheet(id, metal, d + 7, setId, 3, k, { laserDoneAt: at - k * 60000, laserDoneBy: 'Anna', listings: k === 1 && d === 30 ? ['1719999'] : undefined }); });
    st.put(SETS, setId, { setId, seq: 3, day: day(d + 7), runId: `run-${day(d + 7)}`, name: 'Set-3', sheetIds: ids, materials: metals, orders: {}, labelFiles: [], status: 'complete', laserDoneAt: at, laserDoneBy: 'Anna', updatedAt: Timestamp.fromMillis(at), createdAt: Timestamp.fromMillis(at) });
  }
}

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  seed(st, srv.blobUrl);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  // (the sheets' orders are read by the server from the run records' lines; this test is not about a run, and the page would
  //  recall the newest of them into the Nest view on load: it is told there is none, whatever else it asks goes through)
  await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) {} return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
  // what Current showed before anything was clicked: the Sheets list, or its loading line, would be a flash before Sets
  await ctx.addInitScript(() => { window.__sheetsSeen = 0; document.addEventListener('DOMContentLoaded', () => { const b = document.getElementById('libBody'); if (!b) return; const look = () => { if (b.querySelector('[data-laser-card="sheet"]') || /Loading sheets|Cloud offline/.test(b.textContent)) window.__sheetsSeen++; }; look(); new MutationObserver(look).observe(b, { childList: true, subtree: true, characterData: true }); }); });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const shot = async name => { if (SHOTS) { await page.waitForTimeout(350); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };
  const done = id => +(st.doc(SHEETS, id) || {}).laserDoneAt > 0;
  const bar = () => page.evaluate(() => { const b = document.querySelector('#libView .libBar'), i = document.querySelector('#libSearch'), t = document.querySelector('#libTab'); return { h: b.getBoundingClientRect().height, over: b.scrollWidth - b.clientWidth, input: i.getBoundingClientRect().height, tabs: t.getBoundingClientRect().height, tabsTop: t.getBoundingClientRect().top, inputTop: i.getBoundingClientRect().top }; });
  // (a wooden-stamp press of a seal that has just been put on swallows clicks while it lasts, by design: wait for it before the next click)
  // (the wooden-stamp press of a seal just put on swallows clicks while it lasts, by design, and one can follow another as the page redraws:
  //  a tap waits for the moment none is pressing and clicks within that same turn, so no press can begin between the look and the click)
  const tap = sel => page.waitForFunction(q => { if (window.Seal && Seal.busy()) return false; const el = document.querySelector(q); if (!el) return false; el.click(); return true; }, sel, { timeout: 40000, polling: 50 });
  const ok = [], until = async (fn, ms = 25000) => { const t0 = Date.now(); while (!fn()) { if (Date.now() - t0 > ms) throw new Error('timed out: ' + fn); await new Promise(r => setTimeout(r, 50)); } };

  await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone, null, { timeout: 60000 });
  // the Library opens on Sets (Paul, 29 Sep), drawn first: nothing of the Sheets list before it; the count is of sets
  await page.waitForSelector('#libBody .setCard', { timeout: 30000 });
  assert.equal(await page.textContent('#libKind button.on'), 'Sets', 'the switch says Sets');
  await page.waitForFunction(() => document.querySelector('#libDoneCount').textContent === '40', null, { timeout: 10000 });
  await page.waitForTimeout(300);
  assert.equal(await page.evaluate(() => [CN.S.library.kind, window.__sheetsSeen]).then(x => x.join()), 'sets,0', 'no Sheets list or its loading line before the sets');
  ok.push('a fresh load opens the Library on Sets: the sets drawn first, the switch on Sets, the Completed count in sets');
  // Sheets, once chosen, stays while a person goes to another tab and back (tabs keep their state)
  await page.click('#libKind button[data-k=sheets]');
  await page.waitForSelector('#libBody [data-laser-card="sheet"] .libCard', { timeout: 15000 });
  await page.evaluate(() => { CN.setMode('nest'); CN.setMode('library'); });
  await page.waitForTimeout(200);
  assert.deepEqual(await page.evaluate(() => [CN.S.library.kind, document.querySelector('#libKind button.on').textContent, !!document.querySelector('#libBody [data-laser-card="sheet"]'), !!document.querySelector('#libBody .setCard')]), ['sheets', 'Sheets', true, false], 'Sheets is kept across tabs');
  ok.push('Sheets chosen stays Sheets across a tab change');
  await page.waitForSelector('#libBody .libCard', { timeout: 30000 });
  await page.waitForFunction(() => document.querySelector('#libDoneCount').textContent === '120', null, { timeout: 10000 });
  // one line of bar: the tabs no taller than the search, on its line, and nothing to scroll
  let b = await bar();
  assert(b.h < 44 && b.over <= 0 && b.tabs <= b.input + 1 && Math.abs(b.tabsTop + b.tabs / 2 - (b.inputTop + b.input / 2)) < 2, 'one line of bar at 1500: ' + JSON.stringify(b));
  const current0 = await page.$$eval('#libBody .libCard[data-id]', cs => cs.map(c => c.dataset.id));
  assert(current0.length === 24 && !current0.some(id => id.startsWith('old')), 'Current holds what the laser has not done: ' + current0.length);
  // (the Library card's seal cap of 72 px, in a real browser: the jsdom tests of it read the stylesheet through custom properties, as jsdom cannot parse min() around var())
  const caps = await page.$$eval('#libBody .libCard .seal', ss => ss.map(x => Math.round(x.offsetWidth)));
  assert(caps.length > 0 && caps.every(w => w > 0 && w <= 72) && Math.max(...caps) === 72, 'Library card seals are capped at 72 px: ' + caps.slice(0, 12));
  ok.push('Current lists only what is not completed; the tab counts 120 completed sheets; one line of bar at 1500; the Library card seals stand at the 72 px cap');
  await shot('01-current-1500');

  // mark a sheet completed from its corner: it leaves, the count grows, the cloud records who and when; Undo brings it back
  // (since 30 Sep a sheet of a set stays in place when marked, with its seal, until its whole set is done and goes to Completed together:
  //  a set of one sheet is the plain case of a sheet that leaves when marked)
  const first = current0.find(id => (st.doc(SETS, st.doc(SHEETS, id).setId).sheetIds || []).length === 1);
  assert(first, 'a set of one sheet among the current ones');
  await page.hover(`#libBody .libCard[data-id="${first}"]`);
  await page.waitForTimeout(250);
  const vis = await page.$eval(`#libBody .libCard[data-id="${first}"] .ldMark`, m => getComputedStyle(m).opacity);
  assert(+vis > 0.9, 'the check shows on hover: ' + vis);
  await shot('02-hover-mark');
  await page.click(`#libBody .libCard[data-id="${first}"] .ldMark`);
  // (Paul, 27 Sep: it leaves at once, a copy of it flies to the Completed tab, which counts it as it lands)
  await page.waitForFunction(id => !document.querySelector(`#libBody .libCard[data-id="${id}"]`), first, { timeout: 5000 });
  const flying = await page.evaluate(() => [document.querySelectorAll('#motionLayer .mGhost').length, document.querySelector('#libDoneCount').textContent]);
  assert.deepEqual(flying, [1, '120'], 'a copy flies to the tab, which counts it as it lands: ' + flying);
  await page.waitForFunction(() => document.querySelector('#libDoneCount').textContent === '121', null, { timeout: 5000 });
  assert(done(first) && st.doc(SHEETS, first).laserDoneBy === 'Tester', 'the mark is recorded with its name');
  assert.equal(await page.evaluate(id => CN.allSheets().some(p => p.sheetId === id) || LibraryDone.isDone({ id }), first), true);
  // one Undo: in the note under the Completed tab, once the copy has landed (no toast beside it)
  await page.waitForSelector(`.mNote[data-ld="sheet:${first}"] .mNoteBtn`);
  assert.match(await page.textContent(`.mNote[data-ld="sheet:${first}"]`), /Sheet \d+ marked completed/);
  // (the note comes as the copy lands; the copy's own fade-out takes a few frames more)
  await page.waitForFunction(() => !document.querySelector('#motionLayer .mGhost'), null, { timeout: 3000 }).catch(() => {});
  assert.equal(await page.$$eval('#motionLayer .mGhost', x => x.length), 0, 'nothing left on the motion layer');
  assert.equal(await page.$('.toast .toastUndo'), null, 'no second Undo in a toast');
  await shot('03-marked-undo-note');
  await page.click(`.mNote[data-ld="sheet:${first}"] .mNoteBtn`);
  await page.waitForFunction(id => !!document.querySelector(`#libBody .libCard[data-id="${id}"]`), first, { timeout: 5000 });
  await page.waitForFunction(() => document.querySelector('#libDoneCount').textContent === '120', null, { timeout: 5000 });
  assert(!done(first) && !('laserDoneBy' in st.doc(SHEETS, first)), 'Undo takes the mark back in the cloud');
  await page.waitForFunction(() => !document.querySelector('#motionLayer .mGhost'), null, { timeout: 5000 });
  ok.push('a sheet marked from its corner flies to Completed (counted as it lands), is recorded with who and when; Undo in the note under the tab brings it back');

  // the page's own run: a run page of a completed sheet is closed to more charms
  // (a run page with unsaved changes holds its sheet back from the laser, so the page here is the saved run of that sheet)
  const closed = await page.evaluate(() => { const p = CN.activePage('gold'); p.sheetId = 'sh0x1x0'; window.__wasDirty = p.dirty; p.dirty = false; return LibraryDone.mark('sheet', 'sh0x1x0', true).then(() => [!!p.laserDoneAt, !!window.LiveNest?.closed(p)]); });
  assert.deepEqual(closed, [true, true], 'a run page of a completed sheet is closed: ' + closed);
  await page.evaluate(() => LibraryDone.mark('sheet', 'sh0x1x0', false).then(() => { const p = CN.activePage('gold'); p.sheetId = null; p.dirty = window.__wasDirty; }));
  ok.push('window.LibraryDone.mark sets laserDoneAt on the run page (LiveNest.closed) and takes it back');

  // Sets: a set marked completed marks every sheet of it
  await tap('#libKind button[data-k=sets]');
  await page.waitForSelector('#libBody .setCard .ldMarkSet', { timeout: 15000 });
  await page.waitForFunction(() => document.querySelector('#libDoneCount').textContent === '40');
  await shot('04-current-sets-1500');
  const setId = await page.$eval('#libBody .setCard .ldMarkSet', m => m.dataset.ld.slice(4));
  const members = st.doc(SETS, setId).sheetIds;
  await tap(`#libBody .ldMarkSet[data-ld="set:${setId}"]`);
  await page.waitForFunction(id => ![...document.querySelectorAll('#libBody .setCard')].some(c => c._laserSet && c._laserSet.setId === id), setId, { timeout: 25000 });
  await page.waitForFunction(() => document.querySelector('#libDoneCount').textContent === '41', null, { timeout: 25000 });
  assert(members.every(done) && +st.doc(SETS, setId).laserDoneAt > 0, 'the set and each of its sheets are completed');
  // one sheet of another set: that set shows how far the laser is
  const other = await page.evaluate(() => { const c = [...document.querySelectorAll('#libBody .setCard')].find(x => x._laserSet && x._laserSet.setId && x.querySelectorAll('.libCard').length > 1); return c && c.querySelector('.libCard').dataset.id; });
  await page.hover(`#libBody .libCard[data-id="${other}"]`);
  await tap(`#libBody .libCard[data-id="${other}"] .ldMark`);
  await page.waitForSelector('#libBody .ldPartial', { timeout: 25000 });
  assert.match(await page.textContent('#libBody .ldPartial'), /1 of \d sheets completed/);
  ok.push('a set marked completed marks each of its sheets; a set part done says "1 of N sheets completed"');
  await shot('05-sets-partial');
  // the rest of that set marked, then undone: only what that mark changed is taken back
  const partSet = await page.evaluate(id => [...document.querySelectorAll('#libBody .setCard')].find(c => (c._laserSheets || []).includes(id))._laserSet.setId, other);
  const partMembers = st.doc(SETS, partSet).sheetIds;
  await tap(`#libBody .ldMarkSet[data-ld="set:${partSet}"]`);
  await until(() => partMembers.every(done) && +st.doc(SETS, partSet).laserDoneAt > 0);
  await tap(`.mNote[data-ld="set:${partSet}"] .mNoteBtn`);
  await until(() => !st.doc(SETS, partSet).laserDoneAt && partMembers.filter(done).length === 1);
  assert(done(other), 'the sheet marked before keeps its mark');
  await page.waitForFunction(id => [...document.querySelectorAll('#libBody .setCard')].some(c => c._laserSet && c._laserSet.setId === id && c.querySelector('.ldPartial')), partSet, { timeout: 25000 });
  ok.push('Undo of a set marked after one of its sheets takes back only the sheets that mark changed');

  // Completed: the tab in the address and the browser's memory, the set at the top, opened in place
  await tap('#libTab button[data-t=done]');
  await page.waitForSelector('#libDone .ldItem[data-kind=set]', { timeout: 10000 });
  assert.equal(await page.evaluate(() => location.hash), '#library/completed');
  assert.equal(await page.evaluate(() => localStorage.getItem('cn.libTab')), 'done');
  assert(await page.$eval('#libBody', e => e.hidden) && !(await page.$eval('#libDone', e => e.hidden)), 'one panel at a time');
  const top = await page.$eval('#libDone .ldItem', e => e.dataset.set);
  assert.equal(top, setId, 'newest completed first');
  assert.match(await page.textContent('#libDone .ldDayHead'), /^Today/);
  assert.match(await page.getAttribute('#libSearch', 'placeholder'), /^Search completed/);
  b = await bar(); assert(b.h < 44 && b.over <= 0, 'the bar keeps its line in Completed: ' + JSON.stringify(b));
  await page.waitForTimeout(400);
  await shot('06-completed-sets-1500');
  await tap(`#libDone .ldItem[data-set="${setId}"] .ldLine`);
  await page.waitForSelector(`#libDone .ldItem[data-set="${setId}"].open .ldPanelIn .setCard .libCard[data-id]`, { timeout: 10000 });
  const inCard = await page.$$eval(`#libDone .ldItem[data-set="${setId}"] .ldPanelIn .libCard[data-id]`, cs => cs.map(c => c.dataset.id));
  assert.deepEqual(inCard.sort(), members.slice().sort(), 'the opened set shows every sheet as Current does');
  assert(await page.$(`#libDone .ldItem[data-set="${setId}"] .ldPanelIn .ldMarkSet[data-done="1"]`), 'the opened set can be moved back');
  assert(await page.$(`#libDone .ldItem[data-set="${setId}"] .ldPanelIn [data-export-set]`), 'with its downloads, as in Current');
  await page.waitForTimeout(400);
  await shot('07-completed-set-open');
  // a sheet in it opens as a Library sheet does
  await tap(`#libDone .ldItem[data-set="${setId}"] .ldPanelIn .libCard[data-id="${inCard[0]}"] .pv`);
  // (the sheet window, charm-nest-sheetwin.js; the older dialog where that script is missing)
  await page.waitForFunction(() => (window.SheetWin && SheetWin.isOpen()) || document.querySelector('#dlgSheet').open, null, { timeout: 5000 });
  await page.evaluate(async () => { if (window.SheetWin && SheetWin.isOpen()) { await SheetWin.close(); await new Promise(r => setTimeout(r, 250)); } else document.querySelector('#lsClose').click(); });
  ok.push('Completed: in the address (#library/completed) and remembered; newest first under "Today"; a set opens in place into its full card');

  // Sheets in Completed: paged as the list scrolls, by day
  await tap('#libKind button[data-k=sheets]');
  await page.waitForSelector('#libDone .ldItem[data-kind=sheet]', { timeout: 10000 });
  // (the sheet marked in the other set is still in Current with its set, which is not done: Completed counts and lists only what is filed)
  const total = 120 + members.length;
  await page.waitForFunction(n => document.querySelector('#libDoneCount').textContent === String(n), total);
  const n0 = await page.$$eval('#libDone .ldItem[data-kind=sheet]', x => x.length);
  assert.equal(n0, 60, 'the first page: ' + n0);
  const days = await page.$$eval('#libDone .ldDayHead', hs => hs.map(h => h.textContent));
  assert(days.length >= 10 && /^Today/.test(days[0]), 'by day: ' + days.slice(0, 3));
  await shot('08-completed-sheets-1500');
  const sticky = await page.evaluate(async () => { const s = document.querySelector('#stage'); s.scrollTop = 900; await new Promise(r => setTimeout(r, 120)); const t = s.getBoundingClientRect().top; return [...document.querySelectorAll('#libDone .ldDayHead')].some(x => { const r = x.getBoundingClientRect(); return r.top <= t + 1 && r.bottom > t + 12 && x.parentElement.getBoundingClientRect().top < t - 20; }); });
  assert(sticky, 'the day heading stays at the top as its day scrolls');
  await page.evaluate(() => { const s = document.querySelector('#stage'); s.scrollTop = s.scrollHeight; });
  await page.waitForFunction(() => document.querySelectorAll('#libDone .ldItem[data-kind=sheet]').length >= 120, null, { timeout: 10000 });
  await page.evaluate(() => { const s = document.querySelector('#stage'); s.scrollTop = s.scrollHeight; });
  await page.waitForFunction(() => /that is all/.test(document.querySelector('#libDone .ldMore').textContent), null, { timeout: 10000 });
  const n1 = await page.$$eval('#libDone .ldItem[data-kind=sheet]', x => x.length);
  assert.equal(n1, total, 'every completed sheet, a page at a time: ' + n1);
  const imgs = await page.$$eval('#libDone .ldThumb img.on', x => x.length);
  assert(imgs > 0, 'thumbnails show');
  ok.push('Completed sheets: a page of 60, the next as the list scrolls, to the end; sticky day headings; lazy thumbnails');
  await page.evaluate(() => { document.querySelector('#stage').scrollTop = 0; });

  // Move back from Completed: the row leaves, the sheet is back in Current
  const back = members[0];
  await page.hover(`#libDone .ldItem[data-id="${back}"] .ldLine`);
  await shot('09-completed-row-hover');
  await tap(`#libDone .ldItem[data-id="${back}"] .ldBack`);
  await page.waitForFunction(id => !document.querySelector(`#libDone .ldItem[data-id="${id}"]`), back, { timeout: 5000 });
  assert(!done(back), 'moved back in the cloud');
  assert(!st.doc(SETS, setId).laserDoneAt, 'its set is not complete any more');
  ok.push('Move back: the row leaves Completed, the sheet (and its set) are current again');

  // a listing searched in Completed: its sheets, however old
  // (a number shorter than 9 digits waits for Enter: every prefix of one being typed used to be looked up)
  await page.fill('#libSearch', '1719999');
  await page.waitForFunction(() => /Enter looks up order or listing 1719999/.test((document.querySelector('#libDone .ldFound') || {}).textContent || ''), null, { timeout: 10000 });
  await page.press('#libSearch', 'Enter');
  await page.waitForSelector('#libDone .ldFound', { timeout: 10000 });
  await page.waitForFunction(() => /1 completed sheet holds listing/.test(document.querySelector('#libDone .ldFound').textContent), null, { timeout: 10000 });
  assert.deepEqual(await page.$$eval('#libDone .ldItem', x => x.map(e => e.dataset.id)), ['old30x1']);
  await shot('10-completed-search-listing');
  // the same number in Current: nothing current holds it, and the line says where it is
  await tap('#libTab button[data-t=current]');
  await page.waitForFunction(() => /No current sheet holds order or listing 1719999/.test((document.querySelector('#libBody .ldFound') || {}).textContent || ''), null, { timeout: 10000 });
  assert.match(await page.textContent('#libBody .ldFound'), /1 in Completed/);
  // a listing bought on several current sheets: every sheet with it, lit, and the sets they are in
  await page.fill('#libSearch', '1718002');
  await page.press('#libSearch', 'Enter');
  await page.waitForFunction(() => /sheets? holds? listing 1718002 · in \d+ sets?/.test((document.querySelector('#libBody .ldFound') || {}).textContent || ''), null, { timeout: 10000 });
  const hits = await page.$$eval('#libBody .libCard[data-id]', cs => cs.map(c => c.dataset.id));
  // (Current holds a sheet marked in a set that is not done yet, with its seal: it is filed in Completed only with its set)
  const filed = d => +d.laserDoneAt > 0 && (!d.setId || !st.doc(SETS, d.setId) || +st.doc(SETS, d.setId).laserDoneAt > 0);
  const want = st.list(SHEETS).filter(d => !filed(d) && (d.listings || []).includes('1718002')).map(d => d._id);
  assert.deepEqual(hits.sort(), want.sort(), 'every current sheet with the listing, whatever its metal');
  assert(await page.$('#libBody .libCard.ldHit'), 'the sheets found are lit');
  await shot('11-current-search-listing');
  // an order: the one sheet that holds it
  const order = st.doc(SHEETS, 'sh2x1x0').orders[1];
  await page.fill('#libSearch', order);
  await page.waitForFunction(o => new RegExp('1 sheet holds order ' + o).test((document.querySelector('#libBody .ldFound') || {}).textContent || ''), order, { timeout: 10000 });
  assert.deepEqual(await page.$$eval('#libBody .libCard[data-id]', cs => cs.map(c => c.dataset.id)), ['sh2x1x0']);
  // × clears the search and the whole list comes back
  await tap('#libBody .ldFound .ldClear');
  await page.waitForFunction(() => !document.querySelector('#libBody .ldFound') && document.querySelectorAll('#libBody .libCard[data-id]').length > 20, null, { timeout: 10000 });
  assert.equal(await page.inputValue('#libSearch'), '');
  ok.push('search: a listing or an order in either tab finds every sheet with it (lit), says where the rest are, and × resets it');

  // 1280 × 800: the bar keeps one line, the tabs readable, in both tabs
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.waitForTimeout(300);
  b = await bar(); assert(b.h < 44 && b.over <= 0, 'one line of bar at 1280: ' + JSON.stringify(b));
  await shot('12-current-sheets-1280');
  await tap('#libTab button[data-t=done]');
  await page.waitForSelector('#libDone .ldItem', { timeout: 10000 });
  b = await bar(); assert(b.h < 44 && b.over <= 0, 'one line of bar at 1280 in Completed: ' + JSON.stringify(b));
  await shot('13-completed-sheets-1280');
  await tap('#libKind button[data-k=sets]');
  await page.waitForSelector('#libDone .ldItem[data-kind=set]', { timeout: 10000 });
  await tap('#libDone .ldItem[data-kind=set] .ldLine');
  await page.waitForSelector('#libDone .ldItem.open .ldPanelIn .setCard', { timeout: 10000 });
  await shot('14-completed-set-open-1280');
  ok.push('1280 × 800: one line of bar in both tabs');

  // a reload opens the tab it was on; the page opened afresh on #library opens the tab last used; Back / Forward follow
  // (Sheets | Sets is not carried over: Sheets chosen before the reload, the page opens on Sets again)
  await tap('#libKind button[data-k=sheets]');
  await page.waitForSelector('#libDone .ldItem[data-kind=sheet]', { timeout: 10000 });
  await page.reload();
  await page.waitForFunction(() => window.LibraryDone && LibraryDone.tab() === 'done' && document.querySelector('#libDone .ldItem'), null, { timeout: 30000 });
  assert.deepEqual(await page.evaluate(() => [CN.S.library.kind, document.querySelector('#libKind button.on').textContent]), ['sets', 'Sets'], 'a reload opens on Sets');
  await page.waitForSelector('#libDone .ldItem[data-kind=set]', { timeout: 10000 });
  await page.goto('about:blank');
  await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
  await page.waitForFunction(() => window.LibraryDone && LibraryDone.tab() === 'done' && document.querySelector('#libDone .ldItem'), null, { timeout: 30000 });
  assert.equal(await page.evaluate(() => location.hash), '#library/completed');
  await page.click('.topbar button[data-mode="nest"]');
  await page.waitForFunction(() => location.hash === '#nest');
  await page.goBack();
  await page.waitForFunction(() => CN.S.mode === 'library' && location.hash === '#library/completed' && !document.querySelector('#libDone').hidden, null, { timeout: 10000 });
  ok.push('a reload, and a page opened afresh on #library, open the Completed tab again, on Sets; Back returns to it');

  assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  for (const line of ok) console.log('✓ ' + line);
  console.log('Library Completed UI OK');
  await browser.close(); srv.close();
})().catch(e => { console.error(e); process.exit(1); });
