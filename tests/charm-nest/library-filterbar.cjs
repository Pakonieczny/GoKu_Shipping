// LBF1 (Paul, 9 Nov 2026), the Library tab's head: "Image #1 [the 'Sheets | Sets' segmented toggle, Sets selected]: remove this from the Library tab. Image #2 [the 'Oldest first' sort button and the
// 'All metals · GF 14/20 · SS · RG 14/20 · 10K Gold · 14K Gold' pills]: consolidate this under a Filters pill button that expands the filters like the other tabs already do."
// The REAL page over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary function over an in-memory Firestore; no Etsy, nothing live). What it proves:
//   1  no Sheets | Sets switch and no metal pills or sort button in the Library's bar; the Library is the Sets view, and an old "sheets" choice is ignored
//   2  the sheets in no set (here a fully nested GF sheet with no set, on six days) are reachable in the Sets view: each is on a "Sheets" card, and opens as before
//   3  Filters: the Orders tab's button (icon, badge) and pop-over: "Metal" (All metals and the five metals) and "Sort by" (Newest first / Oldest first); Esc closes it, focus back
//   4  a metal narrows the sets exactly as the pills did (the same Sets.renderLibrary filter), the badge counts it, a gold chip under the bar says it and its × lets it go
//   5  Oldest first reorders the list as the button did (the "library" direction), counts in the badge and is a chip; Clear all and Clear let both go; the Completed tab follows too
//   6  keyboard: ArrowDown on the button opens it, the arrows walk the chips, Enter presses one; labels for a screen reader as on Orders
//   node tests/charm-nest/library-filterbar.cjs      (PW_DIR / CHROMIUM as the other browser tests)
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
  // (Paul, 10 Oct 2026, docs/charm-nest/set-rules.md R1 / R2: a set exists only with a COMPLETED GF and a COMPLETED SS sheet, and "completed" is
  //  releaseFull (fully nested), never the nest status word. So every current sheet is fully nested, every set holds a GF and an SS sheet, and the
  //  one-sheet "sets" of before are a GF sheet in no set: the plain case of a sheet that leaves Current when it is marked)
  for (let d = 0; d < 6; d++) for (let s = 1; s <= 2; s++) {
    const setId = `set-${day(d)}-${s}`, ids = [], n = 1 + ((d + s) % 3);
    if (n === 1) { sheet(`sh${d}x${s}x0`, 'gold', d, null, s, 0, { releaseFull: true, setSeq: null }); continue; }
    metals.slice(0, n).forEach((metal, k) => { const id = `sh${d}x${s}x${k}`; ids.push(id); sheet(id, metal, d, setId, s, k, { releaseFull: true }); });
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
  // (LBF1: two completed sheets that belong to no set, a GF draft and an SS sheet with no set: Completed showed them only under its Sheets view)
  { const now = Date.now(), day = new Date(now - 2 * 86400000).toISOString().slice(0, 10);
    for (const [id, metal, extra] of [['loose-gf', 'gold', { draft: true }], ['loose-ss', 'silver', {}]]) st.put(SHEETS, id, Object.assign({ id, metal, metalLabel: metal, day, setId: null, sheetIndex: 1, fileBase: `${metal}_${id}`, folder: id, orders: ['3799000001'], placedCount: 1, charmCount: 1, density: .4, laserDoneAt: now - 2 * 86400000, laserDoneBy: 'Anna', processReady: true, updatedAt: Timestamp.fromMillis(now - 2 * 86400000), createdAt: Timestamp.fromMillis(now - 3 * 86400000) }, extra)); }
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

  const fails = []; let nCheck = 0;
  const check = (c, msg) => { nCheck++; if (!c) fails.push(msg); console.log((c ? '  ✓ ' : '  ✗ ') + msg); };
  const sleep = ms => page.waitForTimeout(ms);
  try {
    // an old page that had chosen Sheets: the choice is not kept anywhere, but a stale state is ignored all the same
    await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone, null, { timeout: 60000 });
    await page.waitForSelector('#libBody .setCard', { timeout: 30000 });
    await sleep(400);

    // ═══ 1 · the bar ═══
    const head = await page.evaluate(() => { const b = document.querySelector('#libView .libBar'); return { kind: CN.S.library.kind, seg: !!document.getElementById('libKind'), segText: /\bSheets\b[\s\S]*\bSets\b/.test([...b.querySelectorAll('button')].map(x => x.textContent).join(' ')), pills: b.querySelectorAll('.fchip').length, dir: b.querySelectorAll('.cnDir,.cnListTools').length, filters: !!b.querySelector('#libFiltersBtn'), ceLast: b.lastElementChild && b.lastElementChild.classList.contains('cnCeTabBtn'), sheetsList: !!document.querySelector('#libBody [data-laser-card="sheet"]') }; });
    check(head.kind === 'sets' && !head.seg && !head.segText, 'no Sheets | Sets switch; the Library is the Sets view: ' + JSON.stringify(head));
    check(head.pills === 0 && head.dir === 0 && head.filters, 'no metal pills and no sort button in the bar; one Filters button where they were: ' + JSON.stringify(head));
    check(head.ceLast, 'the Charm Editor button is still the last thing of the bar, at its top right');
    const order = await page.evaluate(() => { const b = document.querySelector('#libView .libBar'), k = [...b.children].map(c => c.id || c.className.split(' ')[0]); return k; });
    check(order.indexOf('libFilters') === order.indexOf('libSearch') + 1, 'Filters sits right after the search, where the row was: ' + order.join(' · '));
    const bar = await page.evaluate(() => { const b = document.querySelector('#libView .libBar'); return { h: Math.round(b.getBoundingClientRect().height), over: b.scrollWidth - b.clientWidth }; });
    check(bar.h < 46 && bar.over <= 0, 'one slim line of bar at 1500 px: ' + JSON.stringify(bar));

    // ═══ 2 · the loose sheets ═══
    const loose = await page.evaluate(() => { const ids = CN.S.library.rows.filter(r => !r.setId && !r.laserDoneAt).map(r => r.id); const shown = ids.filter(id => document.querySelector(`#libBody .setCard .libCard[data-id="${CSS.escape(id)}"]`)); const cards = [...document.querySelectorAll('#libBody .setCard')].filter(c => c.querySelector('.sh .nm') && /^Sheets$/.test(c.querySelector('.sh .nm').textContent.trim())).length; const allSets = [...document.querySelectorAll('#libBody .setCard')]; const lastSet = allSets.map(c => !!(c._laserSet && c._laserSet.setId)).lastIndexOf(true), firstLoose = allSets.map(c => !(c._laserSet && c._laserSet.setId)).indexOf(true); return { ids: ids.length, shown: shown.length, cards, after: firstLoose < 0 || firstLoose > lastSet }; });
    check(loose.ids > 0 && loose.shown === loose.ids && loose.cards > 0, 'every sheet in no set is on a "Sheets" card of the Sets view: ' + JSON.stringify(loose));
    check(loose.after, 'and those cards come after the real sets, as before');
    const opened = await page.evaluate(async () => { const id = CN.S.library.rows.find(r => !r.setId && !r.laserDoneAt).id; let got = null; const was = window.SheetWin && SheetWin.open; if (window.SheetWin) SheetWin.open = x => { got = x; }; document.querySelector(`#libBody .setCard .libCard[data-id="${CSS.escape(id)}"]`).click(); if (window.SheetWin) SheetWin.open = was; return got === id; });
    check(opened, 'a loose sheet\'s card opens its sheet window, as it did in the Sheets view');

    // ═══ 3 · the Filters pop-over ═══
    const fb = '#libFiltersBtn', pop = '#libFiltersPop';
    const b0 = await page.evaluate(() => { const b = document.getElementById('libFiltersBtn'); return { icon: !!b.querySelector('svg.cnIc'), label: b.getAttribute('aria-label') || b.textContent.trim(), exp: b.getAttribute('aria-expanded'), popup: b.getAttribute('aria-haspopup'), ctl: b.getAttribute('aria-controls'), badge: b.querySelector('.cnBadge').hidden }; });
    check(b0.icon && b0.label === 'Filters' && b0.exp === 'false' && b0.popup === 'dialog' && b0.ctl === 'libFiltersPop' && b0.badge, 'the Filters button: the Orders icon, "Filters", aria-haspopup dialog, closed, no badge: ' + JSON.stringify(b0));
    // (the default: all metals, newest first)
    await page.evaluate(() => { CNListActivity.set('library', { direction: 'desc' }); });
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    const p0 = await page.evaluate(() => { const p = document.getElementById('libFiltersPop'); const r = p.getBoundingClientRect(); return { exp: document.getElementById('libFiltersBtn').getAttribute('aria-expanded'), focus: p.contains(document.activeElement), role: p.getAttribute('role'), groups: [...p.querySelectorAll('.ordFg h4')].map(h => h.textContent), metals: [...p.querySelectorAll('.ordChip[data-f="metal"]')].map(c => c.textContent.trim()), dirs: [...p.querySelectorAll('.ordChip[data-f="dir"]')].map(c => c.textContent.trim() + ':' + c.getAttribute('aria-pressed')), labelled: [...p.querySelectorAll('.ordChipRow')].every(r => document.getElementById(r.getAttribute('aria-labelledby'))), inside: r.left >= 0 && r.right <= innerWidth && r.bottom <= innerHeight, same: p.classList.contains('ordFp') && p.classList.contains('cnPop') }; });
    check(p0.exp === 'true' && p0.focus && p0.role === 'dialog' && p0.same && p0.inside, 'a press opens the Orders tab\'s panel (dialog, focus inside, inside the window): ' + JSON.stringify(p0));
    check(p0.groups.join() === 'Metal,Sort by', 'its sections are Metal and Sort by: ' + p0.groups);
    check(p0.metals.join('|') === 'All metals|GF 14/20|SS|RG 14/20|10K Gold|14K Gold', 'Metal: the same six choices as the pills: ' + p0.metals.join(' · '));
    check(p0.dirs.join('|') === 'Newest first:true|Oldest first:false' && p0.labelled, 'Sort by: Newest first (in force) and Oldest first; each row named by its heading: ' + p0.dirs);
    await page.keyboard.press('Escape'); await sleep(120);
    check(await page.locator(pop).isHidden() && await page.evaluate(() => document.activeElement && document.activeElement.id) === 'libFiltersBtn', 'Esc closes it and the focus is back on the Filters button');

    // ═══ 4 · a metal narrows as the pills did ═══
    const setsOf = () => page.evaluate(() => [...document.querySelectorAll('#libBody .setCard')].filter(c => c.getClientRects().length).map(c => (c._laserSet && c._laserSet.setId) || 'loose:' + (c._laserSheets || []).join('+')));
    const allSets = await setsOf();
    // what the pills did, by the same call (Sets.renderLibrary with filter): the expected list for silver
    const want = await page.evaluate(async () => { CN.S.library.metal = 'silver'; await Sets.renderLibrary(document.getElementById('libBody'), { reuse: true, filter: true }); const r = [...document.querySelectorAll('#libBody .setCard')].filter(c => c.getClientRects().length).map(c => (c._laserSet && c._laserSet.setId) || 'loose:' + (c._laserSheets || []).join('+')); CN.S.library.metal = 'all'; await Sets.renderLibrary(document.getElementById('libBody'), { reuse: true, filter: true }); return r; });
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    await page.click(pop + ' .ordChip[data-f="metal"][data-v="silver"]'); await sleep(700);
    const silver = await setsOf();
    const s1 = await page.evaluate(() => ({ metal: CN.S.library.metal, pressed: [...document.querySelectorAll('#libFiltersPop .ordChip[aria-pressed=true]')].map(c => c.dataset.f + ':' + c.dataset.v), badge: document.querySelector('#libFiltersBtn .cnBadge').hidden ? '' : document.querySelector('#libFiltersBtn .cnBadge').textContent, label: document.getElementById('libFiltersBtn').getAttribute('aria-label'), on: document.getElementById('libFiltersBtn').hasAttribute('data-on'), chips: [...document.querySelectorAll('#libActive .ordAx')].map(c => c.textContent.trim() + '|' + c.getAttribute('aria-label')), activeShown: !document.getElementById('libActive').hidden }));
    check(s1.metal === 'silver' && JSON.stringify(silver) === JSON.stringify(want) && silver.length > 0 && silver.length < allSets.length, 'SS shows exactly what the SS pill showed (' + silver.length + ' of ' + allSets.length + ' cards)');
    check(s1.pressed.join() === 'metal:silver,dir:desc' && s1.badge === '1' && s1.label === 'Filters, 1 applied' && s1.on, 'the chip is pressed, the badge says 1, the button says "Filters, 1 applied": ' + JSON.stringify(s1));
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'library-filters.png'), clip: { x: 0, y: 0, width: 1500, height: 520 } });
    check(s1.activeShown && s1.chips.length === 1 && /^SS\|Remove the filter: Metal, SS$/.test(s1.chips[0]), 'a gold chip under the bar says SS, with a label for a screen reader: ' + s1.chips);
    await page.keyboard.press('Escape'); await sleep(100);
    await page.click('#libActive .ordAx[data-ax="metal"]'); await sleep(700);
    const s2 = await page.evaluate(() => ({ metal: CN.S.library.metal, badge: document.querySelector('#libFiltersBtn .cnBadge').hidden, hidden: document.getElementById('libActive').hidden, focus: document.activeElement && document.activeElement.id }));
    check(s2.metal === 'all' && s2.badge && s2.hidden && JSON.stringify(await setsOf()) === JSON.stringify(allSets), 'its × lets it go: every metal again, no badge, no chip row: ' + JSON.stringify(s2));
    check(s2.focus === 'libFiltersBtn', 'and the focus goes to the Filters button, not lost');

    // ═══ 5 · Sort by ═══
    // what the old button did: the "library" direction flipped, then the Current list drawn again (its change callback: Sets.renderLibrary reuse)
    const wantAsc = await page.evaluate(async () => { CNListActivity.set('library', { direction: 'asc' }); await Sets.renderLibrary(document.getElementById('libBody'), { reuse: true }); const r = [...document.querySelectorAll('#libBody .setCard')].filter(c => c.getClientRects().length).map(c => (c._laserSet && c._laserSet.setId) || 'loose:' + (c._laserSheets || []).join('+')); CNListActivity.set('library', { direction: 'desc' }); await Sets.renderLibrary(document.getElementById('libBody'), { reuse: true }); return r; });
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    await page.click(pop + ' .ordChip[data-f="dir"][data-v="asc"]'); await sleep(800);
    const asc = await setsOf();
    const want2 = await page.evaluate(() => CNListActivity.state('library').direction);
    check(want2 === 'asc' && JSON.stringify(asc) === JSON.stringify(wantAsc) && JSON.stringify(asc) !== JSON.stringify(allSets), 'Oldest first draws the list exactly as the Oldest first button did (the "library" direction is asc), and it is another order than Newest first');
    await page.click(pop + ' .ordChip[data-f="metal"][data-v="gold"]'); await sleep(700);
    const s3 = await page.evaluate(() => ({ badge: document.querySelector('#libFiltersBtn .cnBadge').textContent, chips: [...document.querySelectorAll('#libActive .ordAx')].map(c => c.textContent.trim()), all: !!document.querySelector('#libActive [data-ax-all]') }));
    check(s3.badge === '2' && s3.chips.join() === 'GF 14/20,Oldest first' && s3.all, 'a metal and Oldest first: the badge says 2, both are chips, with Clear all: ' + JSON.stringify(s3));
    await page.keyboard.press('Escape'); await sleep(100);
    // the Completed tab follows the same metal and order
    await page.click('#libTab [data-t="done"]'); await page.waitForSelector('#libDone .ldItem', { timeout: 20000 }); await sleep(500);
    const d1 = await page.evaluate(() => ({ list: LibraryDone && document.querySelectorAll('#libDone .ldItem').length, badge: document.querySelector('#libFiltersBtn .cnBadge').textContent, metal: CN.S.library.metal, dir: CNListActivity.state('library').direction }));
    check(d1.list > 0 && d1.badge === '2' && d1.metal === 'gold' && d1.dir === 'asc', 'Completed keeps the same Filters (badge 2, GF, oldest first): ' + JSON.stringify(d1));
    await page.click('#libActive [data-ax-all]'); await sleep(800);
    const s4 = await page.evaluate(() => ({ metal: CN.S.library.metal, dir: CNListActivity.state('library').direction, badge: document.querySelector('#libFiltersBtn .cnBadge').hidden, hidden: document.getElementById('libActive').hidden, search: document.getElementById('libSearch').value }));
    check(s4.metal === 'all' && s4.dir === 'desc' && s4.badge && s4.hidden, 'Clear all lets both go (all metals, newest first): ' + JSON.stringify(s4));
    await page.click('#libTab [data-t="current"]'); await page.waitForSelector('#libBody .setCard'); await sleep(500);

    // ═══ 5b · Completed: the sheets in no set are its last group ═══
    await page.click('#libTab [data-t="done"]'); await page.waitForSelector('#libDone .ldItem', { timeout: 20000 });
    const scrollAll = async () => { for (let i = 0; i < 12; i++) { await page.evaluate(() => { const st = document.getElementById('stage'); st.scrollTop = st.scrollHeight; }); await sleep(500); if (await page.evaluate(() => /that is all/.test((document.querySelector('#libDone .ldMore') || {}).textContent || ''))) break; } };
    await scrollAll();
    const lz = await page.evaluate(() => { const secs = [...document.querySelectorAll('#libDone .ldRows > .ldDay')], g = document.querySelector('#libDone .ldDay.ldLoose'); return { last: secs.length && secs[secs.length - 1] === g, head: g && g.querySelector('.ldDayHead').textContent, ids: g ? [...g.querySelectorAll('.ldItem[data-kind="sheet"]')].map(x => x.dataset.id).sort() : [], elsewhere: [...document.querySelectorAll('#libDone .ldDay:not(.ldLoose) .ldItem[data-kind="sheet"]')].length, sets: document.querySelectorAll('#libDone .ldItem[data-kind="set"]').length, more: document.querySelector('#libDone .ldMore').textContent.trim() }; });
    check(lz.last && /^Sheets not in a set/.test(lz.head) && lz.ids.join() === 'loose-gf,loose-ss' && lz.elsewhere === 0 && lz.sets === 40, 'Completed: after the 40 sets, one "Sheets not in a set" group holds the two completed sheets in no set: ' + JSON.stringify(lz));
    check(/40 completed sets · 2 sheets not in a set/.test(lz.more), 'and the end line counts both: ' + lz.more);
    const opened2 = await page.evaluate(() => { let got = null; const was = window.SheetWin && SheetWin.open; if (window.SheetWin) SheetWin.open = x => { got = x; }; document.querySelector('#libDone .ldLoose .ldItem[data-id="loose-ss"] .ldLine').click(); if (window.SheetWin) SheetWin.open = was; return got; });
    check(opened2 === 'loose-ss', 'a completed loose sheet opens its sheet window, as in Completed › Sheets');
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    await page.click(pop + ' .ordChip[data-f="metal"][data-v="silver"]'); await sleep(600); await page.keyboard.press('Escape');
    await scrollAll();
    const lz2 = await page.evaluate(() => [...document.querySelectorAll('#libDone .ldLoose .ldItem')].map(x => x.dataset.id).join());
    check(lz2 === 'loose-ss', 'SS in Filters narrows that group too (only the SS sheet): ' + lz2);
    await page.click('#libActive [data-ax="metal"]'); await sleep(500);
    await page.click('#libTab [data-t="current"]'); await page.waitForSelector('#libBody .setCard'); await sleep(500);

    // ═══ 6 · keyboard, and Clear in the panel ═══
    await page.focus(fb); await page.keyboard.press('ArrowDown'); await sleep(150);
    const k1 = await page.evaluate(() => ({ open: !document.getElementById('libFiltersPop').hidden, f: document.activeElement && document.activeElement.dataset.f + ':' + document.activeElement.dataset.v }));
    check(k1.open && k1.f === 'metal:', 'ArrowDown on the button opens it, the focus on the chip in force (All metals): ' + JSON.stringify(k1));
    await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowRight'); await page.keyboard.press('Enter'); await sleep(700);
    const k2 = await page.evaluate(() => ({ metal: CN.S.library.metal, focus: document.activeElement && document.activeElement.dataset.v }));
    check(k2.metal === 'silver' && k2.focus === 'silver', 'the arrows walk the chips, Enter presses the one in focus (SS), and the focus stays on it: ' + JSON.stringify(k2));
    await page.keyboard.press('End'); await page.keyboard.press('Enter'); await sleep(700);
    check(await page.evaluate(() => CNListActivity.state('library').direction) === 'asc', 'End goes to the last chip (Oldest first) and Enter presses it');
    await page.click(pop + ' [data-clear]'); await sleep(800);
    const k3 = await page.evaluate(() => ({ metal: CN.S.library.metal, dir: CNListActivity.state('library').direction, clearOff: document.querySelector('#libFiltersPop [data-clear]').disabled }));
    check(k3.metal === 'all' && k3.dir === 'desc' && k3.clearOff, 'the panel\'s Clear lets everything go and is then off: ' + JSON.stringify(k3));
    await page.click(pop + ' [data-done]'); await sleep(150);
    check(await page.locator(pop).isHidden(), 'Done closes the panel');

    // ═══ 7 · a phone ═══
    await page.setViewportSize({ width: 390, height: 800 }); await sleep(500);
    // (the bar is one line that scrolls sideways on a narrow window, as before: the button is brought into view and pressed)
    await page.evaluate(() => { const b = document.getElementById('libFiltersBtn'); b.scrollIntoView({ block: 'nearest', inline: 'center' }); b.click(); });
    await page.waitForSelector(pop + ':not([hidden])'); await sleep(300);
    const ph = await page.evaluate(() => { const p = document.getElementById('libFiltersPop'), r = p.getBoundingClientRect(), small = [...p.querySelectorAll('.ordChip'), document.getElementById('libFiltersBtn')].filter(x => x.getBoundingClientRect().height < 40).length; return { sheet: p.classList.contains('cnSheet'), inside: r.left >= 0 && r.right <= innerWidth + 0.5 && r.bottom <= innerHeight + 0.5 && r.top >= 0, small, page: document.documentElement.scrollWidth - innerWidth }; });
    check(ph.sheet && ph.inside && ph.small === 0 && ph.page <= 0, '390 px: a bottom sheet inside the window, every target at least 40 px, no sideways page: ' + JSON.stringify(ph));
    await page.keyboard.press('Escape');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } catch (e) { check(false, 'the test itself failed: ' + (e && e.stack || e)); }
  await browser.close(); srv.close();
  console.log(fails.length ? `\nFAIL: ${fails.length} of ${nCheck}\n - ` + fails.join('\n - ') : `\nLibrary Filters OK: ${nCheck} checks`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
