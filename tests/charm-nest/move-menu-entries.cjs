// Browser test of the Library's "Move to…" menu (charm-nest-library-dnd.js, the keyboard way in from each card's grip): it lists
// only the places that can apply. Offline, on the real Library page over the local stand-in for the site (bridge-server.cjs:
// the real charmNestLibrary function over an in-memory Firestore); nothing live is ever called.
//
//   a SET lists the other process columns only: never its own column, "New set", or any set (a set holds sheets, it does not
//   go into one); a SHEET lists every other column, "New set" and the other sets, never its own column or its own set; a place
//   that waits on something (Laser cutting not ready) stays, greyed, with its plain reason; with nothing possible the menu
//   says "Nowhere to move this right now". Dragging is not touched: the dock still lists and dims every place and a drop on a
//   place that does not apply still says why (the same checks are in library-dnd.cjs).
//
// Two page contexts: A has a LibraryFlow stand-in with the exact shapes of plans/library-flow/contract.md (explainTargets gives
// the reasons the real one gives: "It is already in …", a not-ready reason for Laser cutting), B has the REAL LibraryFlow.
//   node tests/charm-nest/move-menu-entries.cjs [playwright dir]      (SHOTS=<dir> also saves the pictures: 1440 and 390 px wide)
const fs = require('fs'), path = require('path'), assert = require('assert'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const REAL = !!process.env.REAL;
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets';

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

/* Today: set 1 (gold + silver) and set 2 (silver + rose) are ready for the laser; set 3 (gold) and set 4 (gold) are still in progress. */
const PLAN = [
  { setId: 'set-t-1', seq: 1, ready: true, sheets: [['dA01', 'gold'], ['dA02', 'silver']] },
  { setId: 'set-t-2', seq: 2, ready: true, sheets: [['dB01', 'silver'], ['dB02', 'rose']] },
  { setId: 'set-t-3', seq: 3, ready: false, sheets: [['dC01', 'gold']] },
  { setId: 'set-t-4', seq: 4, ready: false, sheets: [['dD01', 'gold']] }
];
function seed(st, blobUrl, roseReady = false) {
  const now = Date.now(), today = new Date(now).toISOString().slice(0, 10);
  let n = 0; const lines = {};
  for (const s of PLAN) {
    s.sheets.forEach(([id, metal], k) => {
      n++; const p = `charmnest/sets/${today}/Set-${s.seq}/${id}/preview.png`;
      st.blobs.set(p, { buf: preview(metal, n), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
      const orders = Array.from({ length: 3 + (n % 3) }, (_, i) => String(3700000000 + n * 10 + i));
      const rec = { id, metal, metalLabel: metal, day: today, setId: s.setId, setSeq: s.seq, sheetIndex: k + 1, fileBase: `${metal.slice(0, 2).toUpperCase()}_${today}_Set-${s.seq}_Sheet-${k + 1}`, folder: `${metal}_${id}`, orders, poolIds: orders.map(o => `${o}_1_1`), listings: orders.map((o, i) => String(1718000 + (i % 4))), placedCount: orders.length, charmCount: orders.length, density: 0.7, freePt2: 2000, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url: blobUrl(p) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: `run-${today}` };
      if (s.ready && (roseReady || metal !== 'rose')) {
        orders.forEach(o => { (lines[rec.runId] = lines[rec.runId] || {})[`${o}_1`] = { orderId: o, state: 'written', quantity: 1, poolIds: [`${o}_1_1`], engraveCandidate: false }; });
        Object.assign(rec, { processReady: true, processSeals: [{ id: `laserReady-${now}-${n}`, how: 'laserReady', at: now - 3600000, by: 'Anna' }], outputs: Object.assign({}, rec.outputs, { ai: { path: `${id}.ai`, url: blobUrl(`${id}.ai`) } }), label: { files: [{ path: `${id}-qr.png`, url: rec.outputs.preview.url, payload: orders[0], orders }] } });
      } else Object.assign(rec, { placedCount: orders.length - 1 });
      st.put(SHEETS, id, rec);
    });
    st.put(SETS, s.setId, { setId: s.setId, seq: s.seq, day: today, runId: `run-${today}`, name: `Set-${s.seq}`, sheetIds: s.sheets.map(x => x[0]), materials: s.sheets.map(x => x[1]), orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now - s.seq * 1000), createdAt: Timestamp.fromMillis(now - 86400000), ...(s.ready ? { processReady: true, processSeals: [{ id: `laserReady-${now}-s${s.seq}`, how: 'laserReady', at: now - 3600000, by: 'Anna' }] } : {}) });
  }
  for (const [runId, ls] of Object.entries(lines)) st.put('Charm_Nest_Runs', runId, { runId, lines: ls });
}
/** A sheet's membership changes in the cloud (what the real LibraryFlow.commit would write): its old set loses it, the new one gains it. */
function moveSheet(st, id, toSetId) {
  const rec = st.doc(SHEETS, id), from = st.doc(SETS, rec.setId), to = st.doc(SETS, toSetId);
  st.put(SETS, from.setId, { sheetIds: from.sheetIds.filter(x => x !== id) });
  st.put(SETS, to.setId, { sheetIds: [...to.sheetIds, id] });
  st.put(SHEETS, id, { setId: to.setId, setSeq: to.seq, updatedAt: Timestamp.fromMillis(Date.now()) });
}

/* ── the LibraryFlow stand-in (exact contract shapes), serialised into the page ── */
function fakeFlow(cfg) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  window.__calls = [];
  const AREAS = { progress: 'In progress', laser: 'Laser cutting', completed: 'Completed' };
  const cardOf = it => it.kind === 'sheet' ? document.querySelector(`#libBody .libCard[data-id="${it.id}"]`) : [...document.querySelectorAll('#libBody .setCard')].find(c => c._laserSet && c._laserSet.setId === it.id);
  const areaOf = el => { if (!el) return null; if (el.closest('#libDone')) return 'completed'; const s = el.closest('[data-laser-area]'); return s ? (s.dataset.laserArea === 'ready' ? 'laser' : 'progress') : null; };
  function explain(it) {
    if (cfg.zones && cfg.zones[it.id]) return cfg.zones[it.id].map(z => Object.assign({}, z, z.area ? { name: AREAS[z.area] } : {}));
    const el = cardOf(it), cur = areaOf(el), own = it.kind === 'sheet' && el && el.closest('.setCard') && el.closest('.setCard')._laserSet ? el.closest('.setCard')._laserSet.setId : null;
    const out = Object.keys(AREAS).map(a => ({ area: a, name: AREAS[a], ok: a !== cur, reason: a === cur ? `It is already in ${AREAS[a]}.` : '' }));
    if (it.kind === 'sheet' && cfg.notReady.includes(it.id)) { const z = out.find(x => x.area === 'laser'); if (z.ok) { z.ok = false; z.reason = 'Back engravings missing: 2 of 5'; } }
    if (it.kind === 'sheet') {
      for (const r of new Set((CN.S.library.rows || []).map(r => r.setId).filter(Boolean))) if (r !== own) out.push({ set: r, name: 'Set', ok: true, reason: '' });
      const lone = cfg.lone.includes(it.id);
      out.push({ newSet: true, name: 'New set', ok: lone, reason: lone ? '' : 'It is already in a set.' });
    }
    return out;
  }
  const key = z => z.area ? { area: z.area } : z.set ? { set: z.set } : { newSet: true };
  window.LibraryFlow = {
    explainTargets(item) { window.__calls.push(['explain', item.kind, item.id]); return explain(item); },
    targets(item) { window.__calls.push(['targets', item.kind, item.id]); return explain(item).filter(z => z.ok).map(key); },
    async plan(q) {
      window.__calls.push(['plan', q.kind, q.id, JSON.stringify(q.to)]); await sleep(150);
      return { kind: q.kind, id: q.id, move: { kind: q.kind, id: q.id, to: q.to }, from: {}, to: Object.assign({ area: null, setId: null }, q.to.set ? { setId: q.to.set } : q.to), ok: true, auto: [{ key: 'membership', label: 'Moved' }], needs: [], confirm: [], notes: [] };
    },
    async commit(plan, o) { window.__calls.push(['commit', plan.id, JSON.stringify(plan.move.to)]); await sleep(150); return { ok: true, applied: plan.auto.map(a => ({ key: a.key, label: a.label })) }; },
    async approve() { return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; }
  };
}

const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what + ' ' + fn); await new Promise(r => setTimeout(r, 60)); } };

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  seed(st, srv.blobUrl);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const ok = [];

  async function open(o = {}) {
    st.docs.clear(); seed(st, srv.blobUrl, !!(REAL || o.real));                    // (every page context starts from the same four sets: the earlier ones moved sheets about)
    const ctx = await browser.newContext({ viewport: { width: o.w || 1400, height: o.h || 1700 }, reducedMotion: o.reduced ? 'reduce' : 'no-preference' });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
    // (the real LibraryFlow, LibraryFx, LibraryApprovalUI and LibraryFlowRose are on the page: they are left out so that the fakes
    // below are the only ones, unless the page is to run on the real ones: REAL=1, or open({ real: true }))
    if (!(REAL || o.real)) for (const re of [/charm-nest-flow\.js/, /charm-nest-library-(fx|approval-ui)\.js/, /charm-nest-flow-rose\.js/]) await ctx.route(re, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: '/* left out by the test */' }));
    await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => { errors.push('page: ' + e.message); if (process.env.DEBUG) console.log('PAGEERROR', e.message, (e.stack || '').split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
    await page.exposeFunction('__fixtureMove', (id, to) => moveSheet(st, id, to));
    if (o.flow) {
      const cfg = Object.assign({ planMs: 350, commitMs: 350, blocked: ['dC01'], fixtureMove: true, failCommit: false }, o.cfg || {});
      await page.addInitScript(`(${fakeFlow.toString()})(${JSON.stringify(cfg)})`);
      if (o.modules) await page.addInitScript(`(${fakeModules.toString()})()`);
    }
    await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone, null, { timeout: 60000 });
    await page.waitForSelector('#libBody .setCard', { timeout: 30000 });
    await page.waitForFunction(() => document.querySelectorAll('#libBody .setCard').length >= 4 && CN.S.library.rows.length >= 6, null, { timeout: 20000 });
    await page.waitForTimeout(500);
    return { ctx, page, errors };
  }
  const box = (page, sel) => page.evaluate(s => { const e = typeof s === 'string' ? document.querySelector(s) : s; const r = e.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, l: r.left, t: r.top, w: r.width, h: r.height }; }, sel);
  const sheetSel = id => `#libBody .libCard[data-id="${id}"]`;
  const setSel = n => `#libBody .setCard:has(.libCard[data-id="${n}"])`;
  const area = (page, id) => page.evaluate(i => { const c = document.querySelector(`#libBody .libCard[data-id="${i}"]`); const s = c && c.closest('[data-laser-area]'); return s ? (s.dataset.laserArea === 'ready' ? 'laser' : 'progress') : null; }, id);
  const setOfCard = (page, id) => page.evaluate(i => { const c = document.querySelector(`#libBody .libCard[data-id="${i}"]`); const s = c && c.closest('.setCard'); return s && s._laserSet ? s._laserSet.setId : null; }, id);
  /** The mouse picks a card up (moves past the 6 px threshold) and carries it over `to` (a selector or a point). */
  async function carry(page, fromSel, to, o = {}) {
    const a = await box(page, fromSel); const x0 = a.l + 24, y0 = a.t + 14;
    await page.mouse.move(x0, y0); await page.mouse.down(); await page.mouse.move(x0 + 10, y0 + 8, { steps: 3 });
    try { await page.waitForSelector('.dndDock .dndChip', { timeout: 5000 }); }
    catch (e) {
      const d = await page.evaluate(() => ({ state: LibraryDnd.state(), docks: document.querySelectorAll('.dndDock').length, lifts: document.querySelectorAll('.dndLift').length, chips: document.querySelectorAll('.dndChip').length, html: document.querySelector('.dndDock') ? document.querySelector('.dndDock').outerHTML.slice(0, 600) : null, rect: document.querySelector('.dndChip') ? JSON.stringify(document.querySelector('.dndChip').getBoundingClientRect()) : null, cls: document.documentElement.className, calls: window.__calls }));
      console.log('DIAG', JSON.stringify(d)); throw e;
    }
    let b = typeof to === 'string' ? await box(page, to) : to;
    await page.mouse.move(b.x, b.y, { steps: o.steps || 12 });
    const frames = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));   // (the place under the hand is worked out once a frame)
    await frames();
    // (the page may have moved the place while the hand was on its way: pictures arriving, a list drawn again; the hand follows it)
    for (let i = 0; i < 4 && typeof to === 'string'; i++) { const b2 = await box(page, to); if (Math.hypot(b2.x - b.x, b2.y - b.y) < 2) break; b = b2; await page.mouse.move(b.x, b.y, { steps: 4 }); await frames(); }
    return b;
  }
  /** Put the bar away: its own X when it has one, and the pointer taken off it (a pointer that rests on a bar holds it open). */
  const dismiss = async page => { const x = await page.$('.dndX:not([hidden])'); if (x && await x.isVisible()) await x.click().catch(() => {}); await page.mouse.move(8, 8); };
  const KEYS = { 'In progress': 'area:progress', 'Laser cutting': 'area:laser', Completed: 'area:completed', 'New set': 'newSet' };
  const chipSel = name => `.dndChip[data-key="${KEYS[name] || 'set:set-t-' + name.replace('Set ', '')}"]`;

  const gripOf = (kind, id) => kind === 'sheet' ? `#libBody .libCard[data-id="${id}"] > .h > .dndGrip` : `#libBody .setCard:has(.libCard[data-id="${id}"]) > .sh > .dndGrip`;
  const settled = page => page.waitForFunction(() => { const m = document.querySelector('#libBody .dndMenuIn'); return m && !m.querySelector('.dndMenuWait'); }, null, { timeout: 8000 });
  /** The Library draws its lists again a few seconds after it opens (the live read): wait until #libBody has been still for 2 s. */
  const quiet = page => page.evaluate(() => new Promise(res => { let t = setTimeout(done, 2000), n = 0; const stop = setTimeout(done, 15000); const o = new MutationObserver(() => { clearTimeout(t); t = setTimeout(done, 2000); }); o.observe(document.getElementById('libBody'), { childList: true, subtree: true, attributes: true }); function done() { o.disconnect(); clearTimeout(t); clearTimeout(stop); res(true); } }));
  /** Focus the grip, press Enter and read the menu: its title and each entry (name, greyed, the line under it). */
  async function menuOf(page, grip) {
    await page.evaluate(s => document.querySelector(s).focus(), grip);
    await page.keyboard.press('Enter');
    await settled(page);
    return page.evaluate(() => ({
      title: document.querySelector('#libBody .dndMenuTitle').textContent,
      items: [...document.querySelectorAll('#libBody .dndMenu .dndMenuItem')].map(x => ({ name: x.firstChild.textContent, off: x.getAttribute('aria-disabled') === 'true', note: x.lastChild.textContent })),
      none: [...document.querySelectorAll('#libBody .dndMenuNone')].map(x => x.textContent),
      spin: document.querySelectorAll('#libBody .dndMenuIn .dndSpin').length
    }));
  }
  const closeMenu = async (page, grip) => { await page.keyboard.press('Escape'); await until(() => page.evaluate(() => !document.querySelector('#libBody .dndMenuWrap')), 3000, 'menu closed'); };
  const names = m => m.items.map(x => x.name.replace(/ · .*$/, ''));
  const noAlready = m => assert(!m.items.some(x => /already in|cannot go inside|holds its sheets together/i.test(x.note)), 'no entry says it is already there or can never go there: ' + JSON.stringify(m.items));

  /* ═════ A · a LibraryFlow stand-in with the real reasons ═════ */
  {
    const { ctx, page, errors } = await open({ flow: true, cfg: { notReady: ['dC01'], lone: ['dD01'] }, w: 1400, h: 1700 });
    await quiet(page);

    // 1 · a SET in In progress (Set 3): Laser cutting and Completed, as they read before, and nothing else
    let m = await menuOf(page, gripOf('set', 'dC01'));
    assert.equal(m.title, 'Move Set 3 to…');
    assert.deepEqual(m.items, [{ name: 'Laser cutting', off: false, note: 'ready to cut' }, { name: 'Completed', off: false, note: 'cut by the laser' }], 'a set in progress lists Laser cutting and Completed only: ' + JSON.stringify(m.items));
    assert.deepEqual(m.none, []);
    assert.equal(await page.evaluate(() => document.activeElement.firstChild.textContent), 'Laser cutting', 'focus goes to the first place');
    await closeMenu(page);
    ok.push('A · a set in In progress: "Move Set 3 to…" lists Laser cutting (ready to cut) and Completed (cut by the laser) only; no In progress, no New set, no set');

    // 2 · a SET in Laser cutting (Set 1): In progress (backwards) and Completed, never Laser cutting
    m = await menuOf(page, gripOf('set', 'dA01'));
    assert.equal(m.title, 'Move Set 1 to…');
    assert.deepEqual(m.items, [{ name: 'In progress', off: false, note: 'still being prepared' }, { name: 'Completed', off: false, note: 'cut by the laser' }], 'a set in Laser cutting lists In progress and Completed: ' + JSON.stringify(m.items));
    await closeMenu(page);
    ok.push('A · a set in Laser cutting: In progress (backwards) and Completed, not Laser cutting, no New set, no set');

    // 3 · a SHEET in a set in progress, Laser cutting not ready (Set 3 Sheet 1): its own column and its own set go, Laser cutting stays with its reason
    m = await menuOf(page, gripOf('sheet', 'dC01'));
    assert.deepEqual(names(m), ['Laser cutting', 'Completed', 'Set 4', 'Set 2', 'Set 1'], 'a sheet lists every other column and the other sets: ' + JSON.stringify(m.items));
    assert.deepEqual(m.items[0], { name: 'Laser cutting', off: true, note: 'Back engravings missing: 2 of 5' }, 'Laser cutting not ready stays, greyed, with its plain reason');
    assert.equal(m.items[1].off, false); assert(m.items.slice(2).every(x => !x.off), 'the other sets can take it');
    assert(!names(m).includes('In progress') && !names(m).includes('Set 3'), 'neither its own column nor its own set');
    assert(!names(m).includes('New set'), 'a sheet that is already in a set has no New set to start');
    noAlready(m);
    await closeMenu(page);
    ok.push('A · a sheet in Set 3 (in progress): no In progress, no Set 3, no "Already in" line; Laser cutting stays greyed with "Back engravings missing: 2 of 5"; Completed, Set 4, Set 2 and Set 1 are listed');

    // 4 · a SHEET the flow lets start a new set: New set is listed
    m = await menuOf(page, gripOf('sheet', 'dD01'));
    assert.deepEqual(names(m), ['Laser cutting', 'Completed', 'New set', 'Set 3', 'Set 2', 'Set 1']);
    assert.deepEqual(m.items[2], { name: 'New set', off: false, note: 'start a new set' });
    noAlready(m);
    await closeMenu(page);
    ok.push('A · a sheet that can start a new set lists New set and the other sets, not its own');

    // 5 · a SHEET in Laser cutting: In progress (backwards), Completed, the other sets; not Laser cutting, not its own set
    m = await menuOf(page, gripOf('sheet', 'dA01'));
    assert.deepEqual(names(m), ['In progress', 'Completed', 'Set 4', 'Set 3', 'Set 2'], JSON.stringify(m.items));
    noAlready(m);
    await closeMenu(page);
    ok.push('A · a sheet in Laser cutting: In progress, Completed and the other sets; not Laser cutting, not Set 1');

    // 6 · the keyboard walks the entries that are left, and only those
    m = await menuOf(page, gripOf('sheet', 'dC01'));
    const at = () => page.evaluate(() => document.activeElement.firstChild && document.activeElement.firstChild.textContent.replace(/ · .*$/, ''));
    assert.equal(await at(), 'Completed', 'focus goes to the first place that is allowed (Laser cutting is greyed)');
    const seen = []; for (let i = 0; i < 5; i++) { await page.keyboard.press('ArrowDown'); seen.push(await at()); }
    assert.deepEqual(seen, ['Set 4', 'Set 2', 'Set 1', 'Laser cutting', 'Completed'], 'ArrowDown walks the five entries and wraps: ' + seen);
    await page.keyboard.press('ArrowUp'); assert.equal(await at(), 'Laser cutting');
    await page.keyboard.press('ArrowUp'); assert.equal(await at(), 'Set 1');
    await page.keyboard.press('End'); assert.equal(await at(), 'Set 1');
    await page.keyboard.press('Home'); assert.equal(await at(), 'Laser cutting');
    await page.evaluate(() => { __calls.length = 0; });
    if (process.env.DEBUG) await page.evaluate(() => { window.__fo = []; document.addEventListener('focusout', e => __fo.push([e.target.outerHTML.slice(0, 70), e.relatedTarget && e.relatedTarget.tagName, (new Error().stack || '').split('\n').slice(1, 4).join('|')]), true); new MutationObserver(ms => { for (const m of ms) for (const n of m.removedNodes) if (n.nodeType === 1 && (n.matches('.dndMenuWrap, .libCard, .setCard, .dndMenuItem') || n.querySelector('.dndMenuItem'))) __fo.push(['REMOVED', n.className, (new Error().stack || '').slice(0, 10)]); }).observe(document.body, { childList: true, subtree: true }); });
    if (process.env.DEBUG) console.log('DBG0', await page.evaluate(() => document.activeElement.outerHTML.slice(0, 100)));
    await page.keyboard.press('Enter');                                   // on the greyed Laser cutting: nothing happens
    if (process.env.DEBUG) { await page.waitForTimeout(700); console.log('DBGFO', await page.evaluate(() => JSON.stringify(__fo))); }
    if (process.env.DEBUG) console.log('DBG1', await page.evaluate(() => document.activeElement.outerHTML.slice(0, 100)));
    await page.waitForTimeout(500);
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'plan').length), 0, 'Enter on a greyed entry plans nothing');
    assert.equal(await page.evaluate(() => !!document.querySelector('#libBody .dndMenuWrap')), true, 'and the menu stays open');
    await page.keyboard.press('ArrowDown');                               // Completed
    if (process.env.DEBUG) console.log('DBG', await at(), await page.evaluate(() => JSON.stringify([__calls, LibraryDnd.state(), !!document.querySelector('#libBody .dndMenuWrap')])));
    await page.keyboard.press('Enter');
    if (process.env.DEBUG) { await page.waitForTimeout(800); console.log('DBG2', await page.evaluate(() => JSON.stringify([__calls, LibraryDnd.state(), document.activeElement.outerHTML.slice(0, 120)]))); }
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'plan' && c[1] === 'sheet' && c[2] === 'dC01' && c[3] === '{"area":"completed"}')), 4000, 'Enter on Completed plans the move');
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit' && c[1] === 'dC01')), 5000, 'and commits it');
    await page.evaluate(() => LibraryDnd.cancel()); await dismiss(page);
    ok.push('A · keyboard: focus starts on the first allowed place, arrows and Home/End walk only the listed entries and wrap, Enter on a greyed one does nothing, Enter on Completed plans and commits the move');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  /* ═════ A2 · one place possible, and none ═════ */
  {
    const zonesFor = (own, only) => ['progress', 'laser', 'completed'].map(a => ({ area: a, ok: a === only, reason: a === only ? '' : `It is already in ${a === 'progress' ? 'In progress' : a === 'laser' ? 'Laser cutting' : 'Completed'}.` }));
    const { ctx, page, errors } = await open({ flow: true, cfg: { notReady: [], lone: [], zones: { 'set-t-4': zonesFor('set-t-4', 'completed'), 'set-t-1': zonesFor('set-t-1', null) } }, w: 1400, h: 1700 });
    await quiet(page);
    // one entry: the menu opens normally, focus on it, Enter moves
    let m = await menuOf(page, gripOf('set', 'dD01'));
    assert.deepEqual(m.items, [{ name: 'Completed', off: false, note: 'cut by the laser' }], JSON.stringify(m.items));
    assert.deepEqual(m.none, []);
    assert.equal(await page.evaluate(() => document.activeElement.firstChild.textContent), 'Completed');
    await page.evaluate(() => { __calls.length = 0; });
    await page.keyboard.press('Enter');
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'plan' && c[1] === 'set' && c[2] === 'set-t-4' && c[3] === '{"area":"completed"}')), 4000, 'the one entry moves it');
    await page.evaluate(() => LibraryDnd.cancel()); await dismiss(page);
    await until(() => page.evaluate(() => !document.querySelector('#libBody .dndMenuWrap') && !LibraryDnd.state().move), 6000, 'quiet again');
    ok.push('A · only one place possible: the menu opens normally with that one entry, focused, and Enter moves');
    // none: one calm line, no entries, no spinner; Escape closes it and focus returns to the grip
    m = await menuOf(page, gripOf('set', 'dA01'));
    assert.deepEqual(m.items, []);
    assert.deepEqual(m.none, ['Nowhere to move this right now'], 'the one calm line');
    assert.equal(m.spin, 0, 'the wait is over: no spinner');
    assert.equal(await page.evaluate(() => document.activeElement.className.includes('dndGrip')), true, 'focus stays on the grip');
    await page.keyboard.press('ArrowDown'); await page.keyboard.press('End');
    await page.keyboard.press('Escape');
    await until(() => page.evaluate(() => !document.querySelector('#libBody .dndMenuWrap')), 3000, 'menu closed');
    assert.equal(await page.evaluate(() => document.activeElement.className.includes('dndGrip')), true, 'Escape returns to the grip');
    ok.push('A · no place possible: one line "Nowhere to move this right now" (no entries, no spinner), arrows do nothing, Escape closes it');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  /* ═════ A3 · dragging is as it was: the dock still lists and dims every place, a drop on one that does not apply says why ═════ */
  {
    const { ctx, page, errors } = await open({ flow: true, cfg: { notReady: [], lone: [] }, w: 1400, h: 1700 });
    await quiet(page);
    await carry(page, '#libBody .setCard:has(.libCard[data-id="dC01"]) > .sh', chipSel('Set 1'));       // (a held set carried over the Set 1 chip)
    const dock = await page.$$eval('.dndDock .dndChip', c => c.map(x => [x.querySelector('.dndChipName').textContent.replace(/ · .*$/, ''), x.dataset.state, x.querySelector('.dndChipSub').textContent]));
    const st_ = n => (dock.find(c => c[0] === n) || [])[1];
    assert.equal(st_('In progress'), 'dim', 'the set\'s own column is still on the dock, dimmed: ' + JSON.stringify(dock));
    assert.equal(st_('Laser cutting'), 'armed'); assert.equal(st_('Completed'), 'armed');
    assert.equal(st_('New set'), 'dim', 'New set is on the dock, dimmed');
    for (const n of ['Set 1', 'Set 2', 'Set 3', 'Set 4']) assert.equal(st_(n), 'dim', n + ' is dimmed on the dock');
    // dropped on a set: still refused, with its reason; dropped on its own column: the same
    await page.mouse.up();
    await page.waitForSelector('.dndDock .dndLine.bad b', { timeout: 4000 });
    assert.match(await page.textContent('.dndDock .dndLine.bad b'), /A set cannot go inside another set/);
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'plan' || c[0] === 'commit').length), 0, 'nothing is planned or written');
    await dismiss(page); await until(() => page.evaluate(() => !document.querySelector('.dndDock') && !document.querySelector('.dndLift')), 9000, 'dock gone');
    ok.push('A · dragging is as it was: a held set still shows In progress, New set and every set on the dock, dimmed with their reasons; dropped on a set it is refused with its reason and nothing is planned');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  /* ═════ B · the real LibraryFlow ═════ */
  {
    const { ctx, page, errors } = await open({ real: true, flow: false, w: 1400, h: 1700 });
    await quiet(page);
    assert.equal(await page.evaluate(() => !!(window.LibraryFlow && LibraryFlow.explainTargets && !window.__calls)), true, 'the real LibraryFlow is on the page');
    let m = await menuOf(page, gripOf('set', 'dC01'));
    assert.deepEqual(m.items.map(x => x.name), ['Laser cutting', 'Completed'], 'real flow, a set: ' + JSON.stringify(m.items));
    await closeMenu(page);
    m = await menuOf(page, gripOf('set', 'dA01'));
    assert.deepEqual(m.items.map(x => x.name), ['In progress', 'Completed'], 'real flow, a set in Laser cutting: ' + JSON.stringify(m.items));
    await closeMenu(page);
    m = await menuOf(page, gripOf('sheet', 'dC01'));
    const n = names(m);
    assert(n.includes('Laser cutting') && n.includes('Completed'), 'real flow, a sheet: ' + JSON.stringify(m.items));
    assert(!n.includes('In progress') && !n.includes('Set 3'), 'not its own column or set: ' + n);
    noAlready(m);
    await closeMenu(page);
    ok.push('B · the real LibraryFlow: a set lists only the other columns, a sheet lists no "Already in" entry and not its own column or set');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  /* ═════ C · the pictures: the set menu and a sheet menu at 1440 and 390 px (SHOTS=<dir>) ═════ */
  if (SHOTS) for (const [w, h] of [[1440, 900], [390, 844]]) {
    const { ctx, page, errors } = await open({ flow: true, cfg: { notReady: ['dC01'], lone: [] }, w, h });
    await page.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w);   // (the rail folded below 900 px, as a person does)
    await page.waitForTimeout(450);
    await quiet(page);
    for (const [kind, id, file] of [['set', 'dC01', 'set-menu'], ['sheet', 'dC01', 'sheet-menu']]) {
      await menuOf(page, gripOf(kind, id));
      await page.evaluate(g => document.querySelector(g).closest('.setCard, .libCard').scrollIntoView({ block: 'start' }), gripOf(kind, id));
      await page.waitForTimeout(700);                                       // (the menu has opened and the page has settled)
      const r = await page.evaluate(() => { const g = document.querySelector('#libBody .dndMenuWrap'), c = g.closest('.setCard, .libCard'); const a = c.getBoundingClientRect(), b = g.getBoundingClientRect(); const x = Math.max(0, a.left - 12), y = Math.max(0, a.top - 12); return { x, y, w: Math.min(innerWidth - x, a.width + 24), h: Math.min(innerHeight - y, b.bottom + 14 - y) }; });
      await page.screenshot({ path: path.join(SHOTS, `${file}-${w}.png`), clip: { x: r.x, y: r.y, width: r.w, height: r.h } });
      await page.screenshot({ path: path.join(SHOTS, `${file}-${w}-page.png`) });
      await closeMenu(page);
    }
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  await browser.close(); srv.close && srv.close();
  console.log(ok.map(x => '✓ ' + x).join('\n'));
  console.log(`\n${ok.length} groups passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
