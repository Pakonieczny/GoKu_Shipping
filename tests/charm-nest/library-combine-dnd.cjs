// Browser test of "drop a draft sheet on another draft sheet" (charm-nest-library-dnd.js), on the real Library page over the local
// stand-in for the site (bridge-server.cjs: the real charmNestLibrary function over an in-memory Firestore). Nothing live is called.
//
// LibraryFlow is a FAKE with the exact shapes of plans/library-sets/contract.md (targets, plan, commit and sheetZone), installed here
// and nowhere else. The page has two draft sheets (GF and 14K, in the lists of sheets waiting for a set) and a set of two sheets.
//   node tests/charm-nest/library-combine-dnd.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets';

function crc32(buf) { let c, crc = 0xffffffff; for (let n = 0; n < buf.length; n++) { c = (crc ^ buf[n]) & 0xff; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; crc = (crc >>> 8) ^ c; } return (crc ^ 0xffffffff) >>> 0; }
function png(w, h, fn) {
  const raw = Buffer.alloc((w * 3 + 1) * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const [r, g, b] = fn(x, y), o = y * (w * 3 + 1) + 1 + x * 3; raw[o] = r; raw[o + 1] = g; raw[o + 2] = b; }
  const chunk = (t, d) => { const len = Buffer.alloc(4); len.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]), crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, crc]); };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}
const COLORS = { gold: [201, 166, 92], gold14k: [190, 150, 60], silver: [160, 166, 175] };
const preview = (metal, seed) => { const [r, g, b] = COLORS[metal]; return png(120, 100, (x, y) => ((x + seed * 13) % 24 - 10) ** 2 + ((y + seed * 7) % 22 - 9) ** 2 < 40 ? [r, g, b] : [252, 250, 246]); };

// set-t-1 (two sheets, in progress) and three drafts waiting for a set: dE01 (GF), dF01 (14K), dG01 (silver)
const SHEETS_PLAN = [['dA01', 'gold', 'set-t-1', 1], ['dA02', 'silver', 'set-t-1', 2], ['dE01', 'gold', null, 1], ['dF01', 'gold14k', null, 1], ['dG01', 'silver', null, 2]];
function seed(st, blobUrl) {
  const now = Date.now(), today = new Date(now).toISOString().slice(0, 10);
  let n = 0;
  for (const [id, metal, setId, k] of SHEETS_PLAN) {
    n++; const p = `charmnest/sets/${today}/${id}/preview.png`;
    st.blobs.set(p, { buf: preview(metal, n), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
    const orders = Array.from({ length: 3 + (n % 3) }, (_, i) => String(3700000000 + n * 10 + i));
    st.put(SHEETS, id, { id, metal, metalLabel: metal, day: today, ...(setId ? { setId, setSeq: 1 } : { draft: true }), sheetIndex: k, page: k, fileBase: `${metal.slice(0, 2).toUpperCase()}_${today}_Sheet-${k}`, folder: `${metal}_${id}`, orders, poolIds: orders.map(o => `${o}_1_1`), listings: [], placedCount: orders.length - 1, charmCount: orders.length, density: 0.36, freePt2: 2000, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url: blobUrl(p) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: `run-${today}` });
  }
  st.put(SETS, 'set-t-1', { setId: 'set-t-1', seq: 1, day: today, runId: `run-${today}`, name: 'Set-1', sheetIds: ['dA01', 'dA02'], materials: ['gold', 'silver'], orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now), createdAt: Timestamp.fromMillis(now - 86400000) });
  st.put('Charm_Nest_Runs', `run-${today}`, { runId: `run-${today}`, lines: {} });
}

/* the fake LibraryFlow (contract shapes), serialised into the page */
function fakeFlow() {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  window.__calls = [];
  const draft = id => /^d[EFG]/.test(id);
  window.LibraryFlow = {
    targets(item) { window.__calls.push(['targets', item.id]); return [{ area: 'progress' }, { area: 'laser' }, { area: 'completed' }, { newSet: true }, { set: 'set-t-1' }]; },
    sheetZone(item, other, o) {
      window.__calls.push(['sheetZone', item.id, other, o && o.self, o && o.name]);
      if (!draft(item.id)) return { sheet: other, name: 'Combine with ' + o.name, ok: false, reason: o.self + ' is already in a set: drop ' + o.name + ' on that set to add it.' };
      if (other === 'dG01') return { sheet: other, name: 'Combine with ' + o.name, ok: false, reason: o.name + ' is not ready to join a set. Its layout has not been verified yet.' };
      return { sheet: other, name: 'Combine with ' + o.name, sub: 'both start a new set', ok: true, reason: '' };
    },
    async plan(q) {
      window.__calls.push(['plan', q.kind, q.id, JSON.stringify(q.to)]);
      await sleep(250);
      const base = { kind: q.kind, id: q.id, move: { kind: q.kind, id: q.id, to: q.to }, from: {}, to: { area: null, setId: null }, auto: [], needs: [], confirm: [], notes: [] };
      return Object.assign(base, { ok: true, auto: [{ key: 'membership', label: 'Added to a new set' }, { key: 'membership:other', label: 'The other sheet added to a new set' }] });
    },
    async commit(plan, o) { window.__calls.push(['commit', plan.id, JSON.stringify(plan.move.to), JSON.stringify(o && o.confirmed)]); await sleep(250); return { ok: true, applied: plan.auto.map(a => ({ key: a.key, label: a.label })) }; },
    async approve() { return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; }
  };
}

const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 60)); } };

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  seed(st, srv.blobUrl);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 1700 }, reducedMotion: 'reduce' });
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
  for (const re of [/charm-nest-flow\.js/, /charm-nest-library-(fx|approval-ui)\.js/, /charm-nest-flow-rose\.js/]) await ctx.route(re, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: '/* left out by the test */' }));
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  await page.addInitScript(`(${fakeFlow.toString()})()`);
  await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone, null, { timeout: 60000 });
  await page.waitForSelector('#libBody .libCard[data-id="dE01"]', { timeout: 30000 });
  await page.waitForSelector('#libBody .libCard[data-id="dF01"]', { timeout: 30000 });
  await page.waitForFunction(() => document.querySelectorAll('#libBody .dndGrip').length > 0, null, { timeout: 15000 });
  await page.waitForTimeout(500);

  const sheetSel = id => `#libBody .libCard[data-id="${id}"]`;
  const box = (sel) => page.evaluate(s => { const e = document.querySelector(s); e.scrollIntoView({ block: 'center' }); const r = e.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, l: r.left, t: r.top, w: r.width, h: r.height }; }, sel);
  const frames = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const ok = [];
  /** Pick the card up by its head (past the 6 px threshold) and carry it over `toSel`'s centre. */
  async function carry(fromSel, toSel) {
    const a = await box(fromSel); const x0 = a.l + 24, y0 = a.t + 14;
    await page.mouse.move(x0, y0); await page.mouse.down(); await page.mouse.move(x0 + 10, y0 + 8, { steps: 3 });
    await page.waitForSelector('.dndDock .dndChip', { timeout: 8000 });
    const b = await box(toSel);                                           // (the page may have scrolled for the dock: the target is measured now)
    await page.mouse.move(b.x, b.y, { steps: 10 }); await frames(); await frames();
    return b;
  }
  const article = id => `#libBody .librarySheet:has(.libCard[data-id="${id}"])`;
  const state = id => page.evaluate(i => { const a = document.querySelector(`#libBody .librarySheet:has(.libCard[data-id="${i}"])`); return a ? { state: a.getAttribute('data-dnd-state'), hot: a.hasAttribute('data-dnd-hot'), label: a.getAttribute('data-dnd-label') } : null; }, id);
  const progressHot = () => page.evaluate(() => { const s = document.querySelector('#libBody [data-laser-area="pending"]'); return s ? { state: s.getAttribute('data-dnd-state'), hot: s.hasAttribute('data-dnd-hot') } : null; });
  const dismiss = async () => { const x = await page.$('.dndX:not([hidden])'); if (x && await x.isVisible()) await x.click().catch(() => {}); await page.mouse.move(8, 8); };
  const clean = async what => until(() => page.evaluate(() => !document.querySelector('.dndDock, .dndMovingWrap, .dndLift') && !document.querySelectorAll('[data-dnd-state]').length && !document.querySelector('.dndSource')), 15000, what + ' leaves nothing behind');

  // 1 · carried over another draft sheet: the card itself is the place ("Combine with ..."), armed and hot; the In progress section is not what answers
  await carry(sheetSel('dE01'), sheetSel('dF01'));
  let s = await state('dF01');
  assert.deepEqual({ state: s.state, hot: s.hot }, { state: 'armed', hot: true }, 'the 14K card is the place under the hand: ' + JSON.stringify(s));
  assert.match(s.label, /^Drop to combine with 14K Draft 1$/, s.label);
  assert.notDeepEqual((await progressHot()).hot, true, 'the whole In progress section is not what answers any more');
  const zc = await page.evaluate(() => window.__calls.filter(c => c[0] === 'sheetZone'));
  assert.deepEqual(zc, [['sheetZone', 'dE01', 'dF01', 'GF Draft 1', '14K Draft 1']], 'asked once, with both names: ' + JSON.stringify(zc));
  assert.equal(await page.evaluate(() => [...document.querySelectorAll('.dndChip')].some(c => /Combine/.test(c.textContent))), false, 'a sheet is a place on the page, not a chip');
  assert.equal(await page.evaluate(() => document.querySelectorAll('#libBody [data-dnd-state][data-dnd-label^="Drop to combine"]').length), 1, 'only the card under the hand is framed');
  await page.mouse.up();
  await until(() => page.evaluate(() => window.__calls.some(c => c[0] === 'commit')), 10000, 'committed');
  const calls = await page.evaluate(() => window.__calls.filter(c => c[0] === 'plan' || c[0] === 'commit'));
  assert.deepEqual(calls.map(c => [c[0], c[0] === 'plan' ? c[2] : c[1]]).slice(0, 2), [['plan', 'dE01'], ['commit', 'dE01']]); assert.equal(calls[0][3], '{"sheet":"dF01"}', 'the drop is planned as a drop on that sheet: ' + calls[0][3]); assert.equal(calls[1][2], '{"sheet":"dF01"}');
  // (the dock folds into the bar once the card has landed on the sheet: that is a flight of about a second, so it is waited for, not read at the instant of the commit)
  await until(() => page.evaluate(() => /^Moving /.test((document.querySelector('.dndDockHead b') || {}).textContent || '')), 8000, 'the dock is a bar');
  const title = await page.evaluate(() => (document.querySelector('.dndDockHead b') || {}).textContent);
  assert.match(title || '', /to a set with 14K Draft 1/, 'the bar says where: ' + title);
  await dismiss(); await clean('1');
  ok.push('1 · a draft sheet dragged over another draft sheet: that card is the place (Combine with 14K Sheet 1), planned and committed as a drop on it, the bar names it');

  // 2 · a sheet that is not ready: the card says why, in red, and the drop plans and writes nothing
  await page.evaluate(() => { window.__calls.length = 0; });
  await carry(sheetSel('dE01'), sheetSel('dG01'));
  s = await state('dG01'); assert.deepEqual({ state: s.state, hot: s.hot }, { state: 'dim', hot: true }); assert.match(s.label, /SS Draft 2 is not ready to join a set\. Its layout has not been verified yet\./, s.label);
  await page.mouse.up();
  await page.waitForSelector('.dndMovingWrap .dndLine.bad', { timeout: 6000 });
  assert.match(await page.textContent('.dndMovingWrap .dndLine.bad'), /not ready to join a set/);
  assert.equal(await page.evaluate(() => window.__calls.filter(c => c[0] === 'plan' || c[0] === 'commit').length), 0, 'a refused drop is not planned');
  await dismiss(); await clean('2');
  ok.push('2 · a drop on a sheet that cannot join: the reason is told in plain words on the card and in the bar, nothing is planned or written');

  // 3 · a sheet inside a real set card is not a place of its own (the set answers for it); the sections still answer elsewhere
  await page.evaluate(() => { window.__calls.length = 0; });
  await carry(sheetSel('dE01'), sheetSel('dA01'));
  assert.equal((await state('dA01')).state, null, 'a sheet of a set is not a drop place');
  assert.equal(await page.evaluate(() => window.__calls.filter(c => c[0] === 'sheetZone').length), 0, 'LibraryFlow is not even asked');
  await page.keyboard.press('Escape'); await page.mouse.up(); await dismiss(); await clean('3');
  ok.push('3 · a sheet inside a set card is the set\'s place, not a sheet place');

  // 4 · back over its own card and over the section: the section answers (it is already in In progress), as before
  await page.evaluate(() => { window.__calls.length = 0; });
  await carry(sheetSel('dE01'), '#libBody [data-laser-area="pending"] > h2');
  const ph = await progressHot(); assert.deepEqual(ph, { state: 'dim', hot: true }, 'the section answers: ' + JSON.stringify(ph));
  await page.keyboard.press('Escape'); await page.mouse.up(); await dismiss(); await clean('4');
  ok.push('4 · over the In progress section itself it answers as before (already there)');

  assert.deepEqual(errors, [], 'no errors on the page: ' + errors.join(' | '));
  await browser.close(); srv.close();
  console.log('PASS: ' + ok.length + ' scenarios\n' + ok.map(x => ' · ' + x).join('\n'));
})().catch(e => { console.error(e); process.exitCode = 1; setTimeout(() => process.exit(1), 500); });
