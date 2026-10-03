// Browser test of the Library's drag and drop (charm-nest-library-dnd.js) on the real Library page over the local stand-in
// for the site (bridge-server.cjs: the real charmNestLibrary function over an in-memory Firestore).
//
// LibraryFlow (charm-nest-flow.js, another worker's) and the three modules the drag layer calls (LibraryFx, LibraryApprovalUI,
// LibraryFlowRose) are FAKES with the exact shapes of plans/library-flow/contract.md, installed here and nowhere else. Three
// page contexts: A has no LibraryFlow at all (dragging is simply off); B has only the fake LibraryFlow (the drag layer's own
// plain flight and plain lines); C has the fake LibraryFlow and fakes of the other three modules (their exact call shapes).
//   node tests/charm-nest/library-dnd.cjs [playwright-core dir]      (SHOTS=<dir> also saves screenshots and flight frames)
const fs = require('fs'), path = require('path'), assert = require('assert'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
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
  { setId: 'set-t-1', seq: 1, ready: true, sheets: [['dA1', 'gold'], ['dA2', 'silver']] },
  { setId: 'set-t-2', seq: 2, ready: true, sheets: [['dB1', 'silver'], ['dB2', 'rose']] },
  { setId: 'set-t-3', seq: 3, ready: false, sheets: [['dC1', 'gold']] },
  { setId: 'set-t-4', seq: 4, ready: false, sheets: [['dD1', 'gold']] }
];
function seed(st, blobUrl) {
  const now = Date.now(), today = new Date(now).toISOString().slice(0, 10);
  let n = 0; const lines = {};
  for (const s of PLAN) {
    s.sheets.forEach(([id, metal], k) => {
      n++; const p = `charmnest/sets/${today}/Set-${s.seq}/${id}/preview.png`;
      st.blobs.set(p, { buf: preview(metal, n), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
      const orders = Array.from({ length: 3 + (n % 3) }, (_, i) => String(3700000000 + n * 10 + i));
      const rec = { id, metal, metalLabel: metal, day: today, setId: s.setId, setSeq: s.seq, sheetIndex: k + 1, fileBase: `${metal.slice(0, 2).toUpperCase()}_${today}_Set-${s.seq}_Sheet-${k + 1}`, folder: `${metal}_${id}`, orders, poolIds: orders.map(o => `${o}_1_1`), listings: orders.map((o, i) => String(1718000 + (i % 4))), placedCount: orders.length, charmCount: orders.length, density: 0.7, freePt2: 2000, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url: blobUrl(p) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: `run-${today}` };
      if (s.ready) {
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

/* ── the fakes (exact contract shapes), serialised into the page ── */
function fakeFlow(cfg) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  window.__calls = []; window.__rose = new Set(); window.__roseAdded = [];
  window.LibraryFlow = {
    targets(item) {
      window.__calls.push(['targets', item.kind, item.id]);
      const rows = (CN.S.library.rows || []), t = [{ area: 'progress' }, { area: 'laser' }, { area: 'completed' }];
      if (item.kind === 'sheet') { t.push({ newSet: true }); for (const s of new Set(rows.map(r => r.setId).filter(Boolean))) t.push({ set: s }); }
      return t;
    },
    async plan(q) {
      window.__calls.push(['plan', q.kind, q.id, JSON.stringify(q.to)]);
      await sleep(cfg.planMs);
      const rec = (CN.S.library.rows || []).find(r => r.id === q.id) || {};
      const base = { kind: q.kind, id: q.id, from: {}, to: Object.assign({ area: null, setId: null }, q.to.set ? { setId: q.to.set } : q.to), auto: [], needs: [], confirm: [], notes: [] };
      if (q.to.area === 'laser' && cfg.blocked.includes(q.id)) return Object.assign(base, { ok: false, needs: [{ key: 'backs', label: '2 back engravings not approved', detail: 'Approve them in the Engrave tab', items: [{ kind: 'order', id: '3700000010', label: 'Order 3700000010', why: 'back not approved' }] }] });
      if (q.to.area === 'laser' && rec.metal === 'rose' && !window.__rose.has(q.id)) return Object.assign(base, { ok: true, auto: [{ key: 'qr', label: 'QR label remade', detail: 'for the new layout' }], confirm: [{ key: 'roseLine', label: 'Add the green dash line to RG Sheet 2?', detail: 'This calculates the cut contour for these charms' }] });
      return Object.assign(base, { ok: true, auto: [{ key: 'qr', label: 'QR label remade', detail: 'for 4 orders' }, { key: 'layout', label: 'Layout verified' }, { key: 'membership', label: q.to.set ? 'Set membership changed' : 'Moved to ' + (q.to.area || 'a new set') }] });
    },
    async commit(plan, o) {
      window.__calls.push(['commit', plan.id, JSON.stringify(plan.to), JSON.stringify(o && o.confirmed), o && o.by]);
      await sleep(cfg.commitMs);
      if (cfg.failCommit) return { ok: false, applied: [], error: 'The cloud refused the write. Nothing was changed.' };
      if ((o.confirmed || []).includes('roseLine')) { window.__rose.add(plan.id); window.__roseAdded.push(plan.id); }
      if (plan.to.setId && cfg.fixtureMove) await window.__fixtureMove(plan.id, plan.to.setId);
      return { ok: true, applied: plan.auto.map(a => ({ key: a.key, label: a.label })) };
    },
    async approve() { return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; }
  };
}
function fakeModules() {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  window.__fx = []; window.__ui = []; window.__roseUi = [];
  window.LibraryFx = {
    async fly(from, to, o) { window.__fx.push(['fly', from && from.className, to && to.className, o && o.kind, o && o.duration, typeof (o && o.onDone)]); await sleep(300); },
    async flyBack(from, to, o) { window.__fx.push(['flyBack', from && from.className, to && to.className, o && o.kind]); await sleep(300); }
  };
  window.LibraryApprovalUI = {
    show(host, plan, o) {
      window.__ui.push(['show', host.className, plan.ok, plan.confirm.length, plan.needs.length, typeof o.onConfirm, typeof o.onCancel, o.title]);
      host.innerHTML = `<div class="fakeUI">${plan.auto.map(a => `<p class="fAuto">${a.label}</p>`).join('')}${plan.needs.map(a => `<p class="fNeed">${a.label}</p>`).join('')}${plan.confirm.map(c => `<button class="fOk" data-key="${c.key}">${c.label}</button>`).join('')}${plan.confirm.length ? '<button class="fNo">Not now</button><button class="fOkNoKeys">confirm without keys</button>' : ''}</div>`;
      host.querySelectorAll('.fOk').forEach(b => { b.onclick = () => o.onConfirm([b.dataset.key]); });
      const no = host.querySelector('.fNo'); if (no) no.onclick = () => o.onCancel();
      const nk = host.querySelector('.fOkNoKeys'); if (nk) nk.onclick = () => o.onConfirm();
    },
    update(host, res) { window.__ui.push(['update', res.ok, (res.applied || []).length]); host.insertAdjacentHTML('beforeend', `<p class="fDone">${res.ok ? 'done' : 'error'}</p>`); },
    hide(host) { window.__ui.push(['hide']); host.innerHTML = ''; }
  };
  window.LibraryFlowRose = { confirmBar() { window.__roseUi.push(['confirmBar']); }, check() { return Promise.resolve({ needsLine: false, sheets: [], confirm: null }); }, calculate() { return Promise.resolve({ ok: true }); } };
}

const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what + ' ' + fn); await new Promise(r => setTimeout(r, 60)); } };

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  seed(st, srv.blobUrl);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const ok = [];
  const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name + '.png') }); };

  async function open(o = {}) {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 1700 }, reducedMotion: o.reduced ? 'reduce' : 'no-preference' });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
    await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => { errors.push('page: ' + e.message); if (process.env.DEBUG) console.log('PAGEERROR', e.message, (e.stack || '').split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
    await page.exposeFunction('__fixtureMove', (id, to) => moveSheet(st, id, to));
    if (o.flow) {
      const cfg = Object.assign({ planMs: 350, commitMs: 350, blocked: ['dC1'], fixtureMove: true, failCommit: false }, o.cfg || {});
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
    const b = typeof to === 'string' ? await box(page, to) : to;
    await page.mouse.move(b.x, b.y, { steps: o.steps || 12 });
    return b;
  }
  const KEYS = { 'In progress': 'area:progress', 'Laser cutting': 'area:laser', Completed: 'area:completed', 'New set': 'newSet' };
  const chipSel = name => `.dndChip[data-key="${KEYS[name] || 'set:set-t-' + name.replace('Set ', '')}"]`;

  /* ═════ A · no LibraryFlow on the page: dragging is simply off ═════ */
  {
    const { ctx, page, errors } = await open({ flow: false });
    assert.equal(await page.evaluate(() => !!window.LibraryFlow), false);
    assert.equal(await page.$$eval('.dndGrip', g => g.length), 0, 'no grip when LibraryFlow is absent');
    const a = await box(page, sheetSel('dA1'));
    await page.mouse.move(a.l + 24, a.t + 14); await page.mouse.down(); const h2 = await box(page, '#libBody .laserSection h2'); await page.mouse.move(h2.x + 300, h2.y, { steps: 8 });
    assert.equal(await page.$$eval('.dndDock, .dndLift, .dndSource', n => n.length), 0, 'no dock, no lifted copy, no outline');
    await page.mouse.up(); await page.waitForTimeout(300);
    assert.equal(await page.evaluate(() => !!document.querySelector('dialog[open]')), false, 'dragging with LibraryFlow absent opened nothing');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    ok.push('A · without LibraryFlow: no grip, no dock, no lifted copy, a drag does nothing');
    await ctx.close();
  }

  /* ═════ B · the fake LibraryFlow only: the drag layer's own flight and plain lines ═════ */
  {
    const { ctx, page, errors } = await open({ flow: true });
    // grips on every sheet card and every set card, none on a card of the lifted copy
    assert.equal(await page.$$eval('#libBody .libCard .dndGrip', g => g.length), 6, 'a grip on each of the six sheet cards');
    assert.equal(await page.$$eval('#libBody .setCard > .sh .dndGrip', g => g.length), 4, 'a grip on each of the four set cards');
    await shot(page, '00-library-with-grips');

    // 1 · a sheet picked up: the dock, the places, the lifted copy, the faint original
    const before = await area(page, 'dA2');
    assert.equal(before, 'laser', 'dA2 starts in Laser cutting');
    assert.equal(await area(page, 'dC1'), 'progress', 'dC1 starts in progress');
    const hot = await carry(page, sheetSel('dC1'), chipSel('Laser cutting'));
    const dock = await page.evaluate(() => {
      const d = document.querySelector('.dndDock'), s = document.querySelector('#stage').getBoundingClientRect(), r = d.getBoundingClientRect();
      return { top: r.top, bottom: r.bottom, stageTop: s.top, chips: [...d.querySelectorAll('.dndChip')].map(c => [c.querySelector('.dndChipName').textContent, c.dataset.state]), lift: !!document.querySelector('.dndLift'), liftImg: !!document.querySelector('.dndLift img.pv'), source: document.querySelector('.dndSource') && document.querySelector('.dndSource').className, dialogs: document.querySelectorAll('dialog[open]').length, innerHeight };
    });
    assert(dock.top >= dock.stageTop && dock.bottom < dock.stageTop + 220, 'the dock sits under the top bar, at the top of the Library: ' + JSON.stringify(dock));
    const names = dock.chips.map(c => c[0]);
    for (const n of ['In progress', 'Laser cutting', 'Completed', 'New set', 'Set 1', 'Set 2', 'Set 3', 'Set 4']) assert(names.some(x => x.startsWith(n)), 'a place named ' + n + ': ' + names);
    const state = n => dock.chips.find(c => c[0].startsWith(n))[1];
    assert.equal(state('Laser cutting'), 'armed'); assert.equal(state('In progress'), 'dim', 'its own area is dimmed'); assert.equal(state('Set 3'), 'dim', 'its own set is dimmed'); assert.equal(state('Set 1'), 'armed');
    assert(dock.lift && dock.liftImg, 'a lifted copy with the real preview follows the pointer'); assert(/librarySheet/.test(dock.source), 'the real card stays as an outline');
    assert.equal(dock.dialogs, 0, 'no pop-up');
    assert.equal(await page.$eval('.dndSource', e => +getComputedStyle(e).opacity < 0.6), true, 'the original is faint');
    // the place lights up where it stands, and says its name
    const lit = await page.evaluate(() => [...document.querySelectorAll('[data-dnd-state]')].map(e => [e.dataset.dndState, e.dataset.dndLabel, e.hasAttribute('data-dnd-hot')]));
    assert(lit.some(l => l[1].startsWith('Move to Set 1') && l[0] === 'armed'), 'the set cards show their names: ' + JSON.stringify(lit));
    assert(lit.some(l => /Laser cutting/.test(l[1])), 'the sections show their names');
    // a dimmed place says why while the card is over it
    const dim = await box(page, chipSel('In progress')); await page.mouse.move(dim.x, dim.y, { steps: 6 });
    assert.match(await page.textContent(`${chipSel('In progress')} .dndChipSub`), /Already in In progress/);
    await shot(page, '01-dragging-dock-illegal-reason');
    await page.mouse.move(hot.x, hot.y, { steps: 6 });
    assert.equal(await page.$eval(chipSel('Laser cutting'), c => c.hasAttribute('data-hot')), true);
    await shot(page, '02-dragging-over-laser-cutting');
    // let go over nothing (the middle of the page): the copy settles back, nothing is asked of LibraryFlow
    await page.mouse.move(700, 700, { steps: 5 });
    await page.mouse.up(); await page.waitForTimeout(700);
    assert.equal(await page.$$eval('.dndDock, .dndLift, .dndSource, [data-dnd-state]', n => n.length), 0, 'a card let go over nothing settles back and everything is put away');
    assert.deepEqual(await page.evaluate(() => __calls.filter(c => c[0] === 'plan' || c[0] === 'commit')), [], 'no plan, no commit for a drop over nothing');
    ok.push('B · a sheet held: dock with each place named, own area and set dimmed with the reason, in-place sections and set cards named, real preview in the lifted copy, faint original, no pop-up; let go over nothing: nothing asked');

    // 2 · a sheet dropped on a set card that is not ready: flies there, plan ok, commit, the lists show it in its new set
    await carry(page, sheetSel('dA2'), setSel('dC1'));
    assert.equal(await page.$eval(setSel('dC1'), e => e.hasAttribute('data-dnd-hot') && e.dataset.dndState), 'armed');
    await shot(page, '03-over-set-3');
    await page.mouse.up();
    // the bar is on the set card itself, with a labelled spinner while the plan is asked for
    await page.waitForSelector('#libBody .dndMovingWrap .dndWait:not([hidden])', { timeout: 3000 });
    assert.match(await page.textContent('#libBody .dndMovingWrap .dndWait'), /Checking the move/);
    assert.equal(await page.evaluate(() => !!document.querySelector('.setCard .dndMovingWrap') && !document.querySelector('.dndDock'), null), true, 'the bar stands on the target set card, the dock is put away');
    await page.waitForTimeout(160); await shot(page, '04-flight-1');
    await page.waitForTimeout(180); await shot(page, '05-flight-2');
    // the green lines, one after another
    await page.waitForSelector('#libBody .dndLine.ok', { timeout: 4000 });
    await page.waitForTimeout(900); await shot(page, '06-moving-bar-auto-lines');
    const greens = await page.$$eval('#libBody .dndLine.ok b', b => b.map(x => x.textContent));
    assert.deepEqual(greens.slice(0, 3), ['QR label remade', 'Layout verified', 'Set membership changed'], 'the green lines');
    // the write began before the flight ended (the flight is 720 ms; the commit is asked for the moment the plan is ok)
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit')), 4000, 'commit');
    const commit = await page.evaluate(() => __calls.filter(c => c[0] === 'commit'));
    assert.deepEqual(commit[0].slice(1, 4), ['dA2', '{"set":"set-t-3"}', '[]'], 'commit with nothing confirmed: ' + commit);
    assert.equal(commit[0][4], 'Tester', 'by the signed-in person');
    await until(() => setOfCard(page, 'dA2').then(s => s === 'set-t-3'), 12000, 'dA2 listed under Set 3');
    assert.equal(st.doc(SHEETS, 'dA2').setId, 'set-t-3');
    ok.push('B · a sheet dropped on a set card: the bar stands on that card with a labelled spinner, green lines "QR label remade / Layout verified / Set membership changed", commit asks nothing confirmed and is by the signed-in person, the Library lists it in its new set');
    await page.click('#libBody .dndMovingWrap .dndX').catch(() => {});
    await until(() => page.evaluate(() => !document.querySelector('.dndMovingWrap')), 6000, 'bar put away');

    // 3 · a blocked target: red lines, the copy flies back, nothing changed, commit never called
    const callsBefore = await page.evaluate(() => __calls.filter(c => c[0] === 'commit').length);
    await carry(page, sheetSel('dC1'), chipSel('Laser cutting'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .dndLine.bad', { timeout: 5000 });
    const head = await page.textContent('.dndDock .dndHead.bad');
    assert.match(head, /Cannot move to Laser cutting yet/);
    assert.match(await page.textContent('.dndDock .dndLine.bad'), /2 back engravings not approved/);
    await shot(page, '07-blocked-needs');
    // …then it flies back (a copy on the motion layer travels home), and the real card is as it was
    await until(() => page.evaluate(() => document.querySelectorAll('.dndLift').length > 0 && document.querySelector('.dndDock .dndLine.note')), 9000, 'flying back after the red lines');
    await page.waitForTimeout(300); await shot(page, '08-flying-back');
    await until(() => page.evaluate(() => !document.querySelector('.dndLift') && !document.querySelector('.dndSource')), 8000, 'back home');
    assert.equal(await area(page, 'dC1'), 'progress', 'nothing changed: still in progress');
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit').length), callsBefore, 'a blocked move never reaches commit');
    assert.equal(st.doc(SHEETS, 'dC1').setId, 'set-t-3');
    await page.click('.dndDock .dndX').catch(() => {});
    await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 8000, 'dock put away');
    ok.push('B · a blocked target: "Cannot move to Laser cutting yet", the red "2 back engravings not approved" line, the copy flies back, the card is as it was, commit is never called');

    // 4 · a dimmed place dropped on: the reason on the place, the copy settles back, nothing asked
    const plansBefore = await page.evaluate(() => __calls.filter(c => c[0] === 'plan').length);
    await carry(page, sheetSel('dC1'), chipSel('In progress'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .dndLine.bad b', { timeout: 3000 });
    assert.match(await page.textContent('.dndDock .dndLine.bad b'), /Already in In progress/);
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'plan').length), plansBefore, 'a dropped-on dimmed place is not planned');
    await page.click('.dndDock .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 8000, 'dock gone');
    ok.push('B · a drop on a dimmed place: the reason is told in the bar and nothing is planned or written');

    // 5 · Rose Gold: the drop alone adds nothing; only the button does
    const rose = 'dB2';
    assert.equal(await area(page, rose), 'laser', 'the rose sheet starts in Laser cutting');
    await carry(page, sheetSel(rose), chipSel('In progress'));
    await page.mouse.up();                                       // (a move into In progress: not a laser move, no confirm)
    await page.waitForSelector('.dndDock .dndLine.ok', { timeout: 6000 });
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit' && c[1] === 'dB2')), 5000, 'in-progress commit');
    await page.click('.dndDock .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 9000, 'dock gone');
    await page.evaluate(() => { __calls.length = 0; });
    // into Laser cutting from wherever it is now, by a drop on the Laser cutting section itself
    await until(() => page.evaluate(() => !!document.querySelector('#libBody .libCard[data-id="dB2"]')), 8000, 'rose card listed');
    const secArea = await area(page, rose);
    const toLaser = secArea === 'laser' ? 'In progress' : 'Laser cutting';
    void toLaser;
    // (the sheet's readiness keeps it where the laser section holds it; drop it on the Laser cutting chip: the plan asks for the line)
    await carry(page, sheetSel(rose), chipSel('Laser cutting'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .dndLine.warn', { timeout: 6000 });
    await page.waitForTimeout(900);
    const sentence = await page.textContent('.dndDock .dndLine.warn');
    assert.match(sentence, /This sheet has no green dash line yet\.\s*Moving it into Laser cutting needs one\./);
    assert.deepEqual(await page.$$eval('.dndDock .dndLine.warn .dndBtn, .dndDock .dndActs .dndBtn', b => b.map(x => x.textContent)), ['Add the green dash line', 'Not now']);
    await shot(page, '09-rose-confirm-bar');
    // nothing is added by the drop, however long it waits
    await page.waitForTimeout(1500);
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit').length), 0, 'the drop alone never commits a Rose Gold sheet');
    assert.deepEqual(await page.evaluate(() => __roseAdded), [], 'no green dash line added by the drop');
    // "Not now": the card flies back, still nothing added
    await page.click('.dndDock .dndBtn[data-cancel]');
    await until(() => page.evaluate(() => !document.querySelector('.dndLift') && !document.querySelector('.dndSource')), 9000, 'back home after Not now');
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit').length), 0, 'Not now commits nothing');
    assert.deepEqual(await page.evaluate(() => __roseAdded), []);
    await page.click('.dndDock .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 9000, 'dock gone');
    // again, and this time the button: only then is roseLine passed in `confirmed`
    await carry(page, sheetSel(rose), chipSel('Laser cutting'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .dndBtn[data-confirm="roseLine"]', { timeout: 6000 });
    await page.waitForTimeout(1000);
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit').length), 0, 'still nothing before the press');
    await page.click('.dndDock .dndBtn[data-confirm="roseLine"]');
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit')), 5000, 'commit after the press');
    const rc = await page.evaluate(() => __calls.filter(c => c[0] === 'commit')[0]);
    assert.deepEqual(rc.slice(1, 4), ['dB2', '{"area":"laser"}', '["roseLine"]'], 'roseLine passed in confirmed only after the press: ' + rc);
    assert.deepEqual(await page.evaluate(() => __roseAdded), ['dB2'], 'the line is added once, by the press');
    await page.click('.dndDock .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 9000, 'dock gone');
    ok.push('B · Rose Gold: the drop shows "This sheet has no green dash line yet. Moving it into Laser cutting needs one." with [Add the green dash line] [Not now]; waiting or Not now commits nothing and adds no line; only the press passes roseLine to commit');

    // 6 · the keyboard: the grip, Enter, Move to…, arrows, Enter
    await page.evaluate(() => { __calls.length = 0; });
    await page.evaluate(() => document.querySelector('#libBody .libCard[data-id="dD1"] .dndGrip').focus());
    await page.keyboard.press('Enter');
    await page.waitForSelector('#libBody .dndMenu .dndMenuItem', { timeout: 4000 });
    const menu = await page.$$eval('#libBody .dndMenu .dndMenuItem', m => m.map(x => [x.firstChild.textContent, x.getAttribute('aria-disabled') === 'true', x.lastChild.textContent]));
    const mnames = menu.map(m => m[0]);
    for (const n of ['In progress', 'Laser cutting', 'Completed', 'New set', 'Set 1', 'Set 2', 'Set 3', 'Set 4']) assert(mnames.some(x => x.startsWith(n)), 'Move to… lists ' + n + ': ' + mnames);
    assert(menu.find(m => m[0] === 'In progress')[1] && /Already/.test(menu.find(m => m[0] === 'In progress')[2]), 'a place that is not allowed is listed, dimmed, with the reason: ' + JSON.stringify(menu));
    await shot(page, '10-move-to-menu');
    assert.equal(await page.evaluate(() => document.activeElement.className + ' ' + document.activeElement.textContent.slice(0, 20)), 'dndMenuItem Laser cutting ready to cut'.replace('ready to cut', 'ready to cut'), 'focus moves to the first allowed place');
    await page.keyboard.press('ArrowDown');   // Completed
    await page.keyboard.press('ArrowUp');     // back to Laser cutting
    await page.keyboard.press('Escape');
    assert.equal(await page.evaluate(() => document.activeElement.className.includes('dndGrip')), true, 'Escape closes the menu and returns to the grip');
    await until(() => page.evaluate(() => !document.querySelector('#libBody .dndMenuWrap')), 3000, 'menu closed');
    await page.keyboard.press('Enter'); await page.waitForSelector('#libBody .dndMenu .dndMenuItem');
    await page.evaluate(() => { document.querySelector('#libBody .dndMenu .dndMenuItem[data-key="set:set-t-1"]').focus(); });
    await page.keyboard.press('Enter');
    await page.waitForSelector('#libBody .dndMovingWrap', { timeout: 4000 });
    await page.waitForSelector('#libBody .dndLine.ok', { timeout: 5000 });
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit' && c[1] === 'dD1' && c[2] === '{"set":"set-t-1"}')), 5000, 'keyboard commit');
    await until(() => setOfCard(page, 'dD1').then(s => s === 'set-t-1'), 12000, 'dD1 listed under Set 1');
    assert.equal(await page.evaluate(() => document.activeElement.className.includes('dndGrip') && !!document.activeElement.closest('.libCard[data-id="dD1"]')), true, 'focus returns to the card where it now stands');
    ok.push('B · keyboard: the grip opens "Move to…" with the same places (the not-allowed ones dimmed with the reason), arrows and Escape work, Enter on a set moves the sheet by the same plan and commit, and focus lands on the card in its new set');
    await page.click('#libBody .dndMovingWrap .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndMovingWrap')), 6000, 'bar gone');

    // 7 · a whole set picked up: it can go to the areas, never into another set
    await page.evaluate(() => { __calls.length = 0; });
    const setHead = '#libBody .setCard:has(.libCard[data-id="dB1"]) > .sh .nm';
    const sb = await box(page, setHead);
    await page.mouse.move(sb.l + 6, sb.t + 4); await page.mouse.down(); await page.mouse.move(sb.l + 26, sb.t + 22, { steps: 4 });
    await page.waitForSelector('.dndDock .dndChip');
    const setDock = await page.$$eval('.dndDock .dndChip', c => c.map(x => [x.querySelector('.dndChipName').textContent, x.dataset.state]));
    assert(setDock.filter(c => /^Set \d/.test(c[0])).every(c => c[1] === 'dim'), 'a set cannot be dropped into a set: ' + JSON.stringify(setDock));
    assert.equal(await page.$eval('.dndLift', e => e.querySelectorAll('.libCard').length), 2, 'the copy of a set carries its sheets');
    await shot(page, '11-set-held');
    await page.keyboard.press('Escape'); await page.mouse.up();
    await until(() => page.evaluate(() => !document.querySelector('.dndLift') && !document.querySelector('.dndDock')), 3000, 'Escape put it back');
    ok.push('B · a set card held: its copy carries its sheets, the sets are dimmed (a set never goes into a set), Escape puts it back');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  /* ═════ C · the fake LibraryFlow and the fakes of LibraryFx, LibraryApprovalUI, LibraryFlowRose ═════ */
  {
    const { ctx, page, errors } = await open({ flow: true, modules: true, cfg: { blocked: ['dC1'] } });
    // ok move: the flight is LibraryFx.fly(copy, place, { kind, duration, onDone }), the bar is LibraryApprovalUI.show/update/hide
    await carry(page, sheetSel('dA1'), chipSel('Set 4'));
    await page.mouse.up();
    await page.waitForSelector('#libBody .setCard .fakeUI .fAuto', { timeout: 5000 });
    await until(() => page.evaluate(() => __ui.some(u => u[0] === 'update')), 8000, 'update');
    const fx = await page.evaluate(() => __fx.filter(f => f[0] === 'fly')[0]), ui = await page.evaluate(() => __ui.filter(u => u[0] === 'show')[0]);
    assert.deepEqual([fx[0], fx[1], fx[3], fx[4], fx[5]], ['fly', 'dndLift', 'sheet', 720, 'function'], 'LibraryFx.fly(copy, place, {kind, duration, onDone}): ' + fx);
    assert.deepEqual([ui[0], ui[1], ui[2], ui[3], ui[4], ui[5], ui[6]], ['show', 'dndApprove', true, 0, 0, 'function', 'function'], 'LibraryApprovalUI.show(host, plan, {onConfirm, onCancel, title}): ' + ui);
    assert.match(ui[7], /Moving .*Sheet 1 to Set 4/);
    assert.equal(await page.evaluate(() => __ui.filter(u => u[0] === 'update')[0].join()), 'update,true,3', 'update(host, result)');
    await until(() => page.evaluate(() => __ui.some(u => u[0] === 'hide')), 9000, 'hide');
    ok.push('C · with the other modules: LibraryFx.fly(copy, place, {kind, duration: 720, onDone}), LibraryApprovalUI.show(host, plan, {onConfirm, onCancel, title}), update(host, result) after commit and hide(host) at the end');
    await until(() => page.evaluate(() => !document.querySelector('.dndMovingWrap')), 6000, 'bar gone');
    // blocked: LibraryApprovalUI shows the needs, LibraryFx.flyBack brings the copy home
    await carry(page, sheetSel('dC1'), chipSel('Laser cutting'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .fNeed', { timeout: 5000 });
    await until(() => page.evaluate(() => __fx.some(f => f[0] === 'flyBack')), 12000, 'flyBack');
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit' && c[1] === 'dC1').length), 0);
    ok.push('C · a blocked move: LibraryApprovalUI shows the needs, LibraryFx.flyBack brings the copy home, commit is never called');
    await page.click('.dndDock .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 9000, 'dock gone');
    // Rose Gold through LibraryApprovalUI: the UI is told to confirm; confirming without keys never commits; only its own key does
    await carry(page, sheetSel('dB2'), chipSel('Laser cutting'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .fOk[data-key="roseLine"]', { timeout: 6000 });
    await page.waitForTimeout(1200);
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit' && c[1] === 'dB2').length), 0, 'nothing is committed before a press');
    assert.equal(await page.evaluate(() => __roseUi.length), 0, 'LibraryFlowRose.confirmBar is only the fallback for a missing LibraryApprovalUI');
    await page.click('.dndDock .fOkNoKeys');
    await until(() => page.evaluate(() => !document.querySelector('.dndLift') && !document.querySelector('.dndSource')), 12000, 'back home');
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit' && c[1] === 'dB2').length), 0, 'a confirm that names no key commits nothing');
    assert.deepEqual(await page.evaluate(() => __roseAdded), []);
    ok.push('C · Rose Gold: LibraryApprovalUI.onConfirm() with no key is never taken as yes; nothing is committed and no line is added');
    await page.click('.dndDock .dndX').catch(() => {}); await until(() => page.evaluate(() => !document.querySelector('.dndDock')), 9000, 'dock gone');
    await carry(page, sheetSel('dB2'), chipSel('Laser cutting'));
    await page.mouse.up();
    await page.waitForSelector('.dndDock .fOk[data-key="roseLine"]', { timeout: 6000 });
    await page.click('.dndDock .fOk[data-key="roseLine"]');
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit' && c[1] === 'dB2')), 5000, 'commit');
    assert.equal(await page.evaluate(() => __calls.filter(c => c[0] === 'commit')[0][3]), '["roseLine"]');
    ok.push('C · Rose Gold: its own key, pressed in LibraryApprovalUI, is what commit receives');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  /* ═════ D · a commit that fails changes nothing; reduced motion ═════ */
  {
    const { ctx, page, errors } = await open({ flow: true, cfg: { failCommit: true } });
    await carry(page, sheetSel('dA1'), chipSel('Set 4'));
    await page.mouse.up();
    await page.waitForSelector('.setCard .dndLine.bad', { timeout: 6000 });
    assert.match(await page.textContent('.setCard .dndLine.bad'), /did not go through[\s\S]*cloud refused the write/);
    await until(() => page.evaluate(() => !document.querySelector('.dndLift') && !document.querySelector('.dndSource')), 9000, 'back home');
    assert.equal(await setOfCard(page, 'dA1'), 'set-t-1', 'a failed commit leaves the sheet where it was');
    ok.push('D · a commit that fails: the error is shown in red, the card flies back, the sheet is where it was');
    await page.click('.setCard .dndX').catch(() => {}); await ctx.close();
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
  }
  {
    const { ctx, page, errors } = await open({ flow: true, reduced: true });
    assert.equal(await page.evaluate(() => matchMedia('(prefers-reduced-motion: reduce)').matches), true);
    await carry(page, sheetSel('dA1'), chipSel('Set 4'));
    await page.mouse.up();
    // no transform animation on the copy: it only fades out, and the lines are there at once
    const anims = await page.evaluate(() => document.getAnimations().filter(a => a.effect && a.effect.target && a.effect.target.classList && a.effect.target.classList.contains('dndLift')).map(a => a.effect.getKeyframes().some(k => k.transform && k.transform !== 'none')));
    assert(!anims.some(Boolean), 'reduced motion: the copy does not fly: ' + anims);
    await page.waitForSelector('#libBody .dndLine.ok', { timeout: 6000 });
    assert.equal(await page.$eval('#libBody .dndLine.ok', l => getComputedStyle(l).opacity), '1', 'the green lines are shown at once');
    await until(() => page.evaluate(() => !document.querySelector('.dndLift')), 2000, 'copy gone fast');
    await until(() => page.evaluate(() => __calls.some(c => c[0] === 'commit')), 5000, 'commit still happens');
    await until(() => setOfCard(page, 'dA1').then(s => s === 'set-t-4'), 12000, 'dA1 listed under Set 4');
    ok.push('D · reduced motion: no flight (the copy fades out), the lines are shown at once, the write and the list refresh are the same');
    assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
    await ctx.close();
  }

  await browser.close(); srv.close && srv.close();
  console.log(ok.map(x => '✓ ' + x).join('\n'));
  console.log(`\n${ok.length} groups passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
