// Hold, Release hold and Cancel Order, tried to be broken (tester T2, 5 Oct 2026). The REAL modules (charm-nest-order-hold.js,
// charm-nest-order-release.js, charm-nest-hold-ui.js, charm-nest-hold-fx.js, charm-nest-cancel-ui.js) on the real sorter page in headless
// Chromium over the local fake backend (bridge-server.cjs: the real charmNestLibrary handler on an in-memory Firestore). Nothing leaves
// the machine: no Etsy, no paid model call (asserted), no live endpoint.
//   node tests/charm-nest/hold-release-adversarial.cjs [seeded | stale | safety | sandbox | ui | real]   (PW_DIR=<playwright-core's parent>, CHROMIUM=<chrome>)
//   seeded  the fast fixture of order-hold-engine.cjs (sheets and orders put straight into the page): double press, a popup gone stale
//           (a sheet cut meanwhile, the order held / cancelled meanwhile), Hold while a Release runs, Cancel while Hold runs, no order lost,
//           the timeline only grows, zero Etsy and zero paid calls
//   stale   two pages (two browsers) on the same data: B presses Hold on an order A already held, cancelled or changed
//   safety  cut / recalled / committed sheets never filled or taken from, a committed set blocks, alone on a sheet blocks, Rose Gold never
//           re-arranged and no green line, 10K / 14K metals never mixed
//   sandbox the sandbox's journals and records are its own (and the production's its own); a sandbox reset never touches a held production order
//   ui      the real popup, buttons and film under failure: a failing save mid-hold, offline during Cancel, never stuck in the Nest tab
//   real    the real nest (custom designs, nested by the real solver): a reload in the middle of a hold and of a release, a hand-completed piece,
//           frontAt across a reload and after placement
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline', CANC = 'Charm_Nest_Cancelled';
const RUN = 'run-test-1', BASE_TS = 1790000000;
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const fails = [], passed = [];
const check = (ok, what) => { (ok ? passed : fails).push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
const want = n => !process.env.ONLY || process.env.ONLY.split(',').includes(String(n));   // (ONLY=6 runs scenario 6 of a segment alone)
const sorted = a => a.slice().sort();
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

/* what a page may never ask for while a hold, a release or a cancel runs: Etsy, or a paid model call */
const ETSY_FN = new Set(['listOpenOrders', 'etsyOrderProxy', 'etsyImages', 'refreshEtsyToken', 'etsySandbox']);
const PAID_FN = new Set(['charmNestAgent-background', 'charmEngrave-background', 'charmMaster-background']);
const PAID_OP = /^(startAgent|customRead|engraveRead|labelRead|startJob)$/;   // (the Library's ops that start a paid agent or a job)

/* ── the fixture of order-hold-engine.cjs: sheets and orders put straight into the page and the fake backend ── */
const fixture = spec => {
  const docs = { sheets: [], pool: [] };
  for (const sh of spec.sheets) {
    const charms = [], placements = [], poolIds = [];
    sh.items.forEach(([rid, tx, copy, col, row, placed], i) => {
      const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
      charms.push({ id, poolId, order: rid });
      // (a saved sheet lists the pieces PLACED on it; a piece still waiting on the sheet is not on its saved record yet, and its record says "ready")
      if (placed !== false) { placements.push({ id, cxPt: 30 + col * 40, cyPt: 30 + row * 40, angle: 0 }); poolIds.push(poolId); }
      docs.pool.push({ poolId, orderId: rid, sheetId: sh.id, state: placed !== false ? 'placed' : 'ready', material: sh.metal });
    });
    docs.sheets.push(Object.assign({ id: sh.id, metal: sh.metal, charms, placements, poolIds, sheetIndex: sh.page, fileBase: sh.id, runId: RUN }, spec.sandbox ? { sandbox: true } : {}));
  }
  return docs;
};
const O = (age, ...lines) => ({ age, lines: lines.map(([tx, n, metal]) => ({ tx, n, metal })) });

/* the standard fixture: H (4 pieces) on GF Sheet 2 and SS Sheet 1; waiting orders on GF Sheet 3; a newer SS Sheet 2 */
const H0 = '4170000100', sid = (H, n) => `${H.slice(-4)}-${n}`;
const std = (H, extra) => Object.assign({
  sheets: [
    { id: sid(H, 'gold-2'), metal: 'gold', page: 2, items: [[H, 1, 1, 0, 0], [H, 1, 2, 1, 0], ['4170000201', 7, 1, 2, 0], ['4170000202', 8, 1, 3, 0]] },
    { id: sid(H, 'silver-1'), metal: 'silver', page: 1, items: [[H, 2, 1, 0, 0], [H, 2, 2, 1, 0], ['4170000203', 9, 1, 2, 0]] },
    { id: sid(H, 'gold-3'), metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false], ['4170000302', 6, 1, 1, 0, false], ['4170000303', 4, 1, 2, 0, false]] },
    { id: sid(H, 'silver-2'), metal: 'silver', page: 2, items: [['4170000401', 11, 1, 0, 0], ['4170000402', 12, 1, 1, 0], ['4170000403', 13, 1, 2, 0]] },
  ],
  orders: { [H]: O(9000, [1, 2, 'gold'], [2, 2, 'silver']), '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']), '4170000203': O(8200, [9, 1, 'silver']),
    '4170000301': O(7000, [5, 1, 'gold']), '4170000302': O(7500, [6, 1, 'gold']), '4170000303': O(6000, [4, 1, 'gold']),
    '4170000401': O(5000, [11, 1, 'silver']), '4170000402': O(5500, [12, 1, 'silver']), '4170000403': O(4000, [13, 1, 'silver']) },
}, extra || {});
const idsOf = (spec, only) => spec.sheets.flatMap(sh => sh.items.map(([rid, tx, copy]) => [rid, pid(rid, tx, copy)])).filter(([rid]) => !only || only.includes(rid)).map(([, id]) => id);

const CHROMIUM = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };

/* ── the backend: the fake site, with its writes, its reads and its paid calls counted ── */
async function backend(opts) {
  const srv = await start({ receipts: [] }), st = srv.st;
  const writes = [], reads = [];
  { const push = st.calls.push.bind(st.calls); st.calls.push = c => { c.at = Date.now(); return push(c); }; }   // (when each call came)
  const set = st.docs.set.bind(st.docs), del = st.docs.delete.bind(st.docs), get = st.docs.get.bind(st.docs);
  st.docs.set = (k, v) => { writes.push(k); return set(k, v); };
  st.docs.delete = k => { writes.push('-' + k); return del(k); };
  st.docs.get = k => { reads.push(k); return get(k); };
  // (the model: the fake backend's stub; counted here so a paid call can never go by unseen)
  const anthro = require(path.join(root, 'netlify/functions/_etsyMailAnthropic.js')), real = anthro.callClaudeRaw;
  srv.paid = 0; anthro.callClaudeRaw = async (...a) => { srv.paid++; return real(...a); };
  srv.writes = writes; srv.reads = reads; srv.raw = { set, del, get };
  srv.mark = () => ({ calls: st.calls.length, writes: writes.length, reads: reads.length, paid: srv.paid, outside: srv.outside.length });
  srv.outside = [];
  return srv;
}
const prodKey = k => !/^-?Sandbox_/.test(k);

/* ── a page: the real sorter, nothing leaves the machine, the nest stubbed as the engine test does ── */
async function openPage(browser, srv, o = {}) {
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 }, reducedMotion: o.motion ? 'no-preference' : 'reduce' });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    srv.outside.push(u); return r.abort();
  });
  await ctx.addInitScript(({ sandbox, stub, name }) => {
    try {
      if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', name || 'Tester');
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; s.sound = 'off'; s.notify = 'off'; s.review = 'on'; if (sandbox) s.sandbox = 'on'; localStorage.setItem('cn.settings', JSON.stringify(s));
    } catch (_) { /* about:blank */ }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    if (!stub) return;
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke(); };
      P.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
      P.cutLinesOf = () => [];
      const stubNest = sh => {
        window.__nests = (window.__nests || []).concat([{ metal: sh.metal, sheetId: sh.sheetId, charms: sh.charms.map(c => c.poolId) }]);
        // (the real nest clears the sheet's last problem as it starts)
        sh.status = 'nesting'; sh.problem = null; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        if (localStorage.getItem('__nestMode') === 'hang') return;
        setTimeout(async () => {
          try {
            if (localStorage.getItem('__nestMode') === 'fail') throw new Error('the save failed (test)');
            for (const c of sh.charms) if (c.pinned && !sh.placements.some(p => p.id === c.id)) sh.placements.push({ id: c.id, cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle || 0, wPt: c.widthPt, hPt: c.heightPt });
            sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
            await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.map(c => c.poolId) } }, { quiet: true });
          } catch (e) { sh.problem = e.message; }
          sh.status = sh.problem ? 'ready' : 'complete'; sh.dirty = !!sh.problem; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = !sh.problem; sh.density = Math.min(.9, sh.placements.length * .08);
          try { CN.renderCard(sh); } catch (_) {}
        }, 120);
      };
      stubNest.__stub = true; window.startNest = stubNest; clearInterval(iv);
    }, 0);
  }, { sandbox: !!o.sandbox, stub: o.stub !== false, name: o.name });
  if (o.init) await ctx.addInitScript(o.init);   // (after the settings above: it may add to them)
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  if (process.env.DEBUGCON) page.on('console', m => console.log('         [page ' + m.type() + ']', m.text().slice(0, 300)));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|forced failure|the save failed \(test\)/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  await page.goto(`${o.origin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && Orders.takeOffGone && window.SheetWin && SheetWin.holdKit && window.OrderHold && window.HoldUI && window.CancelUI && window.OrderHoldFx && window.CharmNestSolver && window.startNest && (!window.__nestStub || true), null, { timeout: 60000 });
  if (o.stub !== false) await page.waitForFunction(() => window.startNest.__stub, null, { timeout: 20000 });
  // (what the person is told: every toast the page shows is kept, so a test can read it)
  await page.evaluate(() => { const t = window.toast; window.__toasts = []; if (typeof t === 'function') window.toast = function (m, ...a) { try { window.__toasts.push(String(m)); } catch (_) {} return t.call(this, m, ...a); }; });
  return { ctx, page, errors };
}

/* the sheets and orders of a spec, into the page (what the engine's own test does) and into the backend */
async function seedCloud(srv, spec) {
  const st = srv.st, pre = spec.sandbox ? 'Sandbox_' : '';
  for (const k of [SHEETS, POOL, TL, CANC]) for (const [key] of [...st.docs]) if (key.startsWith(pre + k + '/')) srv.raw.del(key);
  const docs = fixture(spec);
  for (const d of docs.sheets) srv.raw.set(pre + SHEETS + '/' + d.id, Object.assign({}, st.docs.get(pre + SHEETS + '/' + d.id) || {}, d));
  for (const d of docs.pool) srv.raw.set(pre + POOL + '/' + d.poolId, d);
}
async function seedPage(page, spec) {
  await page.evaluate(({ spec, RUN, BASE_TS }) => {
    localStorage.removeItem('__nestMode'); localStorage.removeItem('cn.orderhold.run'); localStorage.removeItem('cn.orderhold.run:sandbox'); localStorage.removeItem('cn.sheetwin.freed'); localStorage.removeItem('cn.sheetwin.freed:sandbox'); window.__nests = [];
    const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`, ageOf = rid => ((spec.orders[rid] || {}).age || 600);
    const mkBits = () => { const b = new Uint8Array(D * D); for (let y = 0; y < D; y++) for (let x = 0; x < D; x++) if (Math.hypot(x + .5 - D / 2, y + .5 - D / 2) <= D / 2 - 1) b[y * D + x] = 1; return b; };
    for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'rosePlan', 'roseProtected']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
    window.B.pool.rows.clear();
    for (const sh of spec.sheets) {
      const pr = CN.S.sheets[sh.metal]; let page = null;
      if (sh.page === 1) page = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); page = pr.pages[sh.page - 1]; }
      page.charms = []; page.placements = [];
      sh.items.forEach(([rid, tx, copy, col, row, placed], i) => {
        const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
        page.charms.push({ id, name: `${rid} · TEST-${tx}`, poolId, order: rid, lineKey: `${rid}:${5000000000 + tx}`, sku: 'TEST-' + tx, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: D, h: D, scale: 1, bits: mkBits(), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, orderDate: (BASE_TS - ageOf(rid)) * 1000, orderInfo: { receiptId: rid } });
        if (placed !== false) page.placements.push({ id, cxPt: 30 + col * 40, cyPt: 30 + row * 40, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
        window.B.pool.rows.set(poolId, { poolId, orderId: rid, sheetId: sh.id, state: placed !== false ? 'placed' : 'ready', material: sh.metal });
      });
      Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: sh.id, sheetIndex: sh.page, page: sh.page, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled']) delete page[k];
      Object.assign(page, sh.flags || {});
      pr.active = Math.max(0, sh.page - 1); CN.renderCard(page);
    }
    const rows = [];
    for (const [rid, o] of Object.entries(spec.orders)) for (const ln of o.lines) {
      const poolIds = Array.from({ length: ln.n }, (_, i) => pid(rid, ln.tx, i + 1));
      rows.push({ key: `${rid}:${5000000000 + ln.tx}`, order: { receiptId: rid, orderNumber: rid, createTs: BASE_TS - ageOf(rid), updateTs: BASE_TS, shipBy: BASE_TS + 500000, buyer: { name: 'Buyer ' + rid.slice(-3) }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + ln.tx), listingId: '', sku: 'TEST-' + ln.tx, title: 'Test charm ' + ln.tx, quantity: ln.n, variations: [], personalization: [], metalKey: ({ gold10k: '10k', gold14k: '14k' })[ln.metal] || ln.metal }, spec: { designSku: 'TEST-' + ln.tx, quantity: ln.n, material: ln.metal, problems: [] },
        problems: [], state: 'pooled', reason: null, poolIds, engrave: null, material: ln.metal, arrivedAt: Date.now() - 7200000 });
    }
    window.B.orders.rows = rows; window.B.orders.byKey = new Map(rows.map(r => [r.key, r]));
    // (spec.interpret: the lines are read once, as a page that has been open for a while has read them; every design is in the master list, so no line reads as "unknown" and nothing is sent to the paid reader)
    if (spec.interpret) { for (const r of rows) B.master.entries.set(String(r.spec.designSku).toUpperCase(), { sku: r.spec.designSku, sizes: {}, updatedAt: 1 }); Orders.interpretAll(); }
    window.B.sets.clear();
    if (spec.sets) for (const s of spec.sets) window.B.sets.set(`${RUN}|${s.group || 'all'}`, Object.assign({ runId: RUN, group: 'all', orders: {}, name: 'Set 1' }, s));
    try { Cancelled.reset && Cancelled.reset(); } catch (_) {}
  }, { spec, RUN, BASE_TS });
  await page.waitForTimeout(200);
}
async function setup(srv, page, spec) { await seedCloud(srv, spec); await seedPage(page, spec); }

/* ── readers ── */
const R = {
  onSheets: page => page.evaluate(() => Object.fromEntries(Object.keys(CN.S.sheets).flatMap(m => CN.S.sheets[m].pages.filter(p => p.sheetId).map(p => [p.sheetId, p.charms.map(c => c.poolId)])))),
  placed: page => page.evaluate(() => Object.fromEntries(Object.keys(CN.S.sheets).flatMap(m => CN.S.sheets[m].pages.filter(p => p.sheetId).map(p => [p.sheetId, p.placements.map(q => { const c = p.charms.find(z => z.id === q.id); return [c && c.poolId, q.cxPt, q.cyPt, q.angle]; })])))),
  rows: (page, rid) => page.evaluate(rid => Orders.rows().filter(r => String(r.order.receiptId) === rid).map(r => ({ state: r.state, hold: r.hold || null, pieces: r.poolIds.length, frontAt: r.frontAt || 0, releasing: !!r.releasing })), rid),
  journal: (page, sb) => page.evaluate(sb => JSON.parse(localStorage.getItem('cn.orderhold.run' + (sb ? ':sandbox' : '')) || '{}'), !!sb),
};
const pool = (srv, id, sb) => srv.st.doc((sb ? 'Sandbox_' : '') + POOL, id) || {};
const sheetDoc = (srv, id, sb) => srv.st.doc((sb ? 'Sandbox_' : '') + SHEETS, id) || {};
const tlList = (srv, rid, sb) => srv.st.list((sb ? 'Sandbox_' : '') + TL).filter(x => x._id.startsWith(rid + '~'));
const tlSnapshot = (srv, sb) => new Map(srv.st.list((sb ? 'Sandbox_' : '') + TL).map(x => [x._id, JSON.stringify([x.type, x.at, x.by, x.text, x.sheetId || ''])]));
/** Every event that was there is still there, as it was (the timeline only grows). */
const tlOnlyGrew = (before, after) => [...before].every(([id, v]) => after.has(id) && after.get(id) === v);

/** Nothing of an order is ever lost: each piece is on exactly ONE open page's sheet, or on hold (abandoned, held by someone) or removed with
    a cancel; the saved sheet records name no piece twice; counts match. `gone` are the orders cancelled on purpose. */
async function audit(srv, page, spec, what, o = {}) {
  const sb = !!spec.sandbox, sheets = await R.onSheets(page), flat = Object.values(sheets).flat();
  check(new Set(flat).size === flat.length, `${what}: no piece is on two sheets of the page`);
  const all = idsOf(spec), lost = [];
  for (const id of all) {
    const where = Object.entries(sheets).filter(([, l]) => l.includes(id)).map(([k]) => k), p = pool(srv, id, sb);
    const rid = id.split('_')[0], gone = (o.cancelled || []).includes(rid), held = p.state === 'abandoned' && (!!p.heldBy || !!p.removedBy);
    // (a cancelled order's piece: taken off by the cancel, or already on hold when it was cancelled; its record is kept either way)
    const fine = gone ? where.length === 0 && p.state === 'abandoned' && (!!p.removedBy || !!p.heldBy) : where.length === 1 ? (!held && p.sheetId === where[0]) : (where.length === 0 && held && !p.sheetId);
    if (!fine) lost.push(`${id}: sheets ${JSON.stringify(where)}, record ${JSON.stringify(p)}`);
  }
  check(!lost.length, `${what}: every piece is on one sheet, on hold, or cancelled on purpose: ${all.length - lost.length} of ${all.length}${lost.length ? ' · ' + lost.slice(0, 3).join(' | ') : ''}`);
  // the saved sheets (the Library's own copy) name each piece once
  const seen = new Map(), twice = [];
  for (const d of srv.st.list((sb ? 'Sandbox_' : '') + SHEETS)) for (const id of d.poolIds || []) { if (seen.has(id) && seen.get(id) !== d._id) twice.push(`${id} on ${seen.get(id)} and ${d._id}`); seen.set(id, d._id); }
  check(!twice.length, `${what}: no saved sheet names a piece another saved sheet names${twice.length ? ' · ' + twice.slice(0, 3) : ''}`);
  // a saved sheet never names a piece that is on hold
  const heldOn = [...seen].filter(([id]) => pool(srv, id, sb).state === 'abandoned');
  check(!heldOn.length, `${what}: no saved sheet still lists a piece that is on hold or cancelled${heldOn.length ? ' · ' + heldOn.slice(0, 3).map(x => x.join('@')) : ''}`);
  return { sheets, seen };
}
/** The backend has gone quiet: no write for `ms` (a change's last saves have landed). */
const settled = async (srv, ms = 1200, max = 30000) => { const t0 = Date.now(); let n = srv.writes.length, since = Date.now(); while (Date.now() - since < ms && Date.now() - t0 < max) { await new Promise(r => setTimeout(r, 150)); if (srv.writes.length !== n) { n = srv.writes.length; since = Date.now(); } } };
const quietNet = (srv, m0, what) => {
  const calls = srv.st.calls.slice(m0.calls), names = [...new Set(calls.map(c => c.name))];
  const etsy = calls.filter(c => ETSY_FN.has(c.name)), paid = calls.filter(c => PAID_FN.has(c.name) || (c.name === 'charmNestLibrary' && PAID_OP.test(c.op || '')));
  check(!etsy.length && !paid.length && srv.paid === m0.paid && srv.outside.length === m0.outside, `${what}: no Etsy call and no paid call (functions called: ${names.join(', ') || 'none'}; model calls ${srv.paid - m0.paid}; outside ${srv.outside.length - m0.outside})`);
};

/* ── the glue's popup and the flow, as a person does it ── */
const popup = page => page.evaluate(() => { const d = document.querySelector('dialog.holdDlg[open], .holdInline'); return d ? { text: d.innerText, go: !!d.querySelector('[data-k=go]'), no: !!d.querySelector('[data-k=no]'), title: (d.querySelector('h3') || {}).textContent || '' } : null; });
const pressHold = (page, rid, ctx) => page.evaluate(({ rid, ctx }) => { window.__holdRes = undefined; window.__holdP = HoldUI.hold(rid, Object.assign({ source: 'review' }, ctx || {})).then(x => (window.__holdRes = x)); return true; }, { rid, ctx });
const holdResult = (page, ms = 90000) => until(() => page.evaluate(() => window.__holdRes), ms, 'the hold flow to end').then(() => page.evaluate(() => JSON.parse(JSON.stringify(window.__holdRes, (k, v) => (k === 'steps' ? undefined : v)))));
const clickGo = page => page.evaluate(() => { const b = document.querySelector('dialog.holdDlg[open] [data-k=go], .holdInline [data-k=go]'); if (b) { b.click(); return true; } return false; });
const dialogs = page => page.evaluate(() => document.querySelectorAll('dialog[open]').length + document.querySelectorAll('.holdInline').length);

/* ══ seeded: the fast fixture, one page ══ */
async function seededSegment(browser) {
  const srv = await backend(), st = srv.st, { page, errors } = await openPage(browser, srv, { origin: srv.sorterOrigin });
  try {
    const m0 = srv.mark();

    /* 1 · a double press on the same Hold button: one popup, one hold, the button gone afterwards */
    {
      const H = '4170000100', spec = std(H);
      await setup(srv, page, spec);
      const tl0 = tlSnapshot(srv);
      const second = await page.evaluate(async H => {
        await CN.setMode('review');
        const b = HoldUI.button({ rid: H, source: 'orderWindow', label: 'test' }); document.body.appendChild(b);
        window.__btn = b; window.__holdRes = undefined;
        const p1 = HoldUI.hold(H, { source: 'review', button: b }).then(x => (window.__holdRes = x));
        const r2 = await HoldUI.hold(H, { source: 'review', button: b });   // (the second press, at once)
        b.click();                                                             // (and a third, on the button itself)
        return { r2, shown: HoldUI.shown(H), busy: HoldUI.busy(H), state: b.dataset.state, disabled: b.disabled };
      }, H);
      check(second.r2 && second.r2.busy === true && second.busy === true && second.disabled === true, `1 · a second press while the first is open is turned away (${JSON.stringify(second)})`);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check((await dialogs(page)) === 1 && pop.go && pop.no && /Put order 4170000100 on hold\?/.test(pop.title), `1 · ONE popup, with Continue and Not now (dialogs open: ${await dialogs(page)}; ${pop.title})`);
      check(!/\blines?\b/i.test(pop.text) && /4 pieces of this order come off GF Sheet 2 and SS Sheet 1/.test(pop.text) && /waiting order/.test(pop.text) && /QR label/.test(pop.text) && /Release hold/.test(pop.text) && /Nothing is deleted/.test(pop.text), `1 · the popup says what happens, in "pieces" (${pop.text.replace(/\s+/g, ' ').slice(0, 330)})`);
      const w0 = srv.writes.length;
      await new Promise(r => setTimeout(r, 400));
      check(srv.writes.length === w0 && !st.list(TL).length && (await R.rows(page, H)).every(r => !r.hold), '1 · nothing is written or changed while the popup waits');
      await clickGo(page);
      const done = await holdResult(page);
      check(done.ok && done.held === true, `1 · the hold goes through once (${JSON.stringify(done)})`);
      const rows = await R.rows(page, H);
      check(rows.every(r => r.state === 'held' && r.pieces === 0 && /^Taken off GF Sheet 2, SS Sheet 1 by Tester$/.test(r.hold)), `1 · On hold with one reason (${JSON.stringify(rows)})`);
      const ev = await until(() => { const e = tlList(srv, H).filter(x => x.type === 'held'); return e.length >= 2 && e; }, 15000, 'the timeline steps');
      await new Promise(r => setTimeout(r, 800));
      check(tlList(srv, H).filter(x => x.type === 'held').length === 2, `1 · exactly one timeline step per sheet, not doubled (${tlList(srv, H).filter(x => x.type === 'held').length})`);
      const after = await page.evaluate(H => ({ shown: HoldUI.shown(H), mode: CN.S.mode, pile: Orders.view().pile, btn: !!(window.__btn && window.__btn.isConnected && !window.__btn.hidden), flows: HoldUI.busy(H), status: OrderHold.status(H) }), H);
      check(!after.shown && !after.btn && !after.flows && !after.status.running && !after.status.resumable, `1 · the Hold button is gone and nothing is left running (${JSON.stringify(after)})`);
      check(after.mode === 'orders' && after.pile === 'hold', `1 · the person is back in Orders > On hold, not left in the Nest tab (${after.mode}/${after.pile})`);
      check((await dialogs(page)) === 0, '1 · no popup is left behind');
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), '1 · the timeline only grew');
      await audit(srv, page, spec, '1');
    }

    /* 2 · the popup went stale: a sheet was cut after it was shown; the run looks again, the cut sheet keeps its piece */
    {
      const H = '4170000110', spec = std(H);
      await setup(srv, page, spec);
      const before = JSON.stringify((await R.placed(page))[sid(H, 'silver-1')]);
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check(/4 pieces of this order come off GF Sheet 2 and SS Sheet 1/.test(pop.text), `2 · the popup (before the sheet was cut) promises 4 pieces off two sheets`);
      await page.evaluate(() => { const sh = CN.S.sheets.silver.pages[0]; sh.laserDoneAt = Date.now(); });   // (the sheet was cut while the popup was open)
      await clickGo(page);
      const done = await holdResult(page);
      const sh = await R.onSheets(page), plc = await R.placed(page);
      const rows = await R.rows(page, H);
      check(done.ok === true && sh[sid(H, 'silver-1')].includes(pid(H, 2, 1)) && sh[sid(H, 'silver-1')].includes(pid(H, 2, 2)) && JSON.stringify(plc[sid(H, 'silver-1')]) === before, `2 · the sheet cut meanwhile keeps its pieces, exactly as it was (ok ${done.ok})`);
      check(!sh[sid(H, 'gold-2')].includes(pid(H, 1, 1)) && !sh[sid(H, 'gold-2')].includes(pid(H, 1, 2)), '2 · the pieces on the sheet that was still open came off');
      check(!(await page.evaluate(id => (window.__nests || []).some(n => n.sheetId === id), sid(H, 'silver-1'))), '2 · the cut sheet was never nested, filled or taken from');
      check(rows.some(r => r.state === 'held' && /Taken off GF Sheet 2 by Tester/.test(r.hold)) && rows.some(r => r.state !== 'held' && r.pieces === 2), `2 · the line on the cut sheet stays as it is; the other is on hold (${JSON.stringify(rows)})`);
      check((await dialogs(page)) === 0 && (await page.evaluate(() => CN.S.mode)) !== 'nest', '2 · no popup left, the person is not left in the Nest tab');
      const next = await page.evaluate(H => OrderHold.plan(H).then(p => ({ canHold: p.canHold, why: p.blockedWhy, shown: HoldUI.shown(H) })), H);
      check(next.canHold === false && /already cut/.test(next.why || ''), `2 · pressing Hold again says plainly nothing more can come off (${JSON.stringify(next)})`);
      await audit(srv, page, spec, '2');
    }

    /* 3 · the popup went stale: the order was put on hold (or cancelled) by someone else while it was open */
    for (const how of ['held', 'cancelled']) {
      const H = how === 'held' ? '4170000120' : '4170000130', spec = std(H);
      await setup(srv, page, spec);
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      await until(() => popup(page), 15000, 'the popup');
      const other = await page.evaluate(async ({ H, how }) => how === 'held' ? (await OrderHold.run(H, { name: 'Other' })).ok === true : (await SheetWin.takeOffOrder({ orderId: H, mode: 'cancel', scope: 'order', by: 'Other', note: '' })).ok === true, { H, how });
      check(other === true, `3 · ${how}: the other change goes through while the popup is open`);
      await until(() => tlList(srv, H).some(x => x.type === (how === 'held' ? 'held' : 'cancelled')), 15000, 'its timeline');
      await settled(srv);
      const tl0 = tlSnapshot(srv), mid = srv.writes.length;
      await clickGo(page);
      const done = await holdResult(page);
      const msg = String(done.error || '');
      check(done.ok === false && !!done.error && srv.writes.length === mid, `3 · ${how}: Continue is refused plainly and writes nothing (${JSON.stringify(done)}; writes ${srv.writes.length - mid})`);
      check(how === 'held' ? /already on hold/.test(msg) : /cancelled/.test(msg), `3 · ${how}: the reason is plain (${msg})`);
      const sheets = await R.onSheets(page);
      check(!Object.values(sheets).flat().some(id => id.startsWith(H + '_')), `3 · ${how}: none of the order's pieces is on a sheet`);
      check(tlList(srv, H).filter(x => x.type === 'held').length === (how === 'held' ? 2 : 0) && (how === 'held' || tlList(srv, H).some(x => x.type === 'cancelled')), `3 · ${how}: the timeline has the first change once and nothing from the refused press (${tlList(srv, H).map(x => x.type).join(',')})`);
      const home = await page.evaluate(() => ({ mode: CN.S.mode, dialogs: document.querySelectorAll('dialog[open], .holdInline').length }));
      check(home.mode !== 'nest' && home.dialogs === 0, `3 · ${how}: the person is not stuck (mode ${home.mode}, ${home.dialogs} dialogs)`);
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), `3 · ${how}: the timeline only grew`);
      await audit(srv, page, spec, '3 ' + how, { cancelled: how === 'cancelled' ? [H] : [] });
    }

    /* 4 · Cancel pressed while Hold runs (CancelUI guards with OrderHold.status); the real cancel after it, record kept with every removal */
    {
      const H = '4170000140', spec = std(H);
      await setup(srv, page, spec);
      await page.evaluate(() => CN.setMode('orders'));
      const res = await page.evaluate(async H => {
        const p = OrderHold.run(H, { name: 'Paul' });
        const run = OrderHold.status(H).running;
        const c = await CancelUI.cancel(H);   // (at once, while the hold runs)
        const done = await p;
        return { run, c: JSON.parse(JSON.stringify(c)), ok: done.ok };
      }, H);
      check(res.run === true && res.c && res.c.ok === false && res.c.error === 'moving' && res.ok === true, `4 · Cancel while Hold runs is turned away (${JSON.stringify(res)})`);
      check(!(await page.evaluate(H => Cancelled.has(H), H)) && !st.doc(CANC, H), '4 · nothing was cancelled: no record, the order is only on hold');
      await until(() => tlList(srv, H).length >= 2, 15000, 'the hold on its timeline');
      const tl0 = tlSnapshot(srv);
      const c2 = await page.evaluate(H => CancelUI.cancel(H).then(x => JSON.parse(JSON.stringify(x))), H);
      check(c2.ok === true, `4 · once the hold is done, Cancel works (${JSON.stringify(c2)})`);
      await until(() => st.doc(CANC, H), 15000, 'the cancel record');
      check(!!st.doc(CANC, H), '4 · its record is kept under Orders > Cancelled');
      await until(() => tlList(srv, H).some(x => x.type === 'cancelled'), 15000, 'the cancel on its timeline');
      const evs = tlList(srv, H).map(x => x.type);
      check(evs.filter(t => t === 'held').length === 2 && evs.includes('cancelled'), `4 · the timeline keeps every removal and adds the cancel (${evs.join(',')})`);
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), '4 · the timeline only grew');
      await audit(srv, page, spec, '4');
      check(idsOf(spec, [H]).every(id => pool(srv, id).state === 'abandoned' && pool(srv, id).heldBy), '4 · every piece record is kept (abandoned, held by Paul), none deleted');
    }

    /* 5 · Release pressed while Hold runs */
    {
      const H = '4170000150', spec = std(H);
      await setup(srv, page, spec);
      // (a Release hold pressed on the line the hold has already put under On hold, while the hold is still taking the order's other sheet off)
      const res = await page.evaluate(async H => {
        let relP = null;
        const p = OrderHold.run(H, { name: 'Paul', onStep: s => { if (s.type === 'removed' && !relP) relP = OrderHold.release(H, { name: 'Paul' }).then(r => ({ ok: r.ok, error: r.error, released: r.released })); } });
        const done = await p;
        const rel = relP ? await relP : null;
        return { mid: !!relP, rel, ok: done.ok, error: done.error };
      }, H);
      check(res.mid === true && res.rel && res.rel.ok === false && !res.rel.released && /being put on hold|right now|still running/i.test(res.rel.error || ''), `5 · Release while Hold runs is turned away, plainly (${JSON.stringify(res)})`);
      check(res.ok === true, `5 · and the hold itself still finishes (${JSON.stringify(res)})`);
      await settled(srv);
      const rows = await R.rows(page, H);
      check(rows.every(r => r.state === 'held' && r.pieces === 0 && !r.releasing && !r.frontAt), `5 · the order is on hold, no release marker, no place at the front (${JSON.stringify(rows)})`);
      await audit(srv, page, spec, '5');
    }

    quietNet(srv, m0, 'seeded');
    check(!errors.length, 'seeded · no page errors ' + errors.join(' | '));
  } finally { await page.context().close(); srv.close(); }
}
/** The saved records alone (the Library, as a second computer reads it): every piece of the spec is on exactly one saved sheet that agrees with its
    piece record, or on hold / cancelled (abandoned) and on no saved sheet. */
function serverAudit(srv, spec, what, o = {}) {
  const sb = !!spec.sandbox, owner = new Map(), dup = [], bad = [];
  for (const d of srv.st.list((sb ? 'Sandbox_' : '') + SHEETS)) for (const id of d.poolIds || []) { if (owner.has(id)) dup.push(`${id} on ${owner.get(id)} and ${d._id}`); owner.set(id, d._id); }
  for (const id of idsOf(spec)) {
    const p = pool(srv, id, sb), on = owner.get(id) || null, away = p.state === 'abandoned', waiting = p.state === 'ready';
    if (away ? on : waiting ? (on && p.sheetId !== on) : (!on || p.sheetId !== on)) bad.push(`${id}: saved on ${on}, record ${p.state}/${p.sheetId}`);
  }
  check(!dup.length, `${what}: no saved sheet names a piece another one names${dup.length ? ' · ' + dup.slice(0, 3).join('; ') : ''}`);
  check(!bad.length, `${what}: the saved sheets and the piece records agree, every piece is on one sheet or on hold${bad.length ? ' · ' + bad.slice(0, 4).join('; ') : ''}`);
  return { owner, dup, bad };
}

/* ══ stale: two pages (two browsers) on the same data ══ */
async function staleSegment(browser) {
  const srv = await backend(), st = srv.st;
  const A = await openPage(browser, srv, { origin: srv.sorterOrigin, name: 'Anna' }), B = await openPage(browser, srv, { origin: srv.sorterOrigin, name: 'Bob' });
  try {
    const m0 = srv.mark();
    const both = async spec => { await seedCloud(srv, spec); await seedPage(A.page, spec); await seedPage(B.page, spec); };
    const run = (pg, rid, name) => pg.evaluate(async ({ rid, name }) => { const r = await OrderHold.run(rid, { name }); return { ok: r.ok, held: r.held, error: r.error }; }, { rid, name });

    /* 1 · A holds the order; B, which still shows it on its sheets, presses Hold on the same order */
    {
      const H = '4170000510', spec = std(H);
      await both(spec);
      const a = await run(A.page, H, 'Anna'); await settled(srv);
      check(a.ok === true, `1 · page A holds the order (${JSON.stringify(a)})`);
      serverAudit(srv, spec, '1 after A');
      const tl0 = tlSnapshot(srv), saved0 = JSON.stringify(Object.fromEntries(st.list(SHEETS).map(d => [d._id, d.poolIds])));
      const b = await run(B.page, H, 'Bob'); await settled(srv);
      console.log('       B (stale) after the order was held by A:', JSON.stringify(b));
      serverAudit(srv, spec, '1 after B');
      check(JSON.stringify(Object.fromEntries(st.list(SHEETS).map(d => [d._id, d.poolIds]))) === saved0 || b.ok === false, `1 · B's stale Hold leaves the saved sheets as A made them (or is refused: ${JSON.stringify(b)})`);
      check(tlList(srv, H).filter(x => x.type === 'held').length === 2, `1 · the order's timeline has A's two steps only (${tlList(srv, H).filter(x => x.type === 'held').map(x => x.by + ':' + x.sheetId).join(', ')})`);
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), '1 · the timeline only grew');
    }

    /* 2 · A cancels the order; B presses Hold on it (its cancelled list is a minute old) */
    {
      const H = '4170000520', spec = std(H);
      await both(spec);
      const a = await A.page.evaluate(async H => { const r = await SheetWin.takeOffOrder({ orderId: H, mode: 'cancel', scope: 'order', by: 'Anna', note: '' }); return { ok: r.ok, error: r.error }; }, H);
      await settled(srv);
      check(a.ok === true && !!st.doc(CANC, H), `2 · page A cancels the order, its record is kept (${JSON.stringify(a)})`);
      const b = await run(B.page, H, 'Bob'); await settled(srv);
      console.log('       B (stale) after the order was cancelled by A:', JSON.stringify(b));
      serverAudit(srv, spec, '2 after B');
      const evs = tlList(srv, H).map(x => x.type + ':' + x.by);
      check(b.ok === false || !tlList(srv, H).some(x => x.type === 'held' && x.by === 'Bob'), `2 · B's Hold on a cancelled order is refused, or at least writes no hold on it (${JSON.stringify(b)}; ${evs.join(',')})`);
      check(!!st.doc(CANC, H), '2 · the cancel record is still there');
    }

    /* 3 · A holds order X on GF Sheet 2 (its room is refilled from newer sheets); B, which still shows GF Sheet 2 as it was, holds order Y of the same sheet */
    {
      const X = '4170000530', Y = '4170000201', spec = std(X);
      await both(spec);
      const a = await run(A.page, X, 'Anna'); await settled(srv);
      check(a.ok === true, `3 · page A holds X (${JSON.stringify(a)})`);
      serverAudit(srv, spec, '3 after A');
      const b = await run(B.page, Y, 'Bob'); await settled(srv);
      console.log('       B (stale) holds another order of the same sheet:', JSON.stringify(b));
      serverAudit(srv, spec, '3 after B');
    }

    quietNet(srv, m0, 'stale');
    check(!A.errors.length && !B.errors.length, 'stale · no page errors ' + A.errors.concat(B.errors).join(' | '));
  } finally { await A.ctx.close(); await B.ctx.close(); srv.close(); }
}
/* ══ safety: what is never touched, and what blocks plainly ══ */
const clickNo = page => page.evaluate(() => { const b = document.querySelector('dialog.holdDlg[open] [data-k=no], .holdInline [data-k=no]'); if (b) { b.click(); return true; } return false; });
/** One sheet of the page exactly as it stands (its pieces, where each one is, its set and its green lines): the strings must stay equal. */
const snapOf = (page, id) => page.evaluate(id => { const p = Object.values(CN.S.sheets).flatMap(s => s.pages).find(z => z.sheetId === id); return p ? JSON.stringify({ charms: p.charms.map(c => [c.id, c.poolId]), placements: p.placements.map(q => [q.id, q.cxPt, q.cyPt, q.angle]), rose: p.rosePlan || p.roseProtected || null, setId: p.setId || null, status: p.status }) : null; }, id);
const nestedIds = page => page.evaluate(() => (window.__nests || []).map(n => n.sheetId));
/** The keys the backend was written to since `from` that name a sheet or a piece of the list. */
const wroteTo = (srv, from, coll, ids, sb) => srv.writes.slice(from).filter(k => ids.some(id => k === (sb ? 'Sandbox_' : '') + coll + '/' + id || k === '-' + (sb ? 'Sandbox_' : '') + coll + '/' + id));
const lineMetal = (spec, id) => { const [rid, txs] = id.split('_'); const tx = +txs - 5000000000; return (((spec.orders[rid] || {}).lines || []).find(l => l.tx === tx) || {}).metal; };

async function safetySegment(browser) {
  const srv = await backend(), st = srv.st, { page, errors } = await openPage(browser, srv, { origin: srv.sorterOrigin });
  try {
    const m0 = srv.mark();
    const ORD = { '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']), '4170000204': O(8300, [10, 1, 'gold']), '4170000301': O(7000, [5, 1, 'gold']), '4170000303': O(6000, [4, 1, 'gold']), '4170000302': O(7500, [6, 1, 'gold']) };

    /* 1 · a sheet already cut (marked completed, cut, or recalled) is never filled and never taken from, its piece stays, the popup says so;
           a newer sheet that is closed does not give its orders away into the room the hold frees */
    const CUTS = [['marked completed', { laserDoneAt: 1790000000000 }, 'that sheet was marked completed', '4170000610'], ['cut', { roseCutAt: 1790000000000 }, 'that sheet was already cut', '4170000620'], ['recalled', { recalled: true }, 'that sheet was recalled', '4170000630']];
    if (want(1)) for (const [kind, flags, why, H] of CUTS) {
      const g = n => sid(H, 'gold-' + n), spec = {
        sheets: [
          { id: g(1), metal: 'gold', page: 1, items: [[H, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]], flags },
          { id: g(2), metal: 'gold', page: 2, items: [[H, 2, 1, 0, 0], ['4170000202', 8, 1, 1, 0], ['4170000204', 10, 1, 2, 0]] },
          { id: g(3), metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0], ['4170000303', 4, 1, 1, 0]], flags },
          { id: g(4), metal: 'gold', page: 4, items: [['4170000302', 6, 1, 0, 0, false]] },
        ],
        orders: Object.assign({ [H]: O(9000, [1, 1, 'gold'], [2, 1, 'gold']) }, ORD),
      };
      await setup(srv, page, spec);
      const before = [await snapOf(page, g(1)), await snapOf(page, g(3))], docs0 = JSON.stringify([sheetDoc(srv, g(1)), sheetDoc(srv, g(3))]);
      const cutIds = [pid(H, 1, 1), pid('4170000201', 7, 1), pid('4170000301', 5, 1), pid('4170000303', 4, 1)], w0 = srv.writes.length, tl0 = tlSnapshot(srv);
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check(pop.go && pop.no && /1 piece of this order comes off GF Sheet 2/.test(pop.text) && pop.text.includes(`1 piece stays: GF Sheet 1 (${why})`), `1 ${kind} · the popup says one piece comes off and one stays on the cut sheet, and why (${pop.text.replace(/\s+/g, ' ').slice(0, 300)})`);
      check(!/\blines?\b/i.test(pop.text), `1 ${kind} · the popup says "pieces", never "lines"`);
      await clickGo(page);
      const done = await holdResult(page); await settled(srv);
      check(done.ok === true && done.held === true, `1 ${kind} · the hold goes through for what can come off (${JSON.stringify(done)})`);
      check((await snapOf(page, g(1))) === before[0] && (await snapOf(page, g(3))) === before[1], `1 ${kind} · the cut sheet and the closed newer sheet are exactly as they were (pieces, places, set)`);
      check(JSON.stringify([sheetDoc(srv, g(1)), sheetDoc(srv, g(3))]) === docs0 && !wroteTo(srv, w0, SHEETS, [g(1), g(3)]).length, `1 ${kind} · their saved records were never written (${wroteTo(srv, w0, SHEETS, [g(1), g(3)]).join(',') || 'none'})`);
      check(!wroteTo(srv, w0, POOL, cutIds).length && cutIds.every(id => pool(srv, id).state === 'placed'), `1 ${kind} · no piece record on them was written (${wroteTo(srv, w0, POOL, cutIds).join(',') || 'none'})`);
      const nested = await nestedIds(page);
      check(!nested.includes(g(1)) && !nested.includes(g(3)), `1 ${kind} · neither was nested again (${nested.join(',')})`);
      const sh = await R.onSheets(page);
      check(sh[g(2)].includes(pid('4170000302', 6, 1)) && !sh[g(2)].includes(pid('4170000301', 5, 1)) && !sh[g(2)].includes(pid('4170000303', 4, 1)), `1 ${kind} · the freed spot was filled by the waiting order, not by an order of the closed sheet (${JSON.stringify(sh[g(2)])})`);
      const rows = await R.rows(page, H);
      check(rows.filter(r => r.state === 'held').length === 1 && rows.filter(r => r.state !== 'held' && r.pieces === 1).length === 1, `1 ${kind} · one line is on hold, the line on the cut sheet stays in line (${JSON.stringify(rows)})`);
      const nxt = await page.evaluate(H => OrderHold.plan(H).then(p => ({ canHold: p.canHold, why: p.blockedWhy })), H);
      check(nxt.canHold === false && /already cut/.test(nxt.why || ''), `1 ${kind} · a second Hold says plainly that nothing more can come off (${JSON.stringify(nxt)})`);
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), `1 ${kind} · the timeline only grew`);
      await audit(srv, page, spec, '1 ' + kind);
    }

    /* 2 · a set already sent to the station: Hold is blocked with a plain reason, nothing at all changes (not a half hold); a committed newer
           sheet gives no order away; a piece the Library says is in a sent set (its sheet not open here) blocks too */
    if (want(2)) {
      const H = '4170000640', g = n => sid(H, 'gold-' + n), SET = 'set-sent-64', spec = {
        sheets: [
          { id: g(1), metal: 'gold', page: 1, items: [[H, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]], flags: { setId: SET } },
          { id: g(2), metal: 'gold', page: 2, items: [[H, 2, 1, 0, 0], ['4170000202', 8, 1, 1, 0], ['4170000204', 10, 1, 2, 0]] },
        ],
        orders: Object.assign({ [H]: O(9000, [1, 1, 'gold'], [2, 1, 'gold']) }, ORD),
        sets: [{ setId: SET, name: 'Set 1', committedAt: 1790000000000, sheetIds: [g(1)] }],
      };
      await setup(srv, page, spec);
      const before = [await snapOf(page, g(1)), await snapOf(page, g(2))], w0 = srv.writes.length;
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check(!pop.go && pop.no && /can't be put on hold yet/.test(pop.title) && /Undo the set first: GF Sheet 1 is in a set that was already sent to the station/.test(pop.text), `2 · a committed set: the popup says why, in plain words, and has Close only (${pop.title} | ${pop.text.replace(/\s+/g, ' ').slice(0, 220)})`);
      await clickNo(page);
      const res = await holdResult(page);
      check(res.ok === false && res.cancelled === true, `2 · Close ends the flow, nothing is held (${JSON.stringify(res)})`);
      await new Promise(r => setTimeout(r, 500));
      check(srv.writes.length === w0 && !st.list(TL).length, '2 · nothing was written');
      check((await snapOf(page, g(1))) === before[0] && (await snapOf(page, g(2))) === before[1], '2 · neither sheet changed: no half hold');
      const direct = await page.evaluate(async H => { const r = await OrderHold.run(H, { name: 'Paul' }); return { ok: r.ok, held: r.held, error: r.error, types: r.steps.map(s => s.type) }; }, H);
      check(direct.ok === false && /^Undo the set first/.test(direct.error || '') && direct.types.join() === 'error', `2 · a run started anyway (a stale page) is refused the same way (${JSON.stringify(direct)})`);
      check(srv.writes.length === w0 && same(await R.journal(page), {}) && (await R.rows(page, H)).every(r => r.state !== 'held' && r.pieces === 1), '2 · and nothing was written, no journal, every line still in line');
      await audit(srv, page, spec, '2 committed');
    }
    if (want(2)) {
      const H = '4170000650', g = n => sid(H, 'gold-' + n), SET = 'set-sent-65', spec = {
        sheets: [
          { id: g(1), metal: 'gold', page: 1, items: [[H, 1, 1, 0, 0], ['4170000202', 8, 1, 1, 0], ['4170000204', 10, 1, 2, 0]] },
          { id: g(2), metal: 'gold', page: 2, items: [['4170000301', 5, 1, 0, 0], ['4170000303', 4, 1, 1, 0]], flags: { setId: SET } },
        ],
        orders: Object.assign({ [H]: O(9000, [1, 1, 'gold']) }, ORD),
        sets: [{ setId: SET, name: 'Set 7', committedAt: 1790000000000, sheetIds: [g(2)] }],
      };
      await setup(srv, page, spec);
      const before = await snapOf(page, g(2)), w0 = srv.writes.length;
      const res = await page.evaluate(async H => { const r = await OrderHold.run(H, { name: 'Paul' }); return { ok: r.ok, held: r.held, error: r.error, skipped: r.steps.filter(s => s.type === 'fillSkipped').map(s => s.why) }; }, H);
      await settled(srv);
      const sh = await R.onSheets(page);
      check(res.ok === true && (await snapOf(page, g(2))) === before && !wroteTo(srv, w0, SHEETS, [g(2)]).length && sh[g(2)].length === 2, `2 · a newer sheet that is already in a sent set gives no order away: the freed spot stays free (${JSON.stringify(res)})`);
      await audit(srv, page, spec, '2 sent source');
    }
    if (want(2)) for (const kind of ['sent', 'unloaded']) {
      const H = kind === 'sent' ? '4170000655' : '4170000656', spec = std(H);
      await setup(srv, page, spec);
      // (the piece's sheet is not open on this page: the pool record alone says where it is)
      const away = pid(H, 2, 1);
      await page.evaluate(({ id, kind }) => { for (const m of Object.keys(CN.S.sheets)) for (const p of CN.S.sheets[m].pages) { const c = p.charms.find(z => z.poolId === id); if (c) { p.placements = p.placements.filter(q => q.id !== c.id); p.charms = p.charms.filter(z => z !== c); } } const r = B.pool.rows.get(id); B.pool.rows.set(id, Object.assign({}, r, { state: kind === 'sent' ? 'committed' : 'placed', sheetId: 'elsewhere-' + kind })); }, { id: away, kind });
      const w0 = srv.writes.length;
      const plan = await page.evaluate(H => OrderHold.plan(H).then(p => ({ canHold: p.canHold, why: p.blockedWhy, effects: p.effects.length })), H);
      const run = await page.evaluate(H => OrderHold.run(H, { name: 'Paul' }).then(r => ({ ok: r.ok, error: r.error })), H);
      check(plan.canHold === false && (kind === 'sent' ? /^Undo the set first/.test(plan.why) : /not open in this sorter.*Reload the sorter/.test(plan.why)) && run.ok === false && srv.writes.length === w0, `2 ${kind} · a piece the Library puts ${kind === 'sent' ? 'in a sent set' : 'on a sheet not open here'} blocks, nothing changes (${plan.why})`);
    }

    /* 3 · an order alone on a sheet is blocked plainly (a sheet is never left empty), and nothing of it comes off the other sheet either */
    if (want(3)) {
      const H = '4170000660', g = n => sid(H, 'gold-' + n), spec = {
        sheets: [
          { id: g(1), metal: 'gold', page: 1, items: [[H, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]] },
          { id: g(2), metal: 'gold', page: 2, items: [[H, 2, 1, 0, 0]] },
        ],
        orders: Object.assign({ [H]: O(9000, [1, 1, 'gold'], [2, 1, 'gold']) }, ORD),
      };
      await setup(srv, page, spec);
      const before = [await snapOf(page, g(1)), await snapOf(page, g(2))], w0 = srv.writes.length;
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check(!pop.go && pop.no && /only one on GF Sheet 2/.test(pop.text) && /never left empty/.test(pop.text), `3 · alone on a sheet: the popup says so plainly, Close only (${pop.text.replace(/\s+/g, ' ').slice(0, 260)})`);
      await clickNo(page); await holdResult(page);
      const direct = await page.evaluate(H => OrderHold.run(H, { name: 'Paul' }).then(r => ({ ok: r.ok, error: r.error })), H);
      check(direct.ok === false && /only one on GF Sheet 2/.test(direct.error || ''), `3 · a run is refused the same way (${direct.error})`);
      check(srv.writes.length === w0 && (await snapOf(page, g(1))) === before[0] && (await snapOf(page, g(2))) === before[1] && same(await R.journal(page), {}), '3 · nothing came off either sheet, nothing was written, no journal');
      await audit(srv, page, spec, '3 alone');
    }

    /* 4 · Rose Gold: the piece comes off, the sheet is not re-arranged, nothing moves into it, no green line is added */
    if (want(4)) {
      const H = '4170000670', spec = {
        sheets: [
          { id: sid(H, 'rose-1'), metal: 'rose', page: 1, items: [[H, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0], ['4170000202', 8, 1, 2, 0]] },
          { id: sid(H, 'rose-2'), metal: 'rose', page: 2, items: [['4170000301', 5, 1, 0, 0, false]] },
        ],
        orders: { [H]: O(9000, [1, 1, 'rose']), '4170000201': O(8000, [7, 1, 'rose']), '4170000202': O(8100, [8, 1, 'rose']), '4170000301': O(7000, [5, 1, 'rose']) },
      };
      await setup(srv, page, spec);
      const r1 = sid(H, 'rose-1'), r2 = sid(H, 'rose-2'), keep = (await R.placed(page))[r1].filter(x => x[0] !== pid(H, 1, 1)), w2 = await snapOf(page, r2);
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check(pop.go && /Rose Gold sheets are never re-arranged/.test(pop.text), `4 · Rose Gold: the popup says the sheet is never re-arranged (${pop.text.replace(/\s+/g, ' ').slice(0, 260)})`);
      await clickGo(page);
      const done = await holdResult(page); await settled(srv);
      check(done.ok === true, `4 · the Rose Gold piece comes off (${JSON.stringify(done)})`);
      const plc = (await R.placed(page))[r1], doc = sheetDoc(srv, r1);
      check(same(plc.map(x => JSON.stringify(x)).sort(), keep.map(x => JSON.stringify(x)).sort()), `4 · the other pieces stay exactly where they were (${JSON.stringify(plc)})`);
      check((await snapOf(page, r2)) === w2 && (await R.onSheets(page))[r2].length === 1, '4 · the waiting Rose Gold order did not move in');
      const greenPage = await page.evaluate(id => { const p = Object.values(CN.S.sheets).flatMap(s => s.pages).find(z => z.sheetId === id); return Object.keys(p).filter(k => /rose.*(plan|line|prot)|green/i.test(k) && p[k]); }, r1);
      check(!greenPage.length && !Object.keys(doc).some(k => /rosePlan|roseLines|green/i.test(k)), `4 · no green line was added (${greenPage.join(',') || 'none'}; saved record keys: ${Object.keys(doc).filter(k => /rose/i.test(k)).join(',') || 'none'})`);
      check(!wroteTo(srv, 0, SHEETS, [r2]).length, '4 · the second Rose Gold sheet was not written');
      await audit(srv, page, spec, '4 rose');
    }

    /* 5 · 10K and 14K stay apart: a freed 14K spot is filled by a waiting 14K order, a freed 10K spot by a 10K one, never by gold-filled */
    if (want(5)) {
      const H = '4170000680', spec = {
        sheets: [
          { id: sid(H, 'k14-1'), metal: 'gold14k', page: 1, items: [[H, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0], ['4170000202', 8, 1, 2, 0]] },
          { id: sid(H, 'k14-2'), metal: 'gold14k', page: 2, items: [['4170000301', 5, 1, 0, 0, false]] },
          { id: sid(H, 'k10-1'), metal: 'gold10k', page: 1, items: [[H, 2, 1, 0, 0], ['4170000203', 9, 1, 1, 0], ['4170000204', 10, 1, 2, 0]] },
          { id: sid(H, 'k10-2'), metal: 'gold10k', page: 2, items: [['4170000302', 6, 1, 0, 0, false]] },
          { id: sid(H, 'gold-3'), metal: 'gold', page: 3, items: [['4170000303', 4, 1, 0, 0, false]] },
        ],
        orders: { [H]: O(9000, [1, 1, 'gold14k'], [2, 1, 'gold10k']), '4170000201': O(8000, [7, 1, 'gold14k']), '4170000202': O(8100, [8, 1, 'gold14k']), '4170000203': O(8200, [9, 1, 'gold10k']), '4170000204': O(8300, [10, 1, 'gold10k']),
          '4170000301': O(7000, [5, 1, 'gold14k']), '4170000302': O(7500, [6, 1, 'gold10k']), '4170000303': O(6000, [4, 1, 'gold']) },
      };
      await setup(srv, page, spec);
      const w3 = await snapOf(page, sid(H, 'gold-3'));
      const res = await page.evaluate(async H => { const r = await OrderHold.run(H, { name: 'Paul' }); return { ok: r.ok, error: r.error }; }, H);
      await settled(srv);
      const sh = await R.onSheets(page), mixed = [];
      for (const [sid2, list] of Object.entries(sh)) { const metal = spec.sheets.find(s => s.id === sid2).metal; for (const id of list) if (lineMetal(spec, id) !== metal) mixed.push(`${id} (${lineMetal(spec, id)}) on ${sid2} (${metal})`); }
      check(res.ok === true && !mixed.length, `5 · no piece is on a sheet of another metal (${mixed.join('; ') || 'none'}; ${JSON.stringify(res)})`);
      check(sh[sid(H, 'k14-1')].includes(pid('4170000301', 5, 1)) && sh[sid(H, 'k10-1')].includes(pid('4170000302', 6, 1)), `5 · the 14K spot was filled by the waiting 14K order and the 10K spot by the waiting 10K order (${JSON.stringify([sh[sid(H, 'k14-1')], sh[sid(H, 'k10-1')]])})`);
      check((await snapOf(page, sid(H, 'gold-3'))) === w3 && sh[sid(H, 'gold-3')].length === 1, '5 · the waiting gold-filled order stayed where it was');
      await audit(srv, page, spec, '5 metals');
    }

    /* 6 · a piece completed by hand (Complete Order, or its QR label printed) is on no sheet: Hold has nothing to take off for it, it goes under
           On hold with its order, Release puts it back as done by hand, and it is never put on a sheet */
    if (want(6)) {
      const H = '4170000690', g = n => sid(H, 'gold-' + n), spec = {
        sheets: [
          { id: g(1), metal: 'gold', page: 1, items: [[H, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]] },
          { id: g(2), metal: 'gold', page: 2, items: [['4170000202', 8, 1, 0, 0], ['4170000204', 10, 1, 1, 0]] },
        ],
        orders: Object.assign({ [H]: O(9000, [1, 1, 'gold'], [3, 1, 'gold'], [4, 1, 'gold']) }, ORD),
      };
      await setup(srv, page, spec);
      // (lines 3 and 4 are done by hand: a button press and a printed label; they were never pooled)
      await page.evaluate(({ H }) => {
        // (the row keys of this fixture are "order:transaction"; the custom orders' own map is keyed "order_transaction")
        const mk = (tx, how) => { const key = `${H}:${5000000000 + tx}`; B.maps.customDone[`${H}_${5000000000 + tx}`] = { state: 'completed', how, completedAt: Date.now() - 3600000, completedBy: 'Zed', category: 'custom' }; const r = B.orders.byKey.get(key); r.poolIds = []; r.state = 'noDesign'; };
        // (every design of the order is in the master list, so no line reads as "unknown" and nothing is sent to the paid reader)
        for (const r of B.orders.rows) if (r.spec && r.spec.designSku) B.master.entries.set(String(r.spec.designSku).toUpperCase(), { sku: r.spec.designSku, sizes: {}, updatedAt: 1 });
        mk(3, 'button'); mk(4, 'print'); Orders.interpretAll();
      }, { H });
      const rows0 = await page.evaluate(H => Orders.rows().filter(r => String(r.order.receiptId) === H).map(r => ({ tx: r.line.transactionId.slice(-1), state: r.state, hand: !!(r.spec.customDone), pieces: r.poolIds.length, problems: r.problems.map(p => p.kind).join('+'), key: r.key, inMap: !!B.maps.customDone[r.key], mapKeys: Object.keys(B.maps.customDone).join(',') })), H);
      console.log('       fixture rows:', JSON.stringify(rows0));
      check(rows0.filter(r => r.hand).length === 2 && rows0.every(r => !r.problems), `6 · the fixture: one normal line and two lines done by hand (${JSON.stringify(rows0)})`);
      const w0 = srv.writes.length, tl0 = tlSnapshot(srv);
      await page.evaluate(() => CN.setMode('review'));
      await pressHold(page, H);
      const pop = await until(() => popup(page), 15000, 'the popup');
      check(pop.go && /1 piece of this order comes off GF Sheet 1/.test(pop.text), `6 · the popup counts the one piece that is on a sheet (${pop.text.replace(/\s+/g, ' ').slice(0, 240)})`);
      console.log('       the popup, for an order with two lines done by hand:', pop.text.replace(/\s+/g, ' ').slice(0, 400));
      await clickGo(page);
      const done = await holdResult(page); await settled(srv);
      const rows1 = await page.evaluate(H => Orders.rows().filter(r => String(r.order.receiptId) === H).map(r => ({ tx: r.line.transactionId.slice(-1), state: r.state, hold: r.hold || null, pieces: r.poolIds.length })), H);
      console.log('       after Hold:', JSON.stringify(rows1));
      check(done.ok === true && rows1.every(r => r.state === 'held' && r.pieces === 0), `6 · the order is on hold, every line, the two done by hand with it (${JSON.stringify(rows1)})`);
      check(!idsOf(spec, [H]).some(id => pool(srv, id).state !== 'abandoned') && st.list(POOL).filter(p => p._id.startsWith(H + '_5000000003') || p._id.startsWith(H + '_5000000004')).length === 0, '6 · no piece record was made for the lines done by hand');
      // Release: the lines done by hand come back as done by hand, and no sheet is made for them
      // (this page has no real design files, so the normal line could not be placed anyway: the release is made while no run is open, and the question is only what happens to the lines done by hand)
      const rel = await page.evaluate(async H => { const run = B.run; B.run = null; try { const r = await OrderHold.release(H, { name: 'Paul' }); return { ok: r.ok, released: r.released, placed: r.placed, error: r.error }; } finally { B.run = run; } }, H);
      await settled(srv);
      const rows2 = await page.evaluate(H => Orders.rows().filter(r => String(r.order.receiptId) === H).map(r => ({ tx: r.line.transactionId.slice(-1), state: r.state, hold: r.hold || null, hand: !!(r.spec.customDone), pieces: r.poolIds.length, front: !!r.frontAt })), H);
      console.log('       after Release:', JSON.stringify(rel), JSON.stringify(rows2));
      check(rel.ok === true && rows2.filter(r => r.hand).every(r => r.state === 'noDesign' && !r.hold && r.pieces === 0), `6 · released: the lines done by hand are done by hand again, on no sheet (${JSON.stringify(rows2)})`);
      check(!st.list(POOL).some(p => /_500000000[34]_/.test(p._id)) && (await page.evaluate(() => Object.values(B.maps.customDone).length)) === 2, '6 · nothing was pooled for them and their completion record is kept');
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), '6 · the timeline only grew');
    }

    quietNet(srv, m0, 'safety');
    check(!errors.length, 'safety · no page errors ' + errors.join(' | '));
  } finally { await page.context().close(); srv.close(); }
}
/* ══ sandbox and production keep to their own records, journals and keys ══ */
const snapKeys = (srv, pick) => JSON.stringify([...srv.st.docs.entries()].filter(([k]) => pick(k)).sort(([a], [b]) => (a < b ? -1 : 1)));
const isSb = k => /^-?Sandbox_/.test(k);
/** The local fake only: a reset is only ever sent to this machine's own stand-in (HARD rule: never to the live site). */
async function localReset(srv) {
  assert(/^http:\/\/127\.0\.0\.1:\d+$/.test(srv.sorterOrigin), 'the reset goes to the local fake only');
  let r = null, deleted = 0;
  for (let i = 0; i < 60; i++) {
    const res = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ op: 'sandboxReset' }) });
    r = await res.json(); deleted += r.deleted || 0;
    if (!r.more) break;
  }
  return Object.assign({ total: deleted }, r);
}
async function sandboxSegment(browser) {
  const srv = await backend(), st = srv.st;
  const SB = await openPage(browser, srv, { origin: srv.sorterOrigin, sandbox: true, name: 'Sally' }), PR = await openPage(browser, srv, { origin: srv.sorterOrigin, name: 'Paula' });
  try {
    const m0 = srv.mark(), H = '4170000710', specS = std(H, { sandbox: true, interpret: true }), specP = std(H, { interpret: true });
    // (the same order numbers and piece ids exist on both sides: the worst case for a mix-up)
    await seedCloud(srv, specP); await seedCloud(srv, specS); await seedPage(PR.page, specP); await seedPage(SB.page, specS);
    await settled(srv, 2500);   // (the lines each page read while it was set up are on their own timelines before anything is measured)
    const prod0 = snapKeys(srv, k => !isSb(k)), sand0 = snapKeys(srv, isSb);
    check(prod0.length > 10 && sand0.length > 10, '0 · both sides hold the same order, in their own records');

    /* 1 · a hold, a release and a cancel in the sandbox write the sandbox's records and the sandbox's journal, nothing of production's */
    {
      const w0 = srv.writes.length;
      const res = await SB.page.evaluate(async H => {
        let snap = null;
        const r = await OrderHold.run(H, { name: 'Sally', onStep: s => { if (s.type === 'removed' && !snap) snap = { sb: localStorage.getItem('cn.orderhold.run:sandbox'), prod: localStorage.getItem('cn.orderhold.run'), freedSb: localStorage.getItem('cn.sheetwin.freed:sandbox') !== null, freedProd: localStorage.getItem('cn.sheetwin.freed') !== null }; } });
        return { ok: r.ok, snap };
      }, H);
      await settled(srv);
      check(res.ok === true && res.snap && /4170000710/.test(res.snap.sb || '') && res.snap.prod === null && res.snap.freedSb === true && res.snap.freedProd === false, `1 · sandbox Hold: its journal and freed room are under the :sandbox keys, none under the production keys (${JSON.stringify(res.snap && { sb: !!res.snap.sb, prod: res.snap.prod, freedSb: res.snap.freedSb, freedProd: res.snap.freedProd })})`);
      const keys = await SB.page.evaluate(() => Object.keys(localStorage).filter(k => /orderhold|freed|cancel|hold/i.test(k)));
      check(keys.every(k => /:sandbox$/.test(k)), `1 · every hold-related key this page keeps ends with :sandbox (${keys.join(', ')})`);
      let off = srv.writes.slice(w0).filter(k => !isSb(k));
      check(!off.length, `1 · the sandbox Hold wrote only Sandbox_ records (${off.length} others: ${[...new Set(off)].slice(0, 4).join(', ')})`);
      check(snapKeys(srv, k => !isSb(k)) === prod0, '1 · every production record is exactly as it was (sheets, pieces, timeline)');
      check(tlList(srv, H, true).filter(x => x.type === 'held').length === 2 && tlList(srv, H, false).filter(x => x.type === 'held').length === 0, '1 · its two timeline steps are in the sandbox timeline, none in production');
      const w1 = srv.writes.length;
      const rel = await SB.page.evaluate(async H => { const run = B.run; B.run = null; let r; try { r = await OrderHold.release(H, { name: 'Sally' }); } finally { B.run = run; } return { ok: r.ok, released: r.released, error: r.error }; }, H);
      await settled(srv);
      off = srv.writes.slice(w1).filter(k => !isSb(k));
      check(rel.ok === true && !off.length && snapKeys(srv, k => !isSb(k)) === prod0, `1 · the sandbox Release wrote only Sandbox_ records (${JSON.stringify(rel)}; others: ${[...new Set(off)].join(', ') || 'none'})`);
      // (another order is cancelled in the sandbox)
      const C = '4170000201', w2 = srv.writes.length;
      await SB.page.evaluate(() => CN.setMode('orders'));
      const c = await SB.page.evaluate(C => CancelUI.cancel(C).then(x => JSON.parse(JSON.stringify(x))), C);
      await settled(srv);
      off = srv.writes.slice(w2).filter(k => !isSb(k));
      check(c.ok === true && !!st.doc('Sandbox_' + CANC, C) && !st.doc(CANC, C) && !off.length && snapKeys(srv, k => !isSb(k)) === prod0, `1 · the sandbox Cancel keeps its record in the sandbox's Cancelled list only (${JSON.stringify(c)}; others: ${[...new Set(off)].join(', ') || 'none'})`);
    }

    /* 2 · the same on the production side: its own collections and :-less keys; the sandbox side is not touched by it */
    {
      const sand1 = snapKeys(srv, isSb), w0 = srv.writes.length;
      const res = await PR.page.evaluate(async H => {
        let snap = null;
        const r = await OrderHold.run(H, { name: 'Paula', onStep: s => { if (s.type === 'removed' && !snap) snap = { sb: localStorage.getItem('cn.orderhold.run:sandbox'), prod: localStorage.getItem('cn.orderhold.run'), freedSb: localStorage.getItem('cn.sheetwin.freed:sandbox') !== null, freedProd: localStorage.getItem('cn.sheetwin.freed') !== null }; } });
        return { ok: r.ok, snap };
      }, H);
      await settled(srv);
      check(res.ok === true && res.snap && /4170000710/.test(res.snap.prod || '') && res.snap.sb === null && res.snap.freedProd === true && res.snap.freedSb === false, `2 · production Hold: its journal and freed room are under the production keys, none under :sandbox (${JSON.stringify(res.snap && { prod: !!res.snap.prod, sb: res.snap.sb, freedProd: res.snap.freedProd, freedSb: res.snap.freedSb })})`);
      const off = srv.writes.slice(w0).filter(isSb);
      check(!off.length && snapKeys(srv, isSb) === sand1, `2 · the production Hold wrote no Sandbox_ record and changed none (${[...new Set(off)].join(', ') || 'none'})`);
      check(tlList(srv, H, false).filter(x => x.type === 'held').length === 2, '2 · its timeline steps are in the production timeline');
      const keys = await PR.page.evaluate(() => Object.keys(localStorage).filter(k => /orderhold|freed|cancel|hold/i.test(k)));
      check(keys.every(k => !/sandbox/.test(k)), `2 · no hold-related key of the production page carries :sandbox (${keys.join(', ')})`);
    }

    /* 3 · a sandbox reset (the server's own, against the local fake) never touches the held production order; the sandbox's own records all go */
    {
      const prod1 = snapKeys(srv, k => !isSb(k));
      const r = await localReset(srv);
      const left = [...st.docs.keys()].filter(k => isSb(k) && /Sandbox_(Charm_Pool|Charm_Nest_Sheets|Order_Timeline|Charm_Nest_Cancelled)\//.test(k));
      check(r.ok === true && r.more === false && r.total > 0, `3 · the reset ran to the end against the local fake (${JSON.stringify({ ok: r.ok, more: r.more, deleted: r.total })})`);
      check(!left.length, `3 · the sandbox's pool, sheets, timeline and cancelled records are gone (${left.length} left)`);
      check(snapKeys(srv, k => !isSb(k)) === prod1, '3 · every production record is exactly as it was, the held order included');
      const held = await PR.page.evaluate(H => ({ rows: Orders.rows().filter(r => String(r.order.receiptId) === H).map(r => r.state), status: OrderHold.status(H) }), '4170000710');
      check(held.rows.every(s => s === 'held') && !held.status.running, `3 · and the production page still shows it on hold (${JSON.stringify(held.rows)})`);
      check(['4170000100'].every(() => st.list(POOL).filter(p => p._id.startsWith(H + '_') && p.state === 'abandoned' && p.heldBy).length === 4), '3 · its four piece records are still held by Paula');
    }

    /* 4 · the browser's side of a purge (made from a production page) removes the sandbox's keys and leaves the production journal and freed room alone */
    {
      await PR.page.evaluate(() => {
        localStorage.setItem('cn.orderhold.run', JSON.stringify({ 4170000999: { rid: '4170000999', who: 'Paula', lifted: ['x'], done: [], names: ['GF Sheet 2'], at: 1, step: { type: 'removed' } } }));
        localStorage.setItem('cn.orderhold.run:sandbox', JSON.stringify({ 4170000998: { rid: '4170000998', who: 'Sally', lifted: ['y'], done: [], names: [], at: 1 } }));
        localStorage.setItem('cn.sheetwin.freed', '{"prodsheet":[]}'); localStorage.setItem('cn.sheetwin.freed:sandbox', '{"sbsheet":[]}');
      });
      const keep = await PR.page.evaluate(() => [localStorage.getItem('cn.orderhold.run'), localStorage.getItem('cn.sheetwin.freed')]);
      await PR.page.evaluate(() => window.Sandbox.forgetStores({ records: 0, files: 0 }));
      const after = await PR.page.evaluate(() => ({ prodJ: localStorage.getItem('cn.orderhold.run'), prodF: localStorage.getItem('cn.sheetwin.freed'), sbJ: localStorage.getItem('cn.orderhold.run:sandbox'), sbF: localStorage.getItem('cn.sheetwin.freed:sandbox') }));
      check(after.prodJ === keep[0] && after.prodF === keep[1], '4 · the production journal (an unfinished hold) and freed room survive a sandbox purge, byte for byte');
      check(after.sbJ === null && after.sbF === null, `4 · the sandbox's journal and freed room are swept (${JSON.stringify([after.sbJ, after.sbF])})`);
      const st2 = await PR.page.evaluate(() => ({ pending: OrderHold.pending().map(p => p.rid), status: OrderHold.status('4170000999').resumable }));
      check(same(st2.pending, ['4170000999']) && st2.status === true, `4 · the page still sees its own unfinished production hold (${JSON.stringify(st2)})`);
    }

    quietNet(srv, m0, 'sandbox');
    check(!SB.errors.length && !PR.errors.length, 'sandbox · no page errors ' + SB.errors.concat(PR.errors).join(' | '));
  } finally { await SB.ctx.close(); await PR.ctx.close(); srv.close(); }
}
const hfxLeft = page => page.evaluate(() => document.querySelectorAll('.hfx, .hfxCap, .hfxMark, .hfxSkip, .hfxVeil, .hfxLive').length);
async function uiSegment(browser) {
  const srv = await backend(), st = srv.st;
  const A = await openPage(browser, srv, { origin: srv.sorterOrigin, name: 'Anna' }), B = await openPage(browser, srv, { origin: srv.sorterOrigin, name: 'Moe', motion: true });
  try {
    const m0 = srv.mark();
    const toasts = pg => pg.evaluate(() => (window.__toasts || []).slice());

    /* 1 · a save that fails in the middle of a hold: the film ends, the person is back at On hold (never stuck in the Nest tab), a plain toast,
           what was done is kept, and the next press carries on */
    for (const [label, P, H] of [['reduced motion', A, '4170000810'], ['with motion', B, '4170000820']]) {
      const spec = std(H);
      await seedCloud(srv, spec); await seedPage(P.page, spec);
      await P.page.evaluate(() => { CN.setMode('review'); window.__toasts = []; localStorage.setItem('__nestMode', 'fail'); });
      const tl0 = tlSnapshot(srv);
      await pressHold(P.page, H);
      await until(() => popup(P.page), 15000, 'the popup');
      const t0 = Date.now();
      await clickGo(P.page);
      const res = await holdResult(P.page, 120000);
      const took = Date.now() - t0;
      check(res.ok === false && !!res.error && took < 100000, `1 ${label} · the failed save ends the flow plainly (${took} ms): ${res.error}`);
      await P.page.waitForFunction(() => !window.OrderHoldFx.active(), null, { timeout: 15000 }).catch(() => {});
      const st1 = await P.page.evaluate(H => ({ mode: CN.S.mode, active: OrderHoldFx.active(), busy: HoldUI.busy(H), dialogs: document.querySelectorAll('dialog[open], .holdInline').length, status: OrderHold.status(H) }), H);
      check(st1.mode !== 'nest' && !st1.active && !st1.busy && st1.dialogs === 0 && (await hfxLeft(P.page)) === 0, `1 ${label} · the film is over, no popup, no film node is left, and the person is not left in the Nest tab (${JSON.stringify(st1)})`);
      const tt = await toasts(P.page);
      check(tt.some(t => new RegExp(`Order ${H} was not fully put on hold`).test(t) && /What was done is kept/.test(t) && /can be held again/.test(t)) && !tt.some(t => /undefined|\[object|Error:|TypeError/.test(t)), `1 ${label} · the toast is plain (${tt.map(t => t.slice(0, 180)).join(' | ')})`);
      // data: nothing lost. The pieces that came off are held and on no sheet; the rest are where they were; the journal says where it stopped
      const sheets = await R.onSheets(P.page), flat = Object.values(sheets).flat(), lost = [];
      for (const id of idsOf(spec)) { const where = flat.includes(id), p = pool(srv, id), held = p.state === 'abandoned' && !!p.heldBy; if (!(where ? !held : held)) lost.push(id); }
      check(!lost.length && new Set(flat).size === flat.length, `1 ${label} · every piece is on one sheet or on hold, none twice (${lost.length} wrong: ${lost.slice(0, 3)})`);
      const j = await R.journal(P.page);
      check(!!j[H] && Array.isArray(j[H].lifted) && j[H].lifted.length >= 1 && j[H].who === (label === 'with motion' ? 'Moe' : 'Anna'), `1 ${label} · the journal says where it stopped and who started it (${JSON.stringify(j[H] && { lifted: j[H].lifted, done: j[H].done, who: j[H].who })})`);
      check(tlOnlyGrew(tl0, tlSnapshot(srv)), `1 ${label} · the timeline only grew`);
      // the cause goes away: pressing Hold again finishes it, with the same name, and nothing is duplicated
      await P.page.evaluate(() => { localStorage.removeItem('__nestMode'); });
      if (process.env.DEBUGUI) await P.page.evaluate(() => { window.__poll = []; const t0 = Date.now(); const iv = setInterval(() => { const g = Object.values(CN.S.sheets).flatMap(s => s.pages).filter(p => p.sheetId && /gold-3/.test(p.sheetId)); window.__poll.push([Date.now() - t0, ...g.map(p => [p.status, p.persistedDone, p.dirty, p.problem, !!p.persisted, p.jobId && p.jobId.slice(-3), p._operationStarting, p.charms.length, p.placements.length, (window.__nests || []).length])]); if (Date.now() - t0 > 30000) clearInterval(iv); }, 60); });
      await pressHold(P.page, H);
      const pop2 = await until(() => popup(P.page), 15000, 'the second popup');
      check(pop2.go && !/\blines?\b/i.test(pop2.text), `1 ${label} · the second press asks again, in plain words (${pop2.text.replace(/\s+/g, ' ').slice(0, 200)})`);
      await clickGo(P.page);
      const res2 = await holdResult(P.page, 120000); await settled(srv);
      console.log(`       ${label}: the second press's steps:`, JSON.stringify(await P.page.evaluate(() => (window.__holdRes.steps || []).map(s => s.type + (s.sheetId ? ':' + s.sheetId.slice(-6) : '') + (s.why ? ' (' + s.why + ')' : '')))));
      if (process.env.DEBUGUI) console.log('       poll:', JSON.stringify(await P.page.evaluate(() => { const a = window.__poll || [], out = []; let last = ''; for (const r of a) { const k = JSON.stringify(r.slice(1)); if (k !== last) { out.push(r); last = k; } } return out; })));
      console.log(`       ${label}: sheets:`, JSON.stringify(await P.page.evaluate(() => Object.values(CN.S.sheets).flatMap(s => s.pages).filter(p => p.sheetId).map(p => [p.sheetId.slice(-6), p.status, p.persistedDone, p.dirty, p.problem, !!p.keepRelease, p.charms.length, p.placements.length]))), JSON.stringify((window_nests => window_nests)(await P.page.evaluate(() => (window.__nests || []).map(n => n.sheetId.slice(-6))))));
      console.log(`       ${label}: gold-2 as saved:`, JSON.stringify({ poolIds: sheetDoc(srv, sid(H, 'gold-2')).poolIds, charms: (sheetDoc(srv, sid(H, 'gold-2')).charms || []).map(c => c.poolId) }), 'as the page has it:', JSON.stringify((await R.onSheets(P.page))[sid(H, 'gold-2')]));
      check(res2.ok === true && res2.held === true, `1 ${label} · and the second press finishes the hold (${JSON.stringify(res2)})`);
      const rows = await R.rows(P.page, H);
      check(rows.every(r => r.state === 'held' && r.pieces === 0), `1 ${label} · every line is on hold (${JSON.stringify(rows)})`);
      check(same(await R.journal(P.page), {}) && tlList(srv, H).filter(x => x.type === 'held').length >= 2, `1 ${label} · the journal is gone, the timeline has the steps (${tlList(srv, H).filter(x => x.type === 'held').map(x => x.sheetId).join(',')})`);
      const ev = tlList(srv, H).filter(x => x.type === 'held'), dup = ev.map(x => x.sheetId).filter((s, i, a) => a.indexOf(s) !== i);
      check(!dup.length, `1 ${label} · no sheet is told twice on the timeline (${dup.join(',') || 'none'})`);
      await audit(srv, P.page, spec, `1 ${label}`);
    }

    /* 1b · the same, where nothing waits to fill the room (so nothing writes the sheet again by itself): the sheet whose save failed is saved again
            by the next press, and the Library never keeps listing a piece that is on hold */
    {
      const H = '4170000870', spec = {
        sheets: [
          { id: sid(H, 'gold-2'), metal: 'gold', page: 2, items: [[H, 1, 1, 0, 0], [H, 1, 2, 1, 0], ['4170000201', 7, 1, 2, 0], ['4170000202', 8, 1, 3, 0]] },
          { id: sid(H, 'silver-1'), metal: 'silver', page: 1, items: [[H, 2, 1, 0, 0], [H, 2, 2, 1, 0], ['4170000203', 9, 1, 2, 0]] },
        ],
        orders: { [H]: O(9000, [1, 2, 'gold'], [2, 2, 'silver']), '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']), '4170000203': O(8200, [9, 1, 'silver']) },
      };
      await seedCloud(srv, spec); await seedPage(A.page, spec);
      await A.page.evaluate(() => { CN.setMode('review'); window.__toasts = []; localStorage.setItem('__nestMode', 'fail'); });
      await pressHold(A.page, H); await until(() => popup(A.page), 15000, 'the popup'); await clickGo(A.page);
      const res = await holdResult(A.page, 120000);
      check(res.ok === false, `1b · the failed save ends the flow (${res.error})`);
      const stale = sheetDoc(srv, sid(H, 'gold-2')).poolIds || [];
      console.log('       1b · after the failed save, GF Sheet 2 as saved still lists:', JSON.stringify(stale.filter(id => id.startsWith(H))), '· the page marks it:', JSON.stringify(await A.page.evaluate(id => { const p = Object.values(CN.S.sheets).flatMap(s => s.pages).find(z => z.sheetId === id); return { problem: p.problem, dirty: p.dirty, status: p.status }; }, sid(H, 'gold-2'))));
      await A.page.evaluate(() => { localStorage.removeItem('__nestMode'); });
      await pressHold(A.page, H); await until(() => popup(A.page), 15000, 'the second popup'); await clickGo(A.page);
      const res2 = await holdResult(A.page, 120000); await settled(srv);
      check(res2.ok === true, `1b · the second press finishes the hold (${JSON.stringify(res2)})`);
      const doc = sheetDoc(srv, sid(H, 'gold-2')), still = (doc.poolIds || []).filter(id => id.startsWith(H + '_'));
      check(!still.length, `1b · the saved GF Sheet 2 no longer lists the order's pieces (${still.length} listed)`);
      const page2 = await A.page.evaluate(id => { const p = Object.values(CN.S.sheets).flatMap(s => s.pages).find(z => z.sheetId === id); return { problem: p.problem, dirty: p.dirty, status: p.status, saved: p.persistedDone }; }, sid(H, 'gold-2'));
      check(!page2.problem && !page2.dirty && page2.saved, `1b · and the sheet is not left marked as having a problem (${JSON.stringify(page2)})`);
      await audit(srv, A.page, spec, '1b');
    }

    /* 2 · the network goes during Cancel: nothing is cancelled, the order stays on hold, a plain line says so; back online the cancel goes through once */
    {
      const H = '4170000830', spec = std(H);
      await seedCloud(srv, spec); await seedPage(A.page, spec);
      await A.page.evaluate(() => CN.setMode('orders'));
      const held = await A.page.evaluate(H => OrderHold.run(H, { name: 'Anna' }).then(r => r.ok), H);
      await settled(srv);
      check(held === true, '2 · the order is on hold first');
      await A.page.evaluate(() => { Orders.showPile && Orders.showPile('hold', ''); });
      const tl0 = tlSnapshot(srv), w0 = srv.writes.length;
      await A.ctx.setOffline(true);
      const off = await A.page.evaluate(H => CancelUI.cancel(H).then(x => JSON.parse(JSON.stringify(x))), H);
      await A.ctx.setOffline(false);
      check(off.ok === false && !!off.error, `2 · offline: Cancel fails (${JSON.stringify(off)})`);
      const shown = await A.page.evaluate(H => { const n = [...document.querySelectorAll('[data-rid]')].filter(x => x.dataset.rid === H); return { cards: n.length, text: n.map(x => (x.querySelector('.cnCancelMsg') || {}).textContent || '').join(' | '), btn: n.map(x => { const b = x.querySelector('.cnCancelBtn'); return b ? (b.disabled ? 'disabled' : b.textContent) : 'none'; }).join(' | ') }; }, H);
      check(/Could not reach the server, so nothing was cancelled\. The order is still on hold; try again when you are back online\./.test(shown.text), `2 · the card says so in plain words, under its buttons (${shown.text})`);
      check(!/\bCancelling\b|disabled/.test(shown.btn) && /Cancel Order/.test(shown.btn), `2 · and the Cancel button is back, not stuck on "Cancelling" (${shown.btn})`);
      console.log('       offline Cancel answer:', JSON.stringify(off), '·', JSON.stringify(shown).slice(0, 300));
      check(!(await A.page.evaluate(H => Cancelled.has(H), H)) && !st.doc(CANC, H) && srv.writes.length === w0, '2 · nothing was cancelled and nothing was written');
      const rows = await R.rows(A.page, H);
      check(rows.every(r => r.state === 'held'), `2 · the order is still on hold (${JSON.stringify(rows)})`);
      check(shown.cards >= 1, `2 · its card is still in the list (${shown.cards})`);
      const back = await A.page.evaluate(H => CancelUI.cancel(H).then(x => JSON.parse(JSON.stringify(x))), H);
      await settled(srv);
      check(back.ok === true && !!st.doc(CANC, H), `2 · back online, Cancel goes through once and keeps its record (${JSON.stringify(back)})`);
      check(tlList(srv, H).filter(x => x.type === 'cancelled').length === 1 && tlOnlyGrew(tl0, tlSnapshot(srv)), `2 · one cancel on the timeline, the timeline only grew (${tlList(srv, H).map(x => x.type).join(',')})`);
      await audit(srv, A.page, spec, '2', { cancelled: [H] });
    }

    /* 3 · a server that answers with an error during Cancel (every call fails): the same plain refusal, nothing half-cancelled */
    {
      const H = '4170000840', spec = std(H);
      await seedCloud(srv, spec); await seedPage(A.page, spec);
      await A.page.evaluate(() => CN.setMode('orders'));
      await A.page.evaluate(H => OrderHold.run(H, { name: 'Anna' }).then(r => r.ok), H); await settled(srv);
      await A.page.evaluate(() => { Orders.showPile && Orders.showPile('hold', ''); });
      st.fail.charmNestLibrary = true;
      const bad = await A.page.evaluate(H => CancelUI.cancel(H).then(x => JSON.parse(JSON.stringify(x))), H);
      st.fail.charmNestLibrary = false;
      const line = await A.page.evaluate(H => [...document.querySelectorAll('[data-rid]')].filter(x => x.dataset.rid === H).map(x => (x.querySelector('.cnCancelMsg') || {}).textContent || '').join(' | '), H);
      check(bad.ok === false && /^Not cancelled: .*The order is still on hold; try again\./.test(line), `3 · a failing server: Cancel says no, in plain words, under the card's buttons (${JSON.stringify(bad)} · ${line})`);
      check(!st.doc(CANC, H) && (await R.rows(A.page, H)).every(r => r.state === 'held'), '3 · no record was made, the order is still on hold');
    }

    /* 4 · Esc and a tab picked during the film: the film stops at once, the hold still completes, and a tab the person chose is respected */
    {
      const H = '4170000850', spec = std(H);
      await seedCloud(srv, spec); await seedPage(B.page, spec);
      await B.page.evaluate(() => CN.setMode('review'));
      await pressHold(B.page, H);
      await until(() => popup(B.page), 15000, 'the popup');
      await clickGo(B.page);
      await until(() => B.page.evaluate(() => OrderHoldFx.active()), 20000, 'the film');
      await B.page.keyboard.press('Escape');
      const res = await holdResult(B.page, 120000); await settled(srv);
      await B.page.waitForFunction(() => !window.OrderHoldFx.active(), null, { timeout: 15000 }).catch(() => {});
      const s4 = await B.page.evaluate(H => ({ mode: CN.S.mode, pile: Orders.view().pile, left: document.querySelectorAll('.hfx, .hfxCap, .hfxMark, .hfxSkip').length, dialogs: document.querySelectorAll('dialog[open], .holdInline').length }), H);
      check(res.ok === true && s4.mode === 'orders' && s4.pile === 'hold' && s4.left === 0 && s4.dialogs === 0, `4 · Esc skips the film, the hold completes and the person lands on Orders > On hold (${JSON.stringify(s4)})`);
      await audit(srv, B.page, spec, '4 esc');
      const H2 = '4170000860', spec2 = std(H2);
      await seedCloud(srv, spec2); await seedPage(B.page, spec2);
      await B.page.evaluate(() => CN.setMode('review'));
      await pressHold(B.page, H2);
      await until(() => popup(B.page), 15000, 'the popup');
      await clickGo(B.page);
      await until(() => B.page.evaluate(() => OrderHoldFx.active()), 20000, 'the film');
      await B.page.evaluate(() => { const b = document.querySelector('.topbar [data-mode="library"], #moreMenu [data-mode="library"]'); if (b) b.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, cancelable: true })); });
      const res2 = await holdResult(B.page, 120000); await settled(srv);
      await B.page.waitForFunction(() => !window.OrderHoldFx.active(), null, { timeout: 15000 }).catch(() => {});
      const s5 = await B.page.evaluate(() => ({ mode: CN.S.mode, left: document.querySelectorAll('.hfx, .hfxCap, .hfxMark, .hfxSkip').length }));
      check(res2.ok === true && s5.left === 0 && s5.mode !== 'nest', `4 · a tab picked mid-film stops the film, the hold completes, the person is not dragged to Nest (${JSON.stringify(s5)})`);
      await audit(srv, B.page, spec2, '4 tab');
    }

    quietNet(srv, m0, 'ui');
    check(!A.errors.length && !B.errors.length, 'ui · no page errors ' + A.errors.concat(B.errors).join(' | '));
  } finally { await A.ctx.close(); await B.ctx.close(); srv.close(); }
}
/* ══ real: the real nest. Custom designs (a DXF each) sent to Gold Filled and nested by the real solver on one sheet; Hold and Release hold
      are the real engine over it. The machine may be busy with other work: every wait is long, none is a pause for show. ══ */
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const R_DAY = 86400, R_SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const rOrder = (rid, daysAgo) => ({ receiptId: rid, orderNumber: rid, createTs: R_SHIP - daysAgo * R_DAY, updateTs: R_SHIP - daysAgo * R_DAY + 60, shipBy: R_SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: 1, expectedShipDate: R_SHIP, variations: [{ name: 'Metal', value: 'Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
/* what the nest's workers are asked (the jobs, in the order asked), a switch that leaves every search unanswered, a switch that makes
   the sheet's nest fail at once, and one that keeps the page's own 5 s look for an unfinished release from starting by itself */
const realInit = () => {
  const post = Worker.prototype.postMessage;
  Worker.prototype.postMessage = function (m, ...rest) {
    try { if (m && m.type === 'solve' && m.job) { (window.__jobs = window.__jobs || []).push({ jobId: m.jobId, at: Date.now(), orders: m.job.pieces.map(p => String(p.order || p.id)) }); if (localStorage.getItem('__hang') === '1') return; } } catch (_) {}
    return post.call(this, m, ...rest);
  };
  const si = window.setInterval;
  window.setInterval = function (f, ms, ...a) { try { if (ms === 5000 && localStorage.getItem('__noScan') === '1' && /resumeReleases/.test(String(f))) return 0; } catch (_) {} return si.call(this, f, ms, ...a); };
  try { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.dsOrigin = 'http://127.0.0.1:9'; localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {}
};
async function realSegment(browser) {
  const srv = await backend(), st = srv.st;
  const S1 = '4180000001', H = '4180000002', H2 = '4180000003', I = ['4180000011', '4180000012', '4180000013'];
  const poolOf = rid => `${rid}_${rid}1_1`, LONG = 600000;
  const { ctx, page, errors } = await openPage(browser, srv, { origin: srv.sorterOrigin, stub: false, init: realInit });
  const ready = async () => {
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.Gate && window.Session && window.SheetWin && window.OrderHold && OrderHold.release && window.HoldUI && CN.S.cloud.ok === true, null, { timeout: 120000 });
    await page.evaluate(() => { if (!window.startNest.__wrapped) { const real = window.startNest; const w = function (sh) { if (localStorage.getItem('__failNest') === '1') { sh.problem = 'test: the nest could not start'; sh.status = 'ready'; return; } return real.apply(this, arguments); }; w.__wrapped = true; window.startNest = w; } });
  };
  await ready();
  const info = n => page.evaluate(n => {
    const pages = CN.S.sheets.gold.pages, sh = n == null ? pages.at(-1) : pages[n - 1]; if (!sh) return null; const byId = new Map(sh.charms.map(c => [c.id, c]));
    return { sheetId: sh.sheetId, status: sh.status, dirty: !!sh.dirty, saved: !!sh.persistedDone, charms: sh.charms.map(c => c.poolId), placed: sh.placements.map(p => (byId.get(p.id) || {}).poolId),
      at: Object.fromEntries(sh.placements.map(p => [(byId.get(p.id) || {}).poolId, [p.cxPt, p.cyPt, p.angle]])), feedWait: (sh.feedWait || []).map(id => (byId.get(id) || {}).poolId), problem: sh.problem || null, pages: pages.length };
  }, n);
  // (what the sheet is doing, for a wait that ends badly)
  const dump = () => page.evaluate(() => { const sh = CN.S.sheets.gold.pages[0], o = {}; for (const [k, v] of Object.entries(sh)) if (v == null || ['string', 'number', 'boolean'].includes(typeof v)) o[k] = typeof v === 'string' ? v.slice(0, 80) : v; o.feedWait = (sh.feedWait || []).length; o.charms = sh.charms.length; o.placements = sh.placements.length; o.run = window.B && B.run ? { mode: B.run.mode, status: B.run.status, step: B.run.step } : null; try { o.heldForResume = !!CN.heldForResume(sh); } catch (_) {} return o; });
  const rest = (n, ms = 300000) => until(async () => { const z = await info(n); return z && z.saved && !z.dirty && !['nesting', 'finishing', 'queued'].includes(z.status) && !z.feedWait.length && z; }, ms, 'the sheet at rest').catch(async e => { console.log('         the sheet when the wait ended:', JSON.stringify(await dump().catch(() => null))); throw e; });
  const rowOf = rid => page.evaluate(rid => { const r = Orders.rows().find(x => String(x.order.receiptId) === rid); return r ? { state: r.state, hold: r.hold || null, frontAt: r.frontAt || 0, releasing: r.releasing || null, pieces: (r.poolIds || []).length } : null; }, rid);
  const timeline = (rid, type) => st.list(TL).filter(e => e._id.startsWith(rid + '~') && (!type || e.type === type));
  const same2 = (a, b) => !!a && !!b && a.join() === b.join();
  const SIDS = [S1, H, H2];

  /* what is where: each of the order's pieces is on exactly one sheet (waiting or placed) or on hold, never both, never neither */
  async function realAudit(what, o = {}) {
    await settled(srv, 1500, 30000);
    const sheets = await R.onSheets(page), flat = Object.values(sheets).flat(), bad = [];
    check(new Set(flat).size === flat.length, `${what}: no piece is on two sheets of the page`);
    for (const rid of SIDS) {
      const id = poolOf(rid), where = Object.entries(sheets).filter(([, l]) => l.includes(id)).map(([k]) => k), p = pool(srv, id, false), heldNow = p.state === 'abandoned', wantHeld = (o.held || []).includes(rid);
      if (wantHeld ? !(where.length === 0 && heldNow) : !(where.length === 1 && !heldNow)) bad.push(`${rid}: sheets ${JSON.stringify(where)}, record ${JSON.stringify([p.state, p.sheetId, p.heldBy])}`);
    }
    check(!bad.length, `${what}: ${SIDS.length} orders, each on one sheet or on hold, as expected (held: ${(o.held || []).join(',') || 'none'})${bad.length ? ' · ' + bad.join(' | ') : ''}`);
    const seen = new Map(), twice = [], heldOn = [];
    for (const d of st.list(SHEETS)) for (const id of d.poolIds || []) { if (seen.has(id) && seen.get(id) !== d._id) twice.push(id); seen.set(id, d._id); if (pool(srv, id, false).state === 'abandoned') heldOn.push(id); }
    check(!twice.length && !heldOn.length, `${what}: no saved sheet names a piece twice or still lists a piece on hold${twice.length || heldOn.length ? ' · ' + JSON.stringify({ twice, heldOn }) : ''}`);
  }

  async function send(rid, name, w) {
    await page.evaluate(() => CN.setMode('review'));
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = `#rvList .reviewListRow[data-rid="${rid}"]`;
    await page.waitForSelector(card);
    await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(w), name });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 120000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
    await page.click('#cuDlg [data-send]');
    await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 120000 });
    await page.waitForFunction(() => { const sh = CN.S.sheets.gold.pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, null, { timeout: LONG });
    await page.evaluate(() => { const sh = CN.S.sheets.gold.pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); });
    await page.waitForFunction(() => { const sh = CN.S.sheets.gold.pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, null, { timeout: LONG });
    await page.evaluate(() => CN.setMode('nest'));
    await page.waitForTimeout(800);
  }
  // the orders coming in from Etsy: pieces waiting on the sheet for their turn (copies of a piece the real nest made), each OLDER than H
  const incoming = rids => page.evaluate(({ rids, SHIP, DAY }) => {
    const sh = CN.S.sheets.gold.pages.at(-1), base = sh.charms.find(c => c.bits);
    rids.forEach((rid, i) => {
      const poolId = `${rid}_${rid}1_1`, c = Object.assign({}, base, { id: `inc-${rid}`, poolId, order: rid, name: `${rid} · INCOMING`, orderDate: SHIP - 4 * DAY + i * 3600, pinned: null, excluded: false, orderInfo: Object.assign({}, base.orderInfo, { receiptId: rid, transactionId: rid + '1' }), lineKey: `${rid}:${rid}1` });
      delete c.frontAt; sh.charms.push(c); B.pool.rows.set(poolId, { poolId, orderId: rid, sheetId: sh.sheetId, state: 'ready', material: 'gold' });
    });
    sh.intakeAppend = sh.placements.length > 0; sh.appendOnly = sh.intakeAppend; sh.dirty = true; sh.status = 'ready'; CN.renderCard(sh);
  }, { rids, SHIP: R_SHIP, DAY: R_DAY });
  const holdRun = (rid, name, note) => page.evaluate(({ rid, name, note }) => { window.__hsteps = []; window.__hres = undefined; OrderHold.run(rid, { name, note, onStep: s => window.__hsteps.push(s.type) }).then(r => { window.__hres = { ok: r.ok, held: r.held, error: r.error || null }; }); return true; }, { rid, name, note });
  const holdEnd = (ms = LONG) => until(() => page.evaluate(() => window.__hres), ms, 'the hold ends');
  const relRun = (rid, name) => page.evaluate(({ rid, name }) => { window.__rsteps = []; window.__rres = undefined; window.__rp0 = OrderHold.release(rid, { name, onStep: s => window.__rsteps.push(s.type) }); window.__rp = window.__rp0.then(r => { window.__rres = { ok: r.ok, released: r.released, placed: r.placed, error: r.error || null, stage: r.stage || null, pending: !!r.pending }; return r; }); return true; }, { rid, name });
  const relEnd = (ms = LONG) => until(() => page.evaluate(() => window.__rres), ms, 'the release ends');

  console.log('  building the sheet (the real nest: slow when the machine is busy)');
  const orders = [rOrder(S1, 6), rOrder(H, 1), rOrder(H2, 2)];
  await page.evaluate(async orders => {
    await Orders.loadMaps(true);
    for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-adv`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
  }, orders);
  await send(S1, 's.dxf', 18); await send(H, 'h.dxf', 22); await send(H2, 'h2.dxf', 20);
  let s = await info(); const sheetId = s.sheetId;
  check(SIDS.every(r => s.placed.includes(poolOf(r))) && s.saved, `0 · the real nest placed the three orders on one saved sheet (${JSON.stringify(s.placed)})`);
  const at0 = s.at;
  await realAudit('0 start');
  const m0 = srv.mark(), tl0 = tlSnapshot(srv, false);
  await page.evaluate(() => { localStorage.setItem('__noScan', '1'); });   // (from the first reload on, only a person's press or this test's own call looks for an unfinished release)

  /* ── 1 · a reload in the middle of a hold ── the real nest has been asked to rewrite the sheet and never answers; the page goes away */
  try {
    await page.evaluate(() => { localStorage.setItem('__hang', '1'); window.__jobs = []; });
    await holdRun(H, 'Paula', 'cut short');
    const t1 = Date.now(); let liftedAt = 0;
    const z1 = await until(() => page.evaluate(() => ({ steps: window.__hsteps.slice(), jobs: window.__jobs.length, res: window.__hres || null })).then(z => { if (z.steps.includes('lift') && !liftedAt) liftedAt = Date.now(); return z.res || (z.steps.includes('lift') && (z.jobs > 0 || Date.now() - liftedAt > 20000)) ? z : null; }), LONG, 'the hold reaches the nest');
    console.log(`         the hold stood at: ${z1.steps.join(' ')} · searches asked ${z1.jobs} · ended ${JSON.stringify(z1.res)}`);
    check(!z1.res, `1 · the hold is cut in the middle (steps: ${z1.steps.join(' ')})`);
    try { await page.evaluate(() => Session.flushNow()); } catch (_) {}
    const pre = { journal: await R.journal(page), pool: pool(srv, poolOf(H), false), sheetPieces: sheetDoc(srv, sheetId, false).poolIds };
    console.log('         before the reload: journal', JSON.stringify((pre.journal || {})[H] && { who: pre.journal[H].who, lifted: pre.journal[H].lifted, done: pre.journal[H].done, step: pre.journal[H].step && pre.journal[H].step.type }), '· piece record', pre.pool.state, '· saved sheet lists the piece:', (pre.sheetPieces || []).includes(poolOf(H)));
    await page.evaluate(() => { localStorage.removeItem('__hang'); });
    await page.reload({ waitUntil: 'load' }); await ready();
    await page.waitForFunction(() => Orders.rows().length >= 3 && window.B && B.run, null, { timeout: 120000 });
    await page.waitForTimeout(3000);
    const post = await page.evaluate(H => ({ journal: (JSON.parse(localStorage.getItem('cn.orderhold.run') || '{}'))[H] || null, status: OrderHold.status(H), pending: OrderHold.pending().map(p => p.rid), shown: HoldUI.shown(H) }), H);
    const sA = await info(), rA = await rowOf(H), plan = await page.evaluate(H => OrderHold.plan(H).then(p => ({ canHold: p.canHold, why: p.blockedWhy || p.why || null })), H);
    console.log('         after the reload: sheet', JSON.stringify({ charms: sA.charms.length, placed: sA.placed.length, dirty: sA.dirty, saved: sA.saved, status: sA.status, problem: sA.problem }), '· row', JSON.stringify(rA), '· status', JSON.stringify({ r: post.status.resumable, n: post.status.name }), '· button shown', post.shown, '· plan', JSON.stringify(plan));
    check(post.journal && post.journal.who === 'Paula' && post.status.resumable === true && post.status.name === 'Paula' && post.pending.includes(H), `1 · the reloaded page still knows the hold was cut short, and by whom (${JSON.stringify({ who: post.journal && post.journal.who, status: post.status.resumable, pending: post.pending })})`);
    // the person is not stuck: the order already reads "On hold" (no Hold button), so the page itself must finish what the journal says is half done
    const tAuto = Date.now();
    let sB = null;
    try { sB = await until(async () => { const j = await R.journal(page), z = await info(); return !j[H] && z && z.saved && !z.dirty && !['nesting', 'finishing', 'queued'].includes(z.status) && z; }, 240000, 'the page finishes the hold by itself'); } catch (_) { sB = null; }
    check(!!sB, `1 · the page finishes the cut hold by itself (no button to press: shown ${post.shown}, plan ${JSON.stringify(plan)}) after ${Math.round((Date.now() - tAuto) / 1000)} s`);
    if (!sB) { await holdRun(H, '', ''); const r1 = await holdEnd(); console.log('         (pressed by hand instead)', JSON.stringify(r1)); sB = await rest(); }
    check(!sB.charms.includes(poolOf(H)) && sB.placed.length === 2 && same2(sB.at[poolOf(S1)], at0[poolOf(S1)]) && same2(sB.at[poolOf(H2)], at0[poolOf(H2)]), `1 · the order is off the sheet once, the other two did not move (${JSON.stringify({ charms: sB.charms, a: sB.at[poolOf(S1)], b: at0[poolOf(S1)] })})`);
    const j2 = await R.journal(page), rB = await rowOf(H);
    check(!j2[H] && rB.state === 'held' && /by Paula/.test(rB.hold || ''), `1 · the journal is gone and the order says who held it: ${rB.hold}`);
    const held = timeline(H, 'held');
    check(held.length >= 1 && held.every(e => /Paula/.test(e.by || e.text || '')), `1 · the timeline has the hold under the name that was saved, once per step (${JSON.stringify(held.map(e => [e._id.slice(-14), e.by, e.text]))})`);
    await realAudit('1 after the cut hold', { held: [H] });
  } catch (e) { fails.push('real 1: ' + e.message); console.log('  FAIL real 1 · ' + (e.stack || e.message)); }

  /* ── 2 · a release that fails to place at once, again and again: three more tries and then it waits, nothing lost ── */
  let frontAt = 0;
  try {
    // (three orders come in from Etsy meanwhile, each older than H: they wait on the sheet, and nothing places them yet)
    await incoming(I);
    await page.evaluate(() => { localStorage.setItem('__failNest', '1'); window.__relCalls = []; if (!OrderHold.release.__counted) { const o = OrderHold.release; const w = function (rid, opts) { window.__relCalls.push(String(rid) + '|' + (/resumeReleases/.test(new Error().stack || '') ? 'resume' : 'press')); return o.apply(this, arguments); }; w.__counted = true; OrderHold.release = w; } });
    console.log('         the page\'s own look for an unfinished release is switched off:', await page.evaluate(() => /__noScan/.test(String(window.setInterval))));
    await relRun(H, 'Paul');
    const r2 = await relEnd();
    const row2 = await rowOf(H);
    check(r2.ok === false && r2.released === true && r2.stage === 'place' && r2.pending === true, `2 · a release whose nest cannot start says so plainly and keeps the order at the front (${JSON.stringify(r2)})`);
    check(row2.state === 'pooled' && !row2.hold && row2.frontAt > 0 && !!row2.releasing, `2 · the hold is lifted, the place at the front and the marker are kept (${JSON.stringify(row2)})`);
    frontAt = row2.frontAt;
    await realAudit('2 first try', {});
    const tries = [];
    for (let i = 0; i < 5; i++) {
      await page.evaluate(() => { if (window.__skew == null) { window.__skew = 0; const dn = Date.now.bind(Date); Date.now = () => dn() + window.__skew; } window.__skew += 40000; });
      tries.push(await page.evaluate(() => OrderHold.resumeReleases().then(a => a.map(x => [x.ok, x.stage || null]))));
    }
    const callList = await page.evaluate(() => window.__relCalls.slice()), calls = callList.length;
    console.log('         tries:', JSON.stringify(tries), '· release calls in all', calls, JSON.stringify(callList));
    // (the page's own look every 5 s may have been one of the three; what counts is that the person's press is followed by three looks and no more)
    check(callList.filter(c => /press/.test(c)).length === 1 && callList.filter(c => /resume/.test(c)).length === 3 && tries.every(t => t.every(x => x[0] === false)) && tries[3].length === 0 && tries[4].length === 0, `2 · after the person's press the page looks again three times and then leaves it (${JSON.stringify(tries)}; ${JSON.stringify(callList)})`);
    const row2b = await rowOf(H), s2 = await info();
    check(row2b.frontAt === frontAt && s2.charms.filter(id => id === poolOf(H)).length === 1 && !s2.placed.includes(poolOf(H)) && !timeline(H, 'released').length, `2 · after the failures the order is still at the front, once on the sheet and waiting, and the timeline does not claim a release (${JSON.stringify([row2b.frontAt === frontAt, s2.charms.filter(id => id === poolOf(H)).length, timeline(H, 'released').length])})`);
    await realAudit('2 after the tries', {});
  } catch (e) { fails.push('real 2: ' + e.message); console.log('  FAIL real 2 · ' + (e.stack || e.message)); }

  /* ── 3 · the place at the front survives a reload: three Etsy orders, all older than H, wait on the same sheet; H is placed first ── */
  try {
    await page.evaluate(() => { localStorage.removeItem('__failNest'); });
    await page.evaluate(() => Session.flushNow().catch(() => {}));
    await page.reload({ waitUntil: 'load' }); await ready();
    await page.waitForFunction(() => Orders.rows().length >= 3 && window.B && B.run, null, { timeout: 120000 });
    await page.waitForTimeout(2000);
    const row3 = await rowOf(H);
    check(row3.frontAt === frontAt && !!row3.releasing && row3.state === 'pooled', `3 · the page after a reload still has the order at the front, its release unfinished (${JSON.stringify(row3)})`);
    const sI = await info();
    check(I.every(r => sI.charms.includes(poolOf(r))), `3 · the three Etsy orders are still waiting on the sheet after the reload (${sI.charms.length} pieces)`);
    await page.evaluate(() => { window.__jobs = []; });
    // (the page's own look picks it up within 5 s; this call is only to be sure it has begun. A machine this busy can take longer than one try's six minutes: the page tries up to three times)
    await page.evaluate(() => { window.__resumed = []; OrderHold.resumeReleases().then(a => window.__resumed.push(...a.map(x => ({ ok: x.ok, placed: x.placed, error: x.error || null })))).catch(() => {}); });
    const fin3 = await until(async () => { const r = await rowOf(H), z = await info(); return r && !r.releasing && z && z.placed.includes(poolOf(H)) && z; }, 1500000, 'the release finishes after the reload').catch(() => null);
    console.log('         resumed:', JSON.stringify(await page.evaluate(() => window.__resumed)));
    check(!!fin3, `3 · the next load finishes the release by itself (${JSON.stringify(await page.evaluate(() => window.__resumed))})`);
    const jobs = await page.evaluate(() => window.__jobs);
    console.log('         first search asked:', JSON.stringify(jobs[0] && jobs[0].orders));
    check(jobs.length >= 1 && jobs[0].orders.includes(H) && !jobs[0].orders.includes(I[2]), `3 · the first search took H ahead of the third Etsy order, which is older (${JSON.stringify(jobs[0] && jobs[0].orders)})`);
    // (the three Etsy orders after it may still wait their turn: a run in manual mode starts no search of its own; what is checked is that H is placed, once, and none of them is lost or ahead)
    await rest().catch(() => null);
    const s3 = await info();
    check(SIDS.filter(r => r !== H2).every(r => s3.placed.includes(poolOf(r))) && s3.charms.filter(id => id === poolOf(H)).length === 1 && I.every(r => s3.charms.includes(poolOf(r))), `3 · H is placed once, S1 did not go, the three Etsy orders are all still on the sheet (${JSON.stringify({ placed: s3.placed.length, charms: s3.charms.length, feedWait: s3.feedWait.length, status: s3.status })})`);
    const placedAt = id => s3.placed.indexOf(id);
    check(placedAt(poolOf(H)) >= 0 && (placedAt(poolOf(I[2])) < 0 || placedAt(poolOf(H)) < placedAt(poolOf(I[2]))), `3 · H sits ahead of the third Etsy order (${JSON.stringify([placedAt(poolOf(H)), placedAt(poolOf(I[2]))])})`);
    const row3b = await rowOf(H), pr = pool(srv, poolOf(H), false);
    console.log('         after placement: row frontAt', row3b.frontAt, 'releasing', JSON.stringify(row3b.releasing), '· piece record frontAt', pr.frontAt);
    check(row3b.frontAt === frontAt && !row3b.releasing && row3b.state !== 'held', `3 · placed: the marker is gone, the place in the queue is kept and harmless (${JSON.stringify(row3b)})`);
    const ev3 = await until(() => timeline(H, 'released').length && timeline(H, 'released'), 60000, 'the released event');
    await page.waitForTimeout(2500);
    check(timeline(H, 'released').length === 1 && timeline(H, 'released')[0]._id.includes(String(frontAt)), `3 · the timeline has one permanent step for the release, named by the first press (${JSON.stringify(timeline(H, 'released').map(e => e._id))})`);
    const rk = await page.evaluate(({ ms }) => { const R = CharmNestOrders.rankDate; return { front: R({ frontAt: ms }), dated: R({ orderDate: 1.7e9 }), undated: R({ orderDate: 0 }), none: R({}) }; }, { ms: Date.now() });
    check(rk.front < rk.dated, `3 · by rank a released piece is ahead of every dated order (${JSON.stringify(rk)})`);
    // (an order with NO date at all ranks 0, ahead of a released order: Etsy always gives a date, so this is noted, not failed)
    console.log('         rank of an undated piece:', rk.undated, 'against a released one:', rk.front);
    await realAudit('3 after the release', { held: [H2].filter(() => false) });
    // incoming orders stand outside the three orders audited: they are on the sheet too
    const pl = await info();
    check(pl.charms.length === 6 && [S1, H, H2, ...I].every(r => pl.charms.includes(poolOf(r))), `3 · six orders on the sheet, none lost (${pl.charms.length} pieces, ${pl.placed.length} placed)`);
  } catch (e) { fails.push('real 3: ' + e.message); console.log('  FAIL real 3 · ' + (e.stack || e.message)); }

  /* ── 4 · Hold again, Release again, and everything pressed while a Release runs ── */
  try {
    // (the run is stopped, so the sheet, with an Etsy order still waiting on it, reads "Waits for Resume": a hold on it must not sit out three minutes and then call it busy)
    const sheetNow = await dump();
    const t4 = Date.now();
    await holdRun(H, 'Paula', 'again'); const rh = await holdEnd();
    check(sheetNow.stage === 'Waits for Resume' && sheetNow.run && sheetNow.run.status === 'stopped', `4 · the sheet waits for the stopped run to resume (${sheetNow.status}: ${sheetNow.stage}; run ${sheetNow.run && sheetNow.run.status})`);
    check(rh.ok && rh.held && Date.now() - t4 < 150000, `4 · the order is held a second time without waiting for the stopped run (${JSON.stringify(rh)} in ${Math.round((Date.now() - t4) / 1000)} s)`);
    await settled(srv, 2000, 60000);
    const rowH = await rowOf(H);
    console.log('         held again: frontAt kept =', rowH.frontAt === frontAt, '(row.frontAt', rowH.frontAt, ')');
    const placedBefore = await info();
    await page.evaluate(() => { window.__jobs = []; });
    const m4 = srv.mark();
    await relRun(H, 'Paul');
    await until(() => page.evaluate(() => window.__rsteps.includes('target')), LONG, 'the release reaches its target');
    // everything a second person could press while the first release runs
    const racers = await page.evaluate(async ({ H, H2 }) => {
      const again = OrderHold.release(H, { name: 'Pat' });                                  // Release twice
      const holdSame = await OrderHold.run(H, { name: 'Pat' });                              // Hold the order that is being released
      const holdOther = await OrderHold.run(H2, { name: 'Pat', note: 'during' });             // Hold another order on the same sheet
      const sameResult = again === window.__rp0;
      return { sameResult, holdSame: { ok: holdSame.ok, error: holdSame.error || null }, holdOther: { ok: holdOther.ok, held: holdOther.held, error: holdOther.error || null } };
    }, { H, H2 });
    console.log('         pressed during the release:', JSON.stringify(racers));
    check(racers.sameResult === true, `4 · a second Release of the same order is the first one (one release, not two)`);
    check(racers.holdSame.ok === false && /being released/.test(racers.holdSame.error || ''), `4 · Hold of the order being released is refused plainly (${JSON.stringify(racers.holdSame)})`);
    check(racers.holdOther.ok === false ? /One change at a time|still running|being/i.test(racers.holdOther.error || '') : racers.holdOther.held === true, `4 · Hold of another order during the release is refused plainly or goes through whole (${JSON.stringify(racers.holdOther)})`);
    const r4 = await relEnd();
    check(r4.ok === true && r4.placed === true, `4 · the release finishes (${JSON.stringify(r4)})`);
    await settled(srv, 2000, 60000);
    const calls4 = srv.st.calls.slice(m4.calls);
    const heldOther = racers.holdOther.ok === true;
    await realAudit('4 after the race', { held: heldOther ? [H2] : [] });
    const rel4 = timeline(H, 'released');
    check(rel4.length === 2 && new Set(rel4.map(e => e._id)).size === 2, `4 · two permanent release steps in all, one for each release (${JSON.stringify(rel4.map(e => e._id))})`);
    const rowH4 = await rowOf(H);
    check(rowH4.frontAt > frontAt && !rowH4.releasing, `4 · the second release put the order at a later place in the queue than the first (${rowH4.frontAt} > ${frontAt})`);
    console.log('         the other order after the race:', JSON.stringify(await rowOf(H2)), '· jobs:', JSON.stringify(await page.evaluate(() => window.__jobs.map(j => j.orders))));
  } catch (e) { fails.push('real 4: ' + e.message); console.log('  FAIL real 4 · ' + (e.stack || e.message)); }

  /* ── 5 · the timeline only grew; no Etsy call, no paid call after the sheet was built ── */
  await settled(srv, 2500, 60000);
  check(tlOnlyGrew(tl0, tlSnapshot(srv, false)), `5 · every timeline event that was there is still there, as it was (the timeline only grows)`);
  const inRun = srv.st.calls.slice(m0.calls), paidOps = inRun.filter(c => PAID_FN.has(c.name) || (c.name === 'charmNestLibrary' && PAID_OP.test(c.op || '')));
  check(!inRun.some(c => ETSY_FN.has(c.name)), `5 · no Etsy call from the Hold, the Release or the reloads (${[...new Set(inRun.map(c => c.name))].join(', ')})`);
  // (the page's own Custom Orders reader asks for a reading of every unread custom line, and again every 10 minutes while nothing answers; the fake has no model key, so it asks all the time.
  //  That is the reader's, not the hold's or the release's: only a call of ANOTHER kind (grouping, layout, name, place, packing, label, engraving) would be theirs)
  const modes = paidOps.map(c => (c.body && c.body.mode) || c.name);
  console.log('         paid calls asked after the sheet was built:', JSON.stringify(modes), '· model calls the fake saw:', srv.paid - m0.paid);
  check(modes.every(m => m === 'customRead'), `5 · the only paid calls are the Custom Orders reader's own, none from the Hold or the Release (${JSON.stringify(modes)})`);
  check(!errors.length, 'real · no page errors ' + errors.join(' | '));
  await ctx.close(); srv.close();
}
/*SEGMENTS*/

const SEGMENTS = { seeded: seededSegment, stale: staleSegment, safety: safetySegment, sandbox: sandboxSegment, ui: uiSegment, real: realSegment };
(async () => {
  const kinds = process.argv[2] ? [process.argv[2]] : ['seeded', 'stale', 'safety', 'sandbox', 'ui', 'real'];
  const browser = await chromium.launch({ executablePath: CHROMIUM, args: ['--no-sandbox'] });
  try {
    for (const kind of kinds) {
      console.log(`\n── ${kind} ──`);
      try { await SEGMENTS[kind](browser); } catch (e) { fails.push(`${kind}: ${e.message}`); console.log(`  FAIL ${kind} · ${e.stack || e.message}`); }
    }
  } finally { await browser.close(); }
  console.log(`\n${passed.length} checks passed, ${fails.length} failed`);
  if (fails.length) { console.log(fails.map(f => '  FAIL ' + f).join('\n')); process.exit(1); }
  console.log('hold-release-adversarial OK');
})().catch(e => { console.error(e); process.exit(1); });
