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
      charms.push({ id, poolId, order: rid }); poolIds.push(poolId);
      if (placed !== false) placements.push({ id, cxPt: 30 + col * 40, cyPt: 30 + row * 40, angle: 0 });
      docs.pool.push({ poolId, orderId: rid, sheetId: sh.id, state: 'placed', material: sh.metal });
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
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
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
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|forced failure|the save failed \(test\)/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  await page.goto(`${o.origin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && Orders.takeOffGone && window.SheetWin && SheetWin.holdKit && window.OrderHold && window.HoldUI && window.CancelUI && window.OrderHoldFx && window.CharmNestSolver && window.startNest && (!window.__nestStub || true), null, { timeout: 60000 });
  if (o.stub !== false) await page.waitForFunction(() => window.startNest.__stub, null, { timeout: 20000 });
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
        window.B.pool.rows.set(poolId, { poolId, orderId: rid, sheetId: sh.id, state: 'placed', material: sh.metal });
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
        line: { transactionId: String(5000000000 + ln.tx), listingId: '', sku: 'TEST-' + ln.tx, title: 'Test charm ' + ln.tx, quantity: ln.n, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + ln.tx, quantity: ln.n, material: ln.metal, problems: [] },
        problems: [], state: 'pooled', reason: null, poolIds, engrave: null, material: ln.metal, arrivedAt: Date.now() - 7200000 });
    }
    window.B.orders.rows = rows; window.B.orders.byKey = new Map(rows.map(r => [r.key, r]));
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
    const fine = gone ? where.length === 0 && p.state === 'abandoned' && !!p.removedBy : where.length === 1 ? (!held && p.sheetId === where[0]) : (where.length === 0 && held && !p.sheetId);
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
async function staleSegment() {}
async function safetySegment() {}
async function sandboxSegment() {}
async function uiSegment() {}
async function realSegment() {}
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
