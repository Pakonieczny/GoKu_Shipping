// WRONGCHARM2 · the designs window's "Save to the charm repository and link this listing" (Paul, 20 Oct 2026): "When a user selects this toggle, their charm repository and matched SKU to that particular
// Etsy listing, and any charms already nested on a given sheet, and all thumbnails that represent the matched vector file must be updated in real time, and there must be some sort of pop-up with a
// progress bar that clearly identifies each place that the charm is being updated so the user understands what just happened." Real headless Chromium over the repo's stand-in site (bridge-server.cjs:
// the REAL charmNestLibrary handler on an in-memory Firestore and Storage; every request off the loopback is aborted). The save engine (charm-nest-keep.js), the keeper and the bridge are the REAL ones;
// the Etsy step answers through a stand-in (nothing leaves the machine); the nest (the solver) is the stand-in of wrong-charm-browser.cjs; the saved design's drawing is put in the page's pool cache
// when the library entry arrives (as a download would).
//   C 4196640001  the custom order: a design dropped in its window (gold), the switch ON → saved as a new SKU, listing 1800004821 · SKU “ACORN'S LEAF” linked to it, Etsy (stand-in) linked
//   W 4196640002  the same listing and SKU, waiting for a charm (no library charm answered it, so it was in Review, on no sheet)
//                 → reads the saved SKU, is made from it and goes onto the draft GF Sheet 1, where it is appended (INPLACE, Paul 20 Oct 2026: nothing on it moves, never nested again); its Orders row picture is the saved design's piece
//   F 4196640091  another listing, on GF Sheet 1 → untouched
//   The pop-up: one line per place (the repository, the link, W onto GF Sheet 1, GF Sheet 1 files saved, the pictures, Etsy), a full bar, a summary, Close.
//   X (sandbox): the same toggle is rehearsed: the pop-up says what it WOULD do (W would read the SKU, Etsy would be queued), "Rehearsal complete. Nothing was saved.", no writing op
//   node tests/charm-nest/keep-relink-live.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000);
const LID = '1800004821', SKU = 'ACORN&#39;S LEAF', RUN = 'run-keeprelink-1', SHEET = 'kr-gold-1';
const C = '4196640001', W = '4196640002', F = '4196640091';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const LEAF_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 2, 70, 1, 10, 0, 20, 0, 42, -0.62, 10, 13, 20, 0, 42, -0.62,
  0, 'CIRCLE', 8, 'CUT', 10, 3.2, 20, 0, 40, 0.9, 0, 'ENDSEC', 0, 'EOF');
const order = (rid, listing, sku, title, age) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY - age, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: listing, sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const ORDERS = [order(C, LID, SKU, "Custom Charm - Acorn's Leaf, Personalized", 900), order(W, LID, SKU, "Custom Charm - Acorn's Leaf, Personalized", 800), order(F, '1700000291', 'GFFILL_9', 'GFFILL_9 charm', 700)];
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const errors = [];
  const mk = async sandbox => {
    const srv = await start({ receipts: [] }), st = srv.st;
    st.put('Charm_Master_Index', 'GFFILL_9', { sku: 'GFFILL_9', aiPath: 'test/GFFILL_9.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
    st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', draft: true, charms: [{ id: SHEET + '-c0', poolId: `${F}_${F}1_1`, order: F, sku: 'GFFILL_9' }], placements: [{ id: SHEET + '-c0', cxPt: 16, cyPt: 16, angle: 0 }], poolIds: [`${F}_${F}1_1`], sheetIndex: 1, fileBase: 'GF Sheet 1', runId: RUN }, false);
    st.put('Charm_Pool', `${F}_${F}1_1`, { poolId: `${F}_${F}1_1`, orderId: F, transactionId: F + '1', lineKey: `${F}_${F}1`, sku: 'GFFILL_9', copy: 1, quantity: 1, sheetId: SHEET, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 }, false);
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, timezoneId: 'UTC', locale: 'en-US' });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await ctx.addInitScript(sandbox => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off', sandbox: sandbox ? 'on' : 'off' })); localStorage.setItem('cn.employee', 'Paul'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.confirm = () => true; window.prompt = () => 'Paul'; window.alert = () => {};
      const iv = setInterval(() => {
        if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
        const stub = sh => {
          window.__nests = (window.__nests || []).concat([{ sheetId: sh.sheetId, charms: sh.charms.map(c => c.poolId), appendOnly: !!sh.appendOnly }]);
          sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve();
          setTimeout(async () => {
            try {
              sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
              const cells = []; for (let row = 0; row < 2; row++) for (let col = 0; col < 6; col++) cells.push([16 + col * 30, 16 + row * 30]);
              const free = cells.filter(([x, y]) => !sh.placements.some(p => Math.hypot(p.cxPt - x, p.cyPt - y) < 24));
              for (const c of sh.charms.filter(c => !sh.placements.some(p => p.id === c.id))) { if (!free.length) break; const [x, y] = free.shift(); sh.placements.push({ id: c.id, cxPt: x, cyPt: y, angle: 0, wPt: c.widthPt, hPt: c.heightPt }); }
              if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: true, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.map(c => c.poolId) } }, { quiet: true });
              sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.persistedDone = true; sh.verification = { ok: true };
            } catch (e) { sh.problem = e.message; sh.status = 'complete'; sh.persistedDone = true; }
            try { CN.renderCard(sh); } catch (_) {}
          }, 120);
        };
        stub.__stub = true; window.startNest = stub; /* INPLACE (Paul, 20 Oct 2026): a sheet whose replaced piece came off is written as it stands (writeAsPlaced, no nest): stood in for here as the nest is, every placement kept */ const wap = sh => { window.__writes = (window.__writes || []).concat([{ sheetId: sh.sheetId, page: sh.page, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle })) }]); sh.status = 'finishing'; sh.persistedDone = false; let done; sh.persisted = new Promise(r => { done = r; }); return new Promise(res => setTimeout(async () => { try { if (sh.sheetId) await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true }); } catch (e) { sh.problem = e.message; } sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.persistedDone = true; sh.verification = { ok: true }; done(); try { CN.renderCard(sh); } catch (_) {} res(!sh.problem); }, 120)); }; wap.__stub = true; window.writeAsPlaced = wap; clearInterval(iv);
      }, 0);
    }, !!sandbox);
    const page = await ctx.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && Review.relinkFlow && window.CustomSheet && window.CNRelinkFlow && CN.S.cloud.ok === true && window.startNest && window.startNest.__stub && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    // the page's world: the run (past its pool step), the draft GF Sheet 1 with F on it, the three orders (C read as a custom order, W waiting for a charm)
    await page.evaluate(async ({ ORDERS, RUN, SHEET, C, F }) => {
      window.__ops = []; const real = window.api;
      window.api = async function (fn, body, o) { try { window.__ops.push(body && body.op); } catch (_) {} if (body && body.op === 'customEtsySkuWrite') return { ok: true, state: 'linked' }; return real.apply(this, arguments); };   // (Etsy: a stand-in answer)
      await Orders.loadMaps(true);
      CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
      const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2;
      const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
      const mkCharm = (id, sku, inner, extra) => { const outline = sq(0, 0, D, D); return Object.assign({ id, name: id, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline, members: [outline, sq(D * inner, D * inner, D * (1 - inner), D * (1 - inner))], rMm: R, w: 8, h: 8, scale: 1, bits: new Uint8Array(64).fill(1), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, hash: 'h-' + sku }, extra || {}); };
      window.__mkCharm = mkCharm;
      B.pool.sources.set('test/GFFILL_9.ai', { id: 'pool:GFFILL_9', pool: true, name: 'GFFILL_9', sku: 'GFFILL_9', charms: [mkCharm('pool:GFFILL_9:0', 'GFFILL_9', .35, { name: 'GFFILL_9' })], usedAt: Date.now() });
      // (the saved design's drawing, when its library entry arrives: the stand-in for reading its file back)
      const set = B.master.entries.set.bind(B.master.entries);
      B.master.entries.set = (k, e) => { if (e && e.aiPath && /\/custom\//.test(e.aiPath) && !B.pool.sources.has(e.aiPath)) B.pool.sources.set(e.aiPath, { id: 'pool:' + k, pool: true, name: k, sku: k, charms: [mkCharm('pool:' + k + ':0', k, .2, { name: k })], usedAt: Date.now() }); return set(k, e); };
      const pr = CN.S.sheets.gold; pr.pages.length = 1; const pg = pr.pages[0];
      Object.assign(pg, { charms: [mkCharm(SHEET + '-c0', 'GFFILL_9', .35, { name: F + ' · GFFILL_9', poolId: `${F}_${F}1_1`, order: F, lineKey: `${F}_${F}1`, orderInfo: { receiptId: F, transactionId: F + '1', sku: 'GFFILL_9', copy: 1, quantity: 1 } })], placements: [{ id: SHEET + '-c0', cxPt: 16, cyPt: 16, angle: 0 }],
        status: 'complete', sheetId: SHEET, fileBase: 'GF Sheet 1', sheetIndex: 1, page: 1, runId: RUN, draft: true, setId: null, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'releaseFull']) delete pg[k];
      CN.renderCard(pg);
      B.pool.rows.clear(); B.pool.rows.set(`${F}_${F}1_1`, { poolId: `${F}_${F}1_1`, orderId: F, transactionId: F + '1', lineKey: `${F}_${F}1`, sku: 'GFFILL_9', copy: 1, quantity: 1, sheetId: SHEET, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 });
      B.orders.rows.length = 0; B.orders.byKey.clear();
      for (const o of ORDERS) for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line), mine = o.receiptId === F; const row = { key, order: o, line, arrivedAt: Date.now() - 3600000, spec: null, problems: [], state: mine ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: mine ? [`${F}_${F}1_1`] : [], engrave: null, material: mine ? 'gold' : null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      B.maps.customRead = Object.assign({}, B.maps.customRead || {}, Object.fromEntries(ORDERS.filter(o => o.receiptId === C).flatMap(o => o.lines.map(l => [CharmNestOrders.lineKey(o, l), { kind: 'custom', confidence: 0.96, summary: 'the buyer\'s own design' }]))));
      B.sets.clear();
      B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: ORDERS.map(o => o.receiptId) };
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { ORDERS, RUN, SHEET, C, F });
    return { srv, st, ctx, page };
  };
  const dropAndSave = async page => {
    await page.click('#reviewView .egTab[data-k="customOrder"]').catch(() => {});
    await page.evaluate(async ({ rid, dxf }) => {
      const key = [...B.orders.byKey.keys()].find(k => k.startsWith(rid + '_')), it = Review.customItemFor(key);
      if (!it) throw new Error('no custom card for ' + rid);
      const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf');
      CustomSheet.add(it, [new File([r.bytes], 'Acorn_Leaf.ai')]);
    }, { rid: C, dxf: LEAF_DXF });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length >= 1, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
    await page.waitForFunction(() => { const h = document.getElementById('cuKeep'); return h && h.dataset.phase === 'off'; }, null, { timeout: 20000 });
    await sleep(400);
    await page.click('#cuKeepSwitch');
  };
  const panel = page => page.evaluate(() => { const el = document.querySelector('.rlFlow'); return el ? { title: el.querySelector('h3').textContent, done: el.dataset.done === '1', bar: el.querySelector('.rlBar').getAttribute('aria-valuenow'), sum: el.querySelector('.rlSum').textContent, lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').textContent]) } : null; });
  try {
    /* ── the real save ── */
    {
      const { st, ctx, page } = await mk(false);
      const w0 = st.calls.length;
      await dropAndSave(page);
      await page.waitForFunction(() => { const el = document.querySelector('.rlFlow'); return el && el.dataset.done === '1'; }, null, { timeout: 90000 }).catch(async e => { throw new Error('the pop-up did not finish: ' + JSON.stringify(await panel(page)) + ' · keep: ' + await page.evaluate(() => document.getElementById('cuKeep').dataset.phase)); });
      const fl = await panel(page), has = (s, re) => fl.lines.some(([x, t]) => x === s && re.test(t));
      const sku = (st.doc('Charm_Sku_Aliases', LID) || { bySku: {} }).bySku[SKU] || (st.doc('Charm_Sku_Aliases', LID) || { bySku: {} }).bySku["ACORN'S LEAF"];
      assert(sku, 'the save linked listing ' + LID + ' · SKU ' + SKU + ': ' + JSON.stringify(st.doc('Charm_Sku_Aliases', LID)));
      assert(new RegExp('Save to the charm repository · ' + sku).test(fl.title) && fl.bar === '100', 'the pop-up names the save and its bar is full: ' + JSON.stringify(fl));
      assert(has('done', new RegExp('^Saved ' + sku + ' to the charm repository$')), 'the repository line: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp('^Linked listing ' + LID + ' · SKU “.+” to ' + sku)), 'the link line: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp(`^Order ${W}: now reads ${sku} and goes onto GF Sheet 1`)), 'W\'s line: made from the saved design onto GF Sheet 1: ' + JSON.stringify(fl.lines));
      assert(has('done', /^GF Sheet 1 files saved \(the new piece added; nothing on it moved\)/) && has('done', /^Thumbnails updated/), 'the sheet and the pictures: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp('^Etsy SKU set on listing ' + LID)), 'the Etsy line follows the keeper: ' + JSON.stringify(fl.lines));
      assert(!fl.lines.some(([s]) => s === 'failed') && !fl.lines.some(([, t]) => new RegExp(`^Order ${C}:`).test(t)) && /Saved and linked: 1 other order updated, 1 sheet saved \(none nested again\)/.test(fl.sum), 'nothing failed, the window\'s own order is not told; the summary: ' + fl.sum);
      pass('the switch ON: the pop-up says each place updated (the repository, the link, W onto GF Sheet 1, GF Sheet 1 files saved with W added and nothing on it moved (INPLACE), the pictures, Etsy), fills and sums up');
      // W: its piece of the saved design on GF Sheet 1, nested, saved; F untouched; the pictures of W are the saved design's piece
      const wNow = await page.evaluate(({ W, sku }) => { const r = B.orders.rows.find(x => x.order.receiptId === W); const id = r.poolIds[0], sh = id && Pool.sheetOf(id), c = sh && sh.charms.find(x => x.poolId === id); return { sku: r.spec.designSku, state: r.state, id, sheet: sh && sh.sheetId, placed: !!(c && sh.placements.some(p => p.id === c.id)), csku: c && c.orderInfo && c.orderInfo.sku, key: PieceMedia.vectorKey(r) }; }, { W, sku });
      assert.deepEqual([wNow.sku, wNow.sheet, wNow.placed, wNow.csku], [sku, SHEET, true, sku], 'W reads the saved SKU and its piece of it is placed on GF Sheet 1: ' + JSON.stringify(wNow));
      assert(wNow.key.includes('sku:' + sku) && wNow.key.includes('|p:' + sku + '#'), 'W\'s picture key is the saved design\'s piece (Orders row, order window, piece dots, Engrave): ' + wNow.key);
      const nests = await page.evaluate(() => window.__nests || []);
      assert(nests.some(x => x.sheetId === SHEET && x.charms.includes(wNow.id) && x.appendOnly), 'GF Sheet 1 took W append-only (nothing on it moves): ' + JSON.stringify(nests));
      assert((st.doc('Charm_Pool', wNow.id) || {}).sku === sku, 'W\'s piece record is the saved SKU');
      assert(st.calls.slice(w0).some(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === SHEET && JSON.stringify(c.body.sheet.charms || []).includes(wNow.id)), 'GF Sheet 1 was saved again with W');
      assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).includes(`${F}_${F}1_1`)).length, 0, 'F\'s piece is untouched');
      // the Orders row of W draws the saved design
      await page.evaluate(() => { try { CustomSheet.shut(); } catch (_) {} CN.setMode('orders'); Orders.render(); });
      await page.waitForFunction(({ W, sku }) => { const r = B.orders.rows.find(x => x.order.receiptId === W); const h = document.querySelector(`[data-key="${CSS.escape(r.key)}"] [data-vector]`); return !!h && h._mvRow === r && h._mvRow.spec.designSku === sku && !!h.querySelector('img'); }, { W, sku }, { timeout: 15000 });
      pass('W is made from the saved design onto GF Sheet 1 (appended, saved), F untouched; W\'s pictures are the saved design\'s piece');
      await page.click('.rlFlow [data-rl=close]');
      assert.equal(await page.evaluate(() => document.querySelectorAll('.rlFlow').length), 0, 'Close puts the pop-up away');
      await ctx.close();
    }
    /* ── the sandbox: rehearsed, nothing written ── */
    {
      const { ctx, page } = await mk(true);
      await dropAndSave(page);
      await page.waitForFunction(() => { const el = document.querySelector('.rlFlow'); return el && el.dataset.done === '1'; }, null, { timeout: 60000 }).catch(async e => { throw new Error('the rehearsal pop-up did not finish: ' + JSON.stringify(await panel(page)) + ' · keep: ' + await page.evaluate(() => document.getElementById('cuKeep').dataset.phase)); });
      const fl = await panel(page), has = (s, re) => fl.lines.some(([x, t]) => x === s && re.test(t));
      assert(/^Rehearsal · Save to the charm repository/.test(fl.title) && fl.sum === 'Rehearsal complete. Nothing was saved.', 'the rehearsal pop-up: ' + JSON.stringify(fl));
      assert(has('would', /^Would save .+ to the charm repository$/) && has('would', new RegExp('^Would link listing ' + LID)) && has('would', new RegExp(`^Order ${W}: would read `)) && has('would', new RegExp('^Would queue the Etsy SKU for listing ' + LID)), 'it says what it would do: ' + JSON.stringify(fl.lines));
      const ops = await page.evaluate(() => window.__ops.filter(Boolean));
      assert(!ops.some(o => /customSaveCommit|aliasPut|poolPut|poolUpdate|putSheet|customEtsySkuWrite/.test(o)), 'the rehearsal writes nothing: ' + ops.join(','));
      pass('in the sandbox the pop-up rehearses: what it would save, link and change, "Rehearsal complete. Nothing was saved.", no write');
      await ctx.close();
    }
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); }
  console.log(`keep-relink-live: ${n} steps passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
