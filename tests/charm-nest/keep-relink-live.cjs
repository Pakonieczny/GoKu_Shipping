// WRONGCHARM2 · the designs window's "Save to the charm repository and link this listing" (Paul, 20 Oct 2026): "When a user selects this toggle, their charm repository and matched SKU to that particular
// Etsy listing, and any charms already nested on a given sheet, and all thumbnails that represent the matched vector file must be updated in real time, and there must be some sort of pop-up with a
// progress bar that clearly identifies each place that the charm is being updated so the user understands what just happened." Real headless Chromium over the repo's stand-in site (bridge-server.cjs:
// the REAL charmNestLibrary handler on an in-memory Firestore and Storage; every request off the loopback is aborted). The save engine (charm-nest-keep.js), the keeper and the bridge are the REAL ones;
// the Etsy step answers through a stand-in (nothing leaves the machine); the nest (the solver) is the stand-in of wrong-charm-browser.cjs; the saved design's drawing is put in the page's pool cache
// when the library entry arrives (as a download would).
//   A · NO MATCH (KEEPOWNSKU, Paul 20 Oct 2026: "use the existing Etsy listing/order SKU and save/match that along with the newly uploaded charm vector to the Charm repo")
//   C 4196640001  the custom order of listing 1800004821, SKU “Test_@” (no library charm): a design dropped in its window (gold), the switch ON → saved as TEST_@ (the order's own SKU, upper case,
//                 nothing invented), Etsy already carries it
//   W 4196640002  a later order of the same listing and SKU, waiting for a charm (on no sheet) → reads TEST_@, is made from it and is appended to the draft GF Sheet 1 (INPLACE: nothing on it moves)
//   F 4196640091  another listing, on GF Sheet 1 → untouched
//   B · ALREADY MATCHED TO X (KEEPOVERRIDE, Paul 20 Oct 2026: "the system must remove the SKU from the offending charm and attach it to the newly uploaded charm in the Charm repo. This must also
//       trigger the immediate updating of all related thumbnails and Nested charms on sheets")
//   X = the library charm test/OAK.ai under OAK_LEAF and OAK_LEAF_GF; listing 1800007733 · SKU OAK_LEAF reads it
//   C2 4196640101 the custom order: its design dropped, the switch ON → OAK_LEAF is removed from X (X keeps OAK_LEAF_GF), the new design holds OAK_LEAF, an audit record, the timeline
//   W2 4196640102 the same listing + SKU, NESTED on the uncut GF Sheet 1 with X's piece → its piece is made again from the new design, in its old spot (the relink's own placement), GF Sheet 1 saved, pictures
//   The pop-up: "Removed SKU OAK_LEAF from charm OAK_LEAF_GF in the charm repository", "Saved the new design as OAK_LEAF", "Linked listing … · SKU OAK_LEAF to the new design", W2's line, the sheet,
//   the pictures, "Etsy already carries SKU OAK_LEAF"; the Library tab: X's card lists OAK_LEAF_GF alone, the new design's card OAK_LEAF.
//   S (sandbox, world B): the same toggle is rehearsed with the same lines in "would" words; "Rehearsal complete. Nothing was saved.", no writing op
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
const LID = '1800004821', SKU = 'Test_@', RUN = 'run-keeprelink-1', SHEET = 'kr-gold-1';
const C = '4196640001', W = '4196640002', F = '4196640091';
const LID2 = '1800007733', S2 = 'OAK_LEAF', C2 = '4196640101', W2 = '4196640102', XPATH = 'test/OAK.ai';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const LEAF_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 2, 70, 1, 10, 0, 20, 0, 42, -0.62, 10, 13, 20, 0, 42, -0.62,
  0, 'CIRCLE', 8, 'CUT', 10, 3.2, 20, 0, 40, 0.9, 0, 'ENDSEC', 0, 'EOF');
const order = (rid, listing, sku, title, age) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY - age, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: listing, sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const FILL = order(F, '1700000291', 'GFFILL_9', 'GFFILL_9 charm', 700);
const XE = { aiPath: XPATH, widthPt: 28.35, heightPt: 28.35, holes: 0, charmHash: 'h-OAKX', masterHash: 'e'.repeat(64) };
const WORLD = {
  own: { custom: C, orders: [order(C, LID, SKU, 'Custom Charm - Test, Personalized', 900), order(W, LID, SKU, 'Custom Charm - Test, Personalized', 800), FILL], index: [], placed: [] },
  override: { custom: C2, orders: [order(C2, LID2, S2, 'Oak Leaf Charm, Personalized', 900), order(W2, LID2, S2, 'Oak Leaf Charm, Personalized', 800), FILL],
    index: [[S2, Object.assign({ sku: S2 }, XE)], ['OAK_LEAF_GF', Object.assign({ sku: 'OAK_LEAF_GF' }, XE)]], placed: [{ rid: W2, sku: S2, aiPath: XPATH, charmHash: 'h-OAKX', at: [46, 16] }] }
};
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const errors = [];
  const mk = async (sandbox, scen) => {
    const srv = await start({ receipts: [] }), st = srv.st;
    st.put('Charm_Master_Index', 'GFFILL_9', { sku: 'GFFILL_9', aiPath: 'test/GFFILL_9.ai', widthPt: 28.35, heightPt: 28.35, holes: 0 }, false);
    for (const [k, e] of scen.index) st.put('Charm_Master_Index', k, e, false);
    // GF Sheet 1 (a draft, not cut): F's piece, and the pieces of the scenario's nested orders (W2 with X's drawing)
    const placed = [{ rid: F, sku: 'GFFILL_9', aiPath: 'test/GFFILL_9.ai', charmHash: 'h-GFFILL_9', at: [16, 16] }].concat(scen.placed), pid = p => `${p.rid}_${p.rid}1_1`;
    st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', draft: true, charms: placed.map((p, i) => ({ id: SHEET + '-c' + i, poolId: pid(p), order: p.rid, sku: p.sku })), placements: placed.map((p, i) => ({ id: SHEET + '-c' + i, cxPt: p.at[0], cyPt: p.at[1], angle: 0 })), poolIds: placed.map(pid), sheetIndex: 1, fileBase: 'GF Sheet 1', runId: RUN }, false);
    for (const p of placed) st.put('Charm_Pool', pid(p), { poolId: pid(p), orderId: p.rid, transactionId: p.rid + '1', lineKey: `${p.rid}_${p.rid}1`, sku: p.sku, aiPath: p.aiPath, charmHash: p.charmHash, copy: 1, quantity: 1, sheetId: SHEET, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 }, false);
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
    await page.evaluate(async ({ ORDERS, RUN, SHEET, C, F, placed, XPATH }) => {
      window.__ops = []; const real = window.api;
      window.api = async function (fn, body, o) { try { window.__ops.push(body && body.op); } catch (_) {} if (body && body.op === 'customEtsySkuWrite') return { ok: true, state: 'linked' }; return real.apply(this, arguments); };   // (Etsy: a stand-in answer)
      await Orders.loadMaps(true);
      CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
      const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2;
      const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
      const mkCharm = (id, sku, inner, extra) => { const outline = sq(0, 0, D, D); return Object.assign({ id, name: id, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline, members: [outline, sq(D * inner, D * inner, D * (1 - inner), D * (1 - inner))], rMm: R, w: 8, h: 8, scale: 1, bits: new Uint8Array(64).fill(1), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, hash: 'h-' + sku }, extra || {}); };
      window.__mkCharm = mkCharm;
      B.pool.sources.set('test/GFFILL_9.ai', { id: 'pool:GFFILL_9', pool: true, name: 'GFFILL_9', sku: 'GFFILL_9', charms: [mkCharm('pool:GFFILL_9:0', 'GFFILL_9', .35, { name: 'GFFILL_9' })], usedAt: Date.now() });
      B.pool.sources.set(XPATH, { id: 'pool:OAKX', pool: true, name: 'OAK', sku: 'OAK_LEAF', charms: [mkCharm('pool:OAKX:0', 'OAK_LEAF', .42, { name: 'OAK', hash: 'h-OAKX' })], usedAt: Date.now() });   // (X's drawing)
      // (the saved design's drawing, when its library entry arrives: the stand-in for reading its file back)
      const set = B.master.entries.set.bind(B.master.entries);
      B.master.entries.set = (k, e) => { if (e && e.aiPath && /\/custom\//.test(e.aiPath) && !B.pool.sources.has(e.aiPath)) B.pool.sources.set(e.aiPath, { id: 'pool:' + k, pool: true, name: k, sku: k, charms: [mkCharm('pool:' + k + ':0', k, .2, { name: k })], usedAt: Date.now() }); return set(k, e); };
      const pr = CN.S.sheets.gold; pr.pages.length = 1; const pg = pr.pages[0];
      const pid = p => `${p.rid}_${p.rid}1_1`;
      Object.assign(pg, { charms: placed.map((p, i) => mkCharm(SHEET + '-c' + i, p.sku, p.rid === F ? .35 : .42, { name: p.rid + ' · ' + p.sku, hash: p.charmHash, poolId: pid(p), order: p.rid, lineKey: `${p.rid}_${p.rid}1`, orderInfo: { receiptId: p.rid, transactionId: p.rid + '1', sku: p.sku, copy: 1, quantity: 1 } })), placements: placed.map((p, i) => ({ id: SHEET + '-c' + i, cxPt: p.at[0], cyPt: p.at[1], angle: 0 })),
        status: 'complete', sheetId: SHEET, fileBase: 'GF Sheet 1', sheetIndex: 1, page: 1, runId: RUN, draft: true, setId: null, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'releaseFull']) delete pg[k];
      CN.renderCard(pg);
      B.pool.rows.clear(); for (const p of placed) B.pool.rows.set(pid(p), { poolId: pid(p), orderId: p.rid, transactionId: p.rid + '1', lineKey: `${p.rid}_${p.rid}1`, sku: p.sku, aiPath: p.aiPath, charmHash: p.charmHash, copy: 1, quantity: 1, sheetId: SHEET, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 });
      B.orders.rows.length = 0; B.orders.byKey.clear();
      for (const o of ORDERS) for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line), mine = placed.find(p => p.rid === o.receiptId); const row = { key, order: o, line, arrivedAt: Date.now() - 3600000, spec: null, problems: [], state: mine ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: mine ? [pid(mine)] : [], engrave: null, material: mine ? 'gold' : null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      B.maps.customRead = Object.assign({}, B.maps.customRead || {}, Object.fromEntries(ORDERS.filter(o => o.receiptId === C).flatMap(o => o.lines.map(l => [CharmNestOrders.lineKey(o, l), { kind: 'custom', confidence: 0.96, summary: 'the buyer\'s own design' }]))));
      B.sets.clear();
      B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: ORDERS.map(o => o.receiptId) };
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { ORDERS: scen.orders, RUN, SHEET, C: scen.custom, F, placed, XPATH });
    return { srv, st, ctx, page };
  };
  const dropAndSave = async (page, rid, viaReview) => {
    if (viaReview) {
      // (a line matched to a library charm is no custom card: it is sent to Review and the design is dropped on its Review row, REVDROP: "overwrite the currently matched charm with a new charm")
      await page.evaluate(async rid => { const m = await CharmNestReviewMark.mark(rid, { by: 'Paul' }); if (!m.ok) throw new Error('not marked: ' + JSON.stringify(m)); Review.syncOrderItems(); CN.setMode('review'); Review.render(); }, rid);
      await page.waitForFunction(rid => !!document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"]`), rid, { timeout: 20000 });
      await page.evaluate(async ({ rid, dxf }) => {
        const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf'), dt = new DataTransfer(); dt.items.add(new File([r.bytes], 'Oak_Leaf_new.ai'));
        const n = document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"]`), t = n.querySelector('.engravingIdentity') || n;
        for (const ty of ['dragenter', 'dragover', 'drop']) t.dispatchEvent(new DragEvent(ty, { bubbles: true, cancelable: true, dataTransfer: dt }));
      }, { rid, dxf: LEAF_DXF });
      await page.waitForFunction(() => !!document.querySelector('#cuAsk [data-ask-yes]') || !!document.querySelector('#cuDlg[open]'), null, { timeout: 15000 });
      await page.evaluate(() => { const b = document.querySelector('#cuAsk [data-ask-yes]'); if (b) b.click(); });
    } else {
    await page.click('#reviewView .egTab[data-k="customOrder"]').catch(() => {});
    await page.evaluate(async ({ rid, dxf }) => {
      const key = [...B.orders.byKey.keys()].find(k => k.startsWith(rid + '_')), it = Review.customItemFor(key);
      if (!it) throw new Error('no custom card for ' + rid);
      const r = await CharmNestDXF.toPdf(new TextEncoder().encode(dxf), 'x.dxf');
      CustomSheet.add(it, [new File([r.bytes], 'Acorn_Leaf.ai')]);
    }, { rid, dxf: LEAF_DXF });
    }
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length >= 1, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
    await page.waitForFunction(() => { const h = document.getElementById('cuKeep'); return h && h.dataset.phase === 'off'; }, null, { timeout: 20000 });
    await sleep(400);
    await page.click('#cuKeepSwitch');
  };
  const panel = page => page.evaluate(() => { const el = document.querySelector('.rlFlow'); return el ? { title: el.querySelector('h3').textContent, done: el.dataset.done === '1', bar: el.querySelector('.rlBar').getAttribute('aria-valuenow'), sum: el.querySelector('.rlSum').textContent, ids: [...el.querySelectorAll('li')].map(li => li.dataset.id), lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').firstChild ? li.querySelector('.rlT').firstChild.textContent : '']) } : null; });
  const finished = async (page, what) => page.waitForFunction(() => { const el = document.querySelector('.rlFlow'); return el && el.dataset.done === '1'; }, null, { timeout: 90000 }).catch(async e => { throw new Error(what + ': the pop-up did not finish: ' + JSON.stringify(await panel(page)) + ' · keep: ' + await page.evaluate(() => (document.getElementById('cuKeep') || {}).dataset && document.getElementById('cuKeep').dataset.phase)); });
  try {
    /* ── A · no match: the order's own Etsy SKU ── */
    {
      const { st, ctx, page } = await mk(false, WORLD.own);
      const w0 = st.calls.length, sku = 'TEST_@';
      await dropAndSave(page, C);
      await finished(page, 'A');
      const fl = await panel(page), has = (s, re) => fl.lines.some(([x, t]) => x === s && re.test(t)), esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const e = st.doc('Charm_Master_Index', sku);
      assert(e && e.hashSource === 'custom-save' && /\/custom\/TEST__k[0-9a-z]+\.ai$/.test(e.aiPath), 'the design is saved under the order\'s own SKU TEST_@ (nothing invented): ' + JSON.stringify(e));
      assert(!st.list('Charm_Master_Index').some(x => /^TEST_(CHARM|\d)|_4821$/.test(x._id)), 'no new name like TEST_CHARM_4821 was made: ' + st.list('Charm_Master_Index').map(x => x._id).join(','));
      assert(new RegExp('Save to the charm repository · ' + esc(sku)).test(fl.title) && fl.bar === '100', 'the pop-up names the save and its bar is full: ' + JSON.stringify(fl));
      assert(has('done', new RegExp('^Saved ' + esc(sku) + ' to the charm repository$')), 'the repository line: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp('^Linked listing ' + LID + ' · SKU ' + esc(sku) + ' to the new design$')), 'the link line: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp(`^Order ${W}: now reads ${esc(sku)} and goes onto GF Sheet 1`)), 'W\'s line: made from the saved design onto GF Sheet 1: ' + JSON.stringify(fl.lines));
      assert(has('done', /^GF Sheet 1 files saved \(the new piece added; nothing on it moved\)/) && has('done', /^Thumbnails updated/), 'the sheet and the pictures: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp('^Etsy already carries SKU ' + esc(sku) + '$')), 'the Etsy line: the listing carries the SKU already: ' + JSON.stringify(fl.lines));
      assert(!fl.lines.some(([s]) => s === 'failed') && !fl.lines.some(([, t]) => new RegExp(`^Order ${C}:`).test(t)) && /Saved and linked: 1 other order updated, 1 sheet saved \(none nested again\)/.test(fl.sum), 'nothing failed, the window\'s own order is not told; the summary: ' + fl.sum);
      pass('no match: the design is saved under the order\'s own SKU TEST_@; the pop-up says the save, the link, W onto GF Sheet 1, the sheet, the pictures and "Etsy already carries SKU TEST_@"');
      const wNow = await page.evaluate(({ W, sku }) => { const r = B.orders.rows.find(x => x.order.receiptId === W); const id = r.poolIds[0], sh = id && Pool.sheetOf(id), c = sh && sh.charms.find(x => x.poolId === id); return { sku: r.spec.designSku, src: r.spec.skuSource, id, sheet: sh && sh.sheetId, placed: !!(c && sh.placements.some(p => p.id === c.id)), csku: c && c.orderInfo && c.orderInfo.sku, key: PieceMedia.vectorKey(r), later: CharmNestOrders.resolveSku({ listingId: r.line.listingId, sku: 'Test_@' }, B.maps.aliases, k => Master.entryFor(k)) }; }, { W, sku });
      assert.deepEqual([wNow.sku, wNow.sheet, wNow.placed, wNow.csku], [sku, SHEET, true, sku], 'W reads TEST_@ and its piece of it is placed on GF Sheet 1: ' + JSON.stringify(wNow));
      assert.deepEqual(wNow.later, { sku, source: 'transaction' }, 'a later order of the listing carrying Test_@ reads TEST_@ directly: ' + JSON.stringify(wNow.later));
      assert(wNow.key.includes('|p:' + sku + '#'), 'W\'s picture key is the saved design\'s piece: ' + wNow.key);
      assert((st.doc('Charm_Pool', wNow.id) || {}).sku === sku, 'W\'s piece record is TEST_@');
      assert(st.calls.slice(w0).some(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === SHEET && JSON.stringify(c.body.sheet.charms || []).includes(wNow.id)), 'GF Sheet 1 was saved again with W');
      assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).includes(`${F}_${F}1_1`)).length, 0, 'F\'s piece is untouched');
      pass('W (a later order of the listing, TEST_@) is made from the saved design onto GF Sheet 1; a later order resolves to TEST_@ directly; F untouched');
      await page.click('.rlFlow [data-rl=close]');
      assert.equal(await page.evaluate(() => document.querySelectorAll('.rlFlow').length), 0, 'Close puts the pop-up away');
      await ctx.close();
    }
    /* ── B · already matched to X: the toggle wins ── */
    {
      const { st, ctx, page } = await mk(false, WORLD.override);
      const w0 = st.calls.length, sku = S2, wid = `${W2}_${W2}1_1`;
      await dropAndSave(page, C2, true);
      await finished(page, 'B');
      const fl = await panel(page), has = (s, re) => fl.lines.some(([x, t]) => x === s && re.test(t));
      const e = st.doc('Charm_Master_Index', sku), xg = st.doc('Charm_Master_Index', 'OAK_LEAF_GF');
      assert(e && e.hashSource === 'custom-save' && /\/custom\/OAK_LEAF__k[0-9a-z]+\.ai$/.test(e.aiPath) && e.charmHash !== 'h-OAKX' && e.override && JSON.stringify(e.override.from.skus) === '["OAK_LEAF_GF"]', 'OAK_LEAF is the new design, taken from X: ' + JSON.stringify(e));
      assert(xg && xg.aiPath === XPATH && xg.charmHash === 'h-OAKX', 'X keeps OAK_LEAF_GF and its drawing');
      const au = st.list('Charm_Keep_Overrides');
      assert(au.length === 1 && au[0].rid === C2 && au[0].sku === sku && au[0].removed && au[0].removed.entry.aiPath === XPATH && au[0].removed.entry.charmHash === 'h-OAKX' && au[0].by === 'Paul', 'ONE audit record: who, which order, the whole old entry: ' + JSON.stringify(au).slice(0, 300));
      assert(/Save to the charm repository · OAK_LEAF/.test(fl.title) && fl.bar === '100', 'the pop-up: ' + JSON.stringify(fl));
      assert(has('done', /^Removed SKU OAK_LEAF from charm OAK_LEAF_GF in the charm repository$/), 'the removal line: ' + JSON.stringify(fl.lines));
      assert(has('done', /^Saved the new design as OAK_LEAF$/), 'the save line: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp('^Linked listing ' + LID2 + ' · SKU OAK_LEAF to the new design$')), 'the link line: ' + JSON.stringify(fl.lines));
      assert(has('done', new RegExp(`^Order ${W2}: new piece placed in the old spot on GF Sheet 1`)), 'W2\'s line: its nested piece remade from the new design, in its old spot (the relink\'s placement step): ' + JSON.stringify(fl.lines));
      assert(has('done', /^GF Sheet 1 files saved/) && has('done', /^Thumbnails updated/) && has('done', /^Etsy already carries SKU OAK_LEAF$/), 'the sheet, the pictures, Etsy: ' + JSON.stringify(fl.lines));
      assert(!fl.lines.some(([s]) => s === 'failed') && /Saved and linked: 1 other order updated, 1 sheet saved \(none nested again\)/.test(fl.sum), 'nothing failed; the summary: ' + fl.sum + ' · ' + JSON.stringify(fl.lines));
      const order = ['remove', 'repo', 'link'].map(id => fl.ids.indexOf(id));
      assert(order[0] === 0 && order[1] === 1 && order[2] === 2, 'the lines come in order: removed, saved, linked: ' + JSON.stringify(fl.ids));
      pass('already matched: OAK_LEAF is removed from X (X keeps OAK_LEAF_GF), the new design holds it, one audit record; the pop-up says removed, saved, linked, Etsy, W2 in its old spot, the sheet, the pictures');
      const wNow = await page.evaluate(({ W2, sku }) => { const r = B.orders.rows.find(x => x.order.receiptId === W2); const id = r.poolIds[0], sh = id && Pool.sheetOf(id), c = sh && sh.charms.find(x => x.poolId === id), p = B.pool.rows.get(id), pl = c && sh.placements.find(q => q.id === c.id); return { sku: r.spec.designSku, id, sheet: sh && sh.sheetId, placed: !!pl, at: pl ? [Math.round(pl.cxPt), Math.round(pl.cyPt)] : null, aiPath: p && p.aiPath, hash: c && (c.hash || c.charmHash), key: PieceMedia.vectorKey(r) }; }, { W2, sku });
      assert(wNow.sku === sku && wNow.sheet === SHEET && wNow.placed && /OAK_LEAF__k/.test(wNow.aiPath) && wNow.hash === 'h-OAK_LEAF', 'W2\'s piece is made again from the new design and placed on GF Sheet 1: ' + JSON.stringify(wNow).slice(0, 300));
      assert(wNow.key.includes('|p:OAK_LEAF#h-OAK_LEAF') && !wNow.key.includes('h-OAKX'), 'W2\'s picture key is the new design\'s piece: ' + wNow.key);
      assert(/OAK_LEAF__k/.test((st.doc('Charm_Pool', wid) || {}).aiPath || ''), 'W2\'s piece record is cut from the new design\'s file');
      assert.deepEqual(wNow.at, [46, 16], 'W2\'s new piece takes the old piece\'s spot (the relink\'s own placement): ' + JSON.stringify(wNow.at));
      assert(st.calls.slice(w0).some(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === SHEET && JSON.stringify(c.body.sheet.charms || []).includes(wid)), 'GF Sheet 1 was saved again');
      assert.equal(st.calls.slice(w0).filter(c => /^(poolPut|poolUpdate)$/.test(String(c.op)) && JSON.stringify(c.body).includes(`${F}_${F}1_1`)).length, 0, 'F\'s piece is untouched');
      pass('W2 (nested on the uncut GF Sheet 1 with X\'s piece) is made again from the new design by the relink, placed in its old spot, GF Sheet 1 saved, its pictures the new piece; F untouched');
      // the Library tab: X's card no longer lists OAK_LEAF, the new design's card does
      await page.evaluate(() => { try { CustomSheet.shut(); } catch (_) {} CN.setMode('master'); });
      await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length >= 2, null, { timeout: 20000 });
      const cards = await page.evaluate(() => [...document.querySelectorAll('#mGrid .skuTile .sku')].map(s => s.title));
      assert(cards.includes('OAK_LEAF_GF') && cards.includes('OAK_LEAF') && !cards.some(t => /OAK_LEAF, OAK_LEAF_GF|OAK_LEAF_GF, OAK_LEAF/.test(t)), 'the Library: X\'s card lists OAK_LEAF_GF alone, the new design\'s card lists OAK_LEAF: ' + JSON.stringify(cards));
      pass('the Library tab: X\'s card lists OAK_LEAF_GF only, the new design\'s card OAK_LEAF');
      await ctx.close();
    }
    /* ── S · the sandbox (world B): rehearsed with the same lines, nothing written ── */
    {
      const { ctx, page, st } = await mk(true, WORLD.override);
      const before = JSON.stringify(st.list('Charm_Master_Index'));
      await dropAndSave(page, C2, true);
      await finished(page, 'S');
      const fl = await panel(page), has = (s, re) => fl.lines.some(([x, t]) => x === s && re.test(t));
      assert(/^Rehearsal · Save to the charm repository/.test(fl.title) && fl.sum === 'Rehearsal complete. Nothing was saved.', 'the rehearsal pop-up: ' + JSON.stringify(fl));
      assert(has('would', /^Would remove SKU OAK_LEAF from charm OAK_LEAF_GF in the charm repository$/) && has('would', /^Would save the new design as OAK_LEAF$/) && has('would', new RegExp('^Would link listing ' + LID2 + ' · SKU OAK_LEAF to the new design$')), 'the same first lines, in would words: ' + JSON.stringify(fl.lines));
      assert(has('would', new RegExp(`^Order ${W2}: would take its piece off GF Sheet 1, make it from OAK_LEAF`)) && has('would', /^Etsy already carries SKU OAK_LEAF: nothing would be sent$/), 'W2 and Etsy, rehearsed: ' + JSON.stringify(fl.lines));
      const ops = await page.evaluate(() => window.__ops.filter(Boolean));
      assert(!ops.some(o => /customSaveCommit|aliasPut|poolPut|poolUpdate|putSheet|customEtsySkuWrite|masterPatch/.test(o)), 'the rehearsal writes nothing: ' + ops.join(','));
      assert(JSON.stringify(st.list('Charm_Master_Index')) === before && !st.list('Charm_Keep_Overrides').length, 'the repository is as it was, no audit record');
      pass('in the sandbox the override is rehearsed with the same lines ("Would remove SKU OAK_LEAF from charm OAK_LEAF_GF …"), "Rehearsal complete. Nothing was saved.", no write');
      await ctx.close();
    }
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); }
  console.log(`keep-relink-live: ${n} steps passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
