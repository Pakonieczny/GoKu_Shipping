// RELONLY (Paul, 25 Nov 2026, order 4247021823, listing 559716663 "Add on an Extender to your Jewelry (w shipping)", design ADD_6663): never on a sheet unless a person chooses Nest it at Release Hold.
//   N  charm-nest-orders.js (the page's and the cloud's one copy): ADD_6663 is held as a blocked SKU (entryBlock / blockFor) with no block in the master; a person's release (blockOverride) lets it through;
//      any other design is untouched
//   P  the REAL page (_engraveRealWorld.cjs, the real intake and pool): an order of ADD_6663 is held, no piece made, while another order's piece is made; its Review card has Release Hold and no
//      "Unblock this SKU"; Release Hold asks Nest it / Skip it; Skip: skipped, no piece; Nest (another order): its piece is made (pooled), the release in the person's name
// Offline only. Made-up names.   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/release-only-extender.cjs
'use strict';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
let failed = 0, passed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1600))); if (cond) passed++; else failed++; };

const O = require(path.join(ROOT, 'charm-nest-orders.js'));
const e = { sku: 'ADD_6663', blocked: null };
ok('N ADD_6663 is held as a blocked SKU with no block in the master', /only when Nest it is chosen at Release Hold/.test(O.entryBlock(e)) && !!O.blockFor(e, 'Add_6663', null));
ok('N ... a person\'s release (blockOverride) lets it through', O.blockFor(e, 'ADD_6663', { sku: 'ADD_6663', by: 'Test Operator', at: 1 }) === '');
ok('N any other design is untouched (no block, a genuine block, a flip-only block)', O.entryBlock({ sku: 'COMET_9427' }) === '' && O.entryBlock({ sku: 'X', blocked: 'open outline', open: true }) === 'open outline' && O.entryBlock({ sku: 'Y', blocked: 'flip check failed — x' }) === '');
const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
ok('N cache tokens: the orders file carries -ro1, the bridge -ro1', /charm-nest-orders\.js\?v=[^"]*-ro1/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-ro1/.test(html));

(async () => {
  const Base = require('./_orderFactsWorld.cjs'), Rw = require('./_engraveRealWorld.cjs');
  const W = await Rw.world({ viewport: { width: 1600, height: 950 }, reducedMotion: 'reduce' });
  if (!W) { console.log('release-only-extender: no playwright-core, the page part is not run'); return done(); }
  const { page, srv } = W, st = srv.st; if (process.env.DBG) page.on('console', m => { if (/^DBG/.test(m.text())) console.log('PAGE', m.text()); });
  const V = (name, value) => ({ name, value });
  const fx = (n, sku, listing, title) => ({ rid: '42940000' + String(n).padStart(2, '0'), sku, listing, title, buyer: 'Name ' + n, placed: { day: 0, h: 9, m: n }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] });
  const X1 = fx(1, 'Add_6663', '559716663', 'Add on an Extender to your Jewelry (w shipping)'), X2 = fx(2, 'Add_6663', '559716663', 'Add on an Extender to your Jewelry (w shipping)'), C = fx(3, 'COMET_9427', '1894000003', 'Comet Necklace');
  const keyOf = f => `${f.rid}_${f.rid}1`;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  const rowOf = f => page.evaluate(k => { const r = B.orders.byKey.get(k); return r ? { state: r.state, pool: (r.poolIds || []).length, held: Orders.heldNow(r), probs: (r.problems || []).map(p => p.kind), reason: r.reason || null, ov: r.blockOverride || null } : null; }, keyOf(f));
  const dlg = () => page.evaluate(() => { const d = [...document.querySelectorAll('dialog.naDlg')].find(x => x.open); return d ? { rid: d.dataset.rid, text: d.innerText.replace(/\s+/g, ' ').slice(0, 200) } : null; });
  const review = () => page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
  const card = f => `#rvList .reviewListRow[data-rid="${f.rid}"]`;
  try {
    // the design ADD_6663 in the library (the comet's drawing stands in for the extender's tag), with no block in the master
    const comet = st.doc('Charm_Master_Index', 'COMET_9427');
    st.put('Charm_Master_Index', 'ADD_6663', Object.assign({}, comet, { sku: 'ADD_6663', blocked: null, open: false, conflict: null }), false);
    await page.evaluate(async () => { await Master.load({ force: true, fresh: true }); });
    ok('P the library holds ADD_6663, not blocked in the master', await page.evaluate(() => { const e = Master.entryFor('ADD_6663'); return !!e && !e.blocked; }));
    await page.evaluate(() => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
      B.run = { runId: 'run-2026-11-25-relonly', day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    });
    await page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const f of fixtures) { const order = orderOf(f, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: [X1, X2, C], orderSrc: Base.orderOf.toString() });
    await page.evaluate(async () => { await Pool.addAll(B.run); });
    const x1 = await rowOf(X1), x2 = await rowOf(X2), c = await rowOf(C);
    ok('P an order of ADD_6663 is held, no piece made (both orders)', [x1, x2].every(r => r && r.pool === 0 && r.state !== 'pooled' && r.probs.includes('blockedSku')), { x1, x2 });
    ok('P ... another order\'s piece is made as always', c && c.state === 'pooled' && c.pool > 0, c);

    await review();
    const cb = await until(() => page.evaluate(sel => { const n = document.querySelector(sel); return n ? { rel: n.querySelectorAll('[data-rel-hold]').length, unblock: n.querySelectorAll('[data-rv-unblock],[data-a=unblock]').length, text: n.innerText.replace(/\s+/g, ' ').slice(0, 300) } : null; }, card(X1)), 15000);
    ok('P its Review card has Release Hold and no "Unblock this SKU"', cb && cb.rel === 1 && cb.unblock === 0, cb);

    // Skip
    let n0 = await page.evaluate(() => window.__toasts.length);
    await page.click(`${card(X1)} [data-rel-hold]`);
    let d = await until(dlg, 8000);
    ok('P Release Hold asks Nest it / Skip it, naming ADD_6663', d && d.rid === X1.rid && /ADD_6663/.test(d.text), d);
    if (d) await page.click('dialog.naDlg[open] [data-na="skip"]');
    await until(() => page.evaluate(({ k, n0 }) => window.__toasts.length > n0 && B.orders.byKey.get(k).state === 'skipped', { k: keyOf(X1), n0 }), 30000);
    await until(() => page.evaluate(n0 => window.__toasts.slice(n0).some(t => !/Workspace could not be saved/.test(t[1]) && /no charm is made|Release hold again|not released|could not/i.test(t[1])), n0), process.env.DBG ? 60000 : 20000);
    const s1 = await rowOf(X1), said1 = await page.evaluate(n0 => window.__toasts.slice(n0), n0);
    if (process.env.DBG) console.log('DBG', JSON.stringify(await page.evaluate(rid => ({ st: OrderHold.releaseStatus && OrderHold.releaseStatus(rid), busy: !!document.querySelector('[data-rel-hold][aria-busy="true"]'), all: window.__toasts }), X1.rid)).slice(0, 2500));
    ok('P Skip: skipped, no piece, told no charm is made', s1.state === 'skipped' && s1.pool === 0 && said1.some(t => /no charm is made/.test(t[1])), { s1, said1 });

    // Nest
    await review(); n0 = await page.evaluate(() => window.__toasts.length);
    await page.click(`${card(X2)} [data-rel-hold]`);
    d = await until(dlg, 8000);
    ok('P the other order: asked too', d && d.rid === X2.rid, d);
    if (d) await page.click('dialog.naDlg[open] [data-na="nest"]');
    const s2 = await until(async () => { const r = await rowOf(X2); return r && r.state === 'pooled' && r.pool > 0 ? r : null; }, 45000) || await rowOf(X2);
    const said2 = await page.evaluate(n0 => window.__toasts.slice(n0), n0);
    ok('P Nest: its piece is made (pooled) for the next sheet, released in the person\'s name', s2.state === 'pooled' && s2.pool > 0 && s2.ov && s2.ov.by === 'Test Operator' && !s2.probs.includes('blockedSku'), { s2, said2 });
    ok('P ... no failure told', !said2.some(t => t[0] === 'bad' && !/Workspace could not be saved on this browser/.test(t[1])), said2);
  } catch (err) { ok('run', false, String(err && err.stack || err).split('\n').slice(0, 5).join(' | ')); }
  const errs = W.errors.filter(x => !/ResizeObserver/.test(x));
  ok('no page error', errs.length === 0, errs.slice(0, 3).join(' | '));
  await W.close();
  done();
})().catch(err => { console.log('FAIL release-only-extender: ' + (err && err.stack || err)); process.exit(1); });
function done() { console.log(`\nrelease-only-extender: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0); }
