// Adversarial (wave 3, area 16): SKUs and Review.
//  1 · "Use this charm" saved before 27 Sep (a listing-wide alias, no v) keeps standing in for the SKU it was saved for
//      after another variation of the same listing is answered the new way (listing + SKU): the server marked the whole
//      listing v: 2 on that answer, so the old alias silently stopped applying and its orders were asked about again.
//      Run against charmNestLibrary's aliasPut/aliasGet with a Firestore fake that merges nested maps as Firestore does.
//  2 · Review's "Show" (a card that moved to another folder or chip) and "Jump" find a card beyond the first 40: a new
//      view reset the list to its first 40 after Show had raised it, so an older card was never drawn and Show did nothing.
// No network but the loopback.
//   node tests/charm-nest/adv-skus.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome> for part 2)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');

/* ── a Firestore fake: set(…, { merge: true }) merges nested maps, as Firestore does ── */
const store = new Map(), TS = { __ts: true };
const isMap = v => v && typeof v === 'object' && !Array.isArray(v) && v !== TS;
const merge = (a, b) => { const out = Object.assign({}, a); for (const [k, v] of Object.entries(b)) out[k] = isMap(v) && isMap(out[k]) ? merge(out[k], v) : v; return out; };
function docRef(coll, id) {
  const key = coll + '/' + id;
  return { id, async get() { const d = store.get(key); return { exists: !!d, id, data: () => (d ? JSON.parse(JSON.stringify(d)) : undefined) }; },
    async set(data, o) { store.set(key, o && o.merge ? merge(store.get(key) || {}, data) : Object.assign({}, data)); } };
}
function query(coll, lim = 0, after = null) {
  return { orderBy() { return query(coll, lim, after); }, limit(n) { return query(coll, n, after); }, startAfter(v) { return query(coll, lim, v); },
    async get() { let rows = [...store.keys()].filter(k => k.startsWith(coll + '/')).sort().map(k => k.slice(coll.length + 1)).filter(id => after == null || id > after); if (lim) rows = rows.slice(0, lim);
      const docs = rows.map(id => ({ id, data: () => JSON.parse(JSON.stringify(store.get(coll + '/' + id))) })); return { size: docs.length, docs, empty: !docs.length }; },
    doc: id => docRef(coll, id) };
}
const db = { collection: c => query(c) };
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => TS, increment: n => n, delete: () => undefined }, FieldPath: { documentId: () => '__name__' } }), storage: () => ({ bucket: () => ({}) }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin; if (req === 'node-fetch') return async () => ({ ok: true, status: 202, text: async () => '' }); return realLoad.call(this, req, ...rest); };

async function part1() {
  delete process.env.EDIT_PASSCODE;
  const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
  const O = require(path.join(root, 'charm-nest-orders.js'));
  const post = body => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }).then(r => JSON.parse(r.body || '{}'));
  // listing 7001's alias as saved before 27 Sep (no v): its line's own SKU BR-GONE-09 is in no master file
  store.set('Charm_Sku_Aliases/7001', { listingId: '7001', sku: 'BR-ALS-02', by: 'Ana', title: 'Star necklace' });
  const probe = await post({ op: 'aliasGet' }); assert(probe.aliases && probe.aliases['7001'], 'the fake holds the legacy alias under the collection aliasGet reads: ' + JSON.stringify(probe).slice(0, 200));
  const lib0 = { 'BR-ALS-02': {}, 'BR-NEW-03': {} }, ctx = aliases => ({ optionMaps: {}, aliases, noDesign: {}, masterEntry: s => lib0[s] || null });
  const order = { receiptId: '4100000001', updateTs: 1, staffNote: '', buyerMessage: '' };
  const line = sku => ({ transactionId: '41000000011', listingId: '7001', sku, title: 'Star necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [] });
  let sp = O.interpretLine(order, line('BR-GONE-09'), ctx(probe.aliases));
  assert.equal(sp.designSku, 'BR-ALS-02', 'before: the old alias stands in for the unknown SKU');
  // another variation of the listing is answered today, for its own SKU
  const put = await post({ op: 'aliasPut', listingId: '7001', sku: 'BR-NEW-03', fromSku: 'STAR-SILVER-X', by: 'Ben', title: 'Star necklace' });
  assert.equal(put.ok, true, JSON.stringify(put));
  const after = (await post({ op: 'aliasGet' })).aliases;
  sp = O.interpretLine(order, line('STAR-SILVER-X'), ctx(after)); assert.equal(sp.designSku, 'BR-NEW-03', 'the new answer is its variation\'s');
  sp = O.interpretLine(order, line('BR-GONE-09'), ctx(after));
  assert.equal(sp.designSku, 'BR-ALS-02', 'the answer saved before 27 Sep still stands in for its SKU (got ' + sp.designSku + ', ' + sp.problems.map(p => p.kind).join(',') + ')');
  assert.deepEqual(sp.problems, []);
  // a listing-wide answer given today (a line with no SKU) is the no-SKU lines' only, as before
  await post({ op: 'aliasPut', listingId: '7002', sku: 'BR-NEW-03', by: 'Ben', title: 'Moon necklace' });
  const a2 = (await post({ op: 'aliasGet' })).aliases;
  sp = O.interpretLine(order, Object.assign(line(''), { listingId: '7002' }), ctx(a2)); assert.equal(sp.designSku, 'BR-NEW-03');
  sp = O.interpretLine(order, Object.assign(line('MOON-GOLD-X'), { listingId: '7002' }), ctx(a2)); assert.equal(sp.designSku, 'MOON-GOLD-X', 'a today\'s no-SKU answer does not stand in for a variation SKU');
  console.log('  ✓ an old "Use this charm" keeps standing in after a variation of its listing is answered for its SKU');
  // "Use this charm" on a blocked SKU's card (it offers the button): the answer is that line's design from now on
  lib0['BR-BLK-07'] = { blocked: 'two labels under one charm' };
  sp = O.interpretLine(order, Object.assign(line('BR-BLK-07'), { listingId: '7003' }), ctx({}));
  assert.deepEqual(sp.problems.map(p => p.kind), ['blockedSku'], 'the blocked SKU is asked about');
  await post({ op: 'aliasPut', listingId: '7003', sku: 'BR-NEW-03', fromSku: 'BR-BLK-07', by: 'Ben', title: 'Blocked necklace' });
  const a3 = (await post({ op: 'aliasGet' })).aliases;
  sp = O.interpretLine(order, Object.assign(line('BR-BLK-07'), { listingId: '7003' }), ctx(a3));
  assert.equal(sp.designSku, 'BR-NEW-03', '"Use this charm" on a blocked SKU is its design (got ' + sp.designSku + ', ' + sp.problems.map(p => p.kind).join(',') + ')');
  assert.deepEqual(sp.problems, []);
  sp = O.interpretLine(order, Object.assign(line('BR-BLK-07'), { listingId: '7004' }), ctx(a3));
  assert.deepEqual(sp.problems.map(p => p.kind), ['blockedSku'], 'another listing\'s line with that SKU is still asked about');
  console.log('  ✓ "Use this charm" on a blocked SKU takes, for that listing and SKU only');
}

async function part2() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; }, { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
    const page = await context.newPage(); page.setDefaultTimeout(20000);
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // 60 orders, each with a SKU no master file holds: 60 Unknown SKU decisions, the oldest last
    const r = await page.evaluate(async () => {
      await Orders.loadMaps(true);
      const t0 = Date.now() - 3600e3;
      for (let i = 0; i < 60; i++) {
        const rid = String(4200000000 + i), order = { receiptId: rid, orderNumber: rid, createTs: 1789000000 + i, updateTs: 1789000000 + i, shipBy: 1790000000, buyer: { name: 'Buyer ' + i }, buyerMessage: '', staffNote: '', messages: [],
          lines: [{ transactionId: rid + '1', listingId: String(1900000 + i), sku: 'NOPE_' + i, title: 'Nope ' + i, quantity: 1, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
        const row = { key, order, line, arrivedAt: t0 + i * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
      }
      Orders.interpretAll(); if (Review.syncOrderItems) Review.syncOrderItems();
      const its = Review.items().filter(it => it.kind === 'unmatchedSku');
      return { n: its.length, oldest: its.map(it => it.key).find(k => /NOPE_0$/.test(k)) || '' };
    });
    assert(r.n >= 60 && r.oldest, 'sixty Unknown SKU decisions: ' + JSON.stringify(r));
    // from the Completed folder, "Show" for the oldest card under Unknown SKU (as a note's Show does)
    await page.evaluate(() => { CN.setMode('review'); Review.view().cseg = 'done'; Review.render(); });
    await page.waitForFunction(() => document.querySelector('#reviewView [data-cseg="done"].on'));
    const shown = await page.evaluate(k => { Review.showCard(k, 'open', 'unmatchedSku'); const n = [...document.querySelectorAll('#rvList .reviewListRow')]; return { drawn: n.length, found: n.some(x => x.dataset.mkey === k), open: !!document.querySelector('#reviewView [data-cseg="open"].on') }; }, r.oldest);
    assert(shown.open, 'Show opened the Open folder');
    assert(shown.found, `Show finds the card it names (${shown.drawn} cards drawn, the oldest of 60 not among them)`);
    console.log('  ✓ Review › Show from Completed finds a card beyond the first 40 under its chip (' + shown.drawn + ' drawn)');
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); await srv.close(); }
}

(async () => {
  let failed = 0;
  for (const [name, fn] of [['part 1', part1], ['part 2', part2]]) { try { await fn(); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } }
  console.log(failed ? `adv-skus: ${failed} failed` : 'adv-skus: all passed'); process.exit(failed ? 1 : 0);
})();
