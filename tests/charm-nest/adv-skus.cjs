// Adversarial (wave 3, area 16): SKUs and Review.
//  1 · "Use this charm" saved before 27 Sep (a listing-wide alias, no v) keeps standing in for the SKU it was saved for
//      after another variation of the same listing is answered the new way (listing + SKU): the server marked the whole
//      listing v: 2 on that answer, so the old alias silently stopped applying and its orders were asked about again.
//      Run against charmNestLibrary's aliasPut/aliasGet with a Firestore fake that merges nested maps as Firestore does.
//  2 · Review's "Show" (a card that moved to another folder or chip) and "Jump" find a card beyond the first 40: a new
//      view reset the list to its first 40 after Show had raised it, so an older card was never drawn and Show did nothing.
//  3 · A charm-only listing's "Huggie CHARM SET" (Paul, 27 Sep 19:14: "SKU (Huggie)" from the master, else Unknown SKU, never
//      the necklace charm): a huggie set line with no SKU took the listing's necklace "Use this charm" answer, and its own
//      answer was saved as the necklace's. It now has its own answer (listing + huggie) and its own Unknown SKU card.
//      "X-CO" bought as a huggie set was asked for "X-CO (HUGGIE)", which no master holds: it is "X (HUGGIE)".
//  4 · (wave 4) "Nothing to cut" on a no-SKU huggie set's card saved a title rule that also marked the listing's no-SKU
//      necklace lines; and a real SKU ending in "-CO" (the master holds it) lost its "-CO" as a huggie set.
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

async function part3() {
  delete process.env.EDIT_PASSCODE;
  const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
  const O = require(path.join(root, 'charm-nest-orders.js'));
  const post = body => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }).then(r => JSON.parse(r.body || '{}'));
  const lib0 = { 'BR-ALS-02': {}, 'BUNNY_42980': {}, 'BUNNY_42980 (HUGGIE)': {} }, ctx = aliases => ({ optionMaps: {}, aliases, noDesign: {}, masterEntry: s => lib0[s] || null });
  const order = { receiptId: '4300000001', updateTs: 1, staffNote: '', buyerMessage: '' };
  const line = (sku, type, lid) => ({ transactionId: '43000000011', listingId: lid || '7101', sku, title: 'Bunny charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold',
    personalization: [], variations: [{ name: 'Metal', value: 'Gold Filled' }, { name: 'Charm Type', value: type }] });
  // the listing's necklace charm, answered for a line with no SKU
  await post({ op: 'aliasPut', listingId: '7101', sku: 'BR-ALS-02', by: 'Ana', title: 'Bunny charm' });
  let al = (await post({ op: 'aliasGet' })).aliases;
  let sp = O.interpretLine(order, line('', 'Necklace CHARM'), ctx(al)); assert.equal(sp.designSku, 'BR-ALS-02', 'the necklace charm is the listing\'s answer');
  sp = O.interpretLine(order, line('', 'Huggie CHARM SET'), ctx(al));
  assert.notEqual(sp.designSku, 'BR-ALS-02', 'a Huggie CHARM SET with no SKU never takes the necklace charm');
  assert.deepEqual(sp.problems.map(p => p.kind), ['unmatchedSku'], 'it is an Unknown SKU: ' + JSON.stringify(sp.problems));
  assert.match(sp.problems[0].reason, /Huggie CHARM SET: pick its huggie design/);
  // its answer is its own: the necklace charm stays the necklace's
  const put = await post({ op: 'aliasPut', listingId: '7101', sku: 'BUNNY_42980 (HUGGIE)', huggie: true, by: 'Ben', title: 'Bunny charm' });
  assert.equal(put.ok, true, JSON.stringify(put));
  al = (await post({ op: 'aliasGet' })).aliases;
  sp = O.interpretLine(order, line('', 'Huggie CHARM SET'), ctx(al)); assert.equal(sp.designSku, 'BUNNY_42980 (HUGGIE)', 'the huggie set\'s own answer'); assert.deepEqual(sp.problems, []);
  sp = O.interpretLine(order, line('', 'Necklace CHARM'), ctx(al)); assert.equal(sp.designSku, 'BR-ALS-02', 'the necklace charm is untouched by the huggie answer');
  sp = O.interpretLine(order, line('', 'CHARM + Engraving'), ctx(al)); assert.equal(sp.designSku, 'BR-ALS-02');
  console.log('  ✓ a Huggie CHARM SET with no SKU has its own answer, never the listing\'s necklace charm');
  // "X-CO" bought as a huggie set is X's huggie design
  sp = O.interpretLine(order, line('Bunny_42980-CO', 'Huggie CHARM SET', '7102'), ctx({}));
  assert.equal(sp.designSku, 'BUNNY_42980 (HUGGIE)', '"X-CO" as a huggie set is "X (HUGGIE)" (got ' + sp.designSku + ')'); assert.deepEqual(sp.problems, []);
  sp = O.interpretLine(order, line('Bunny_42980-CO', 'Necklace CHARM', '7102'), ctx({})); assert.equal(sp.designSku, 'BUNNY_42980', 'and as a necklace charm "X"');
  sp = O.interpretLine(order, line('DRAGON 11-CO', 'Huggie CHARM SET', '7102'), ctx({}));
  assert.deepEqual(sp.problems.map(p => p.kind + ':' + p.sku), ['unmatchedSku:DRAGON 11 (HUGGIE)'], 'no huggie design: asked as "X (HUGGIE)"');
  console.log('  ✓ "X-CO" bought as a Huggie CHARM SET reads as "X (HUGGIE)"');
  // (wave 4) a real SKU that ends in "-CO" keeps it: the master (the copy already loaded) is asked before the mark is dropped
  Object.assign(lib0, { 'MOON-CO': {}, 'MOON-CO (HUGGIE)': {}, 'MOON (HUGGIE)': {}, 'STAR-CO': {}, 'STAR (HUGGIE)': {} });
  sp = O.interpretLine(order, line('Moon-CO', 'Huggie CHARM SET', '7103'), ctx({}));
  assert.equal(sp.designSku, 'MOON-CO (HUGGIE)', 'a real "X-CO" with its own huggie design is that design (got ' + sp.designSku + ')'); assert.deepEqual(sp.problems, []);
  sp = O.interpretLine(order, line('STAR-CO', 'Huggie CHARM SET', '7103'), ctx({}));
  assert.deepEqual(sp.problems.map(p => p.kind + ':' + p.sku), ['unmatchedSku:STAR-CO (HUGGIE)'], 'a real "X-CO" without a huggie design is asked as "X-CO (HUGGIE)", never as X\'s huggie (got ' + sp.designSku + ')');
  sp = O.interpretLine(order, line('STAR-CO', 'Necklace CHARM', '7103'), ctx({})); assert.equal(sp.designSku, 'STAR-CO', 'and as a necklace charm it is itself');
  // a title rule saved before (a no-SKU necklace's "Nothing to cut") still covers the listing's huggie sets, as it did
  const nd0 = { patterns: ['^Bunny charm'], skus: [] }, ctxN = Object.assign(ctx({}), { noDesign: nd0 });
  assert.equal(O.interpretLine(order, line('', 'Necklace CHARM', '7104'), ctxN).noDesign, true);
  assert.equal(O.interpretLine(order, line('', 'Huggie CHARM SET', '7104'), ctxN).noDesign, true, 'an existing title rule still covers the huggie set');
  console.log('  ✓ a real "X-CO" bought as a Huggie CHARM SET keeps its "-CO" when the master holds it');
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

    // a Huggie CHARM SET with no SKU is its own Unknown SKU card, and its "Use this charm" is saved as the huggie set's
    const sent = []; page.on('request', q => { if (/charmNestLibrary/.test(q.url()) && q.method() === 'POST') { try { const b = JSON.parse(q.postData() || '{}'); if (b.op === 'aliasPut') sent.push(b); } catch (_) {} } });
    const h = await page.evaluate(() => {
      B.master.entries.set('BUNNY_42980 (HUGGIE)', { sku: 'BUNNY_42980 (HUGGIE)' });
      const mk = (i, type) => { const rid = String(4300000100 + i), order = { receiptId: rid, orderNumber: rid, createTs: 1789000000, updateTs: 1789000000, shipBy: 1790000000, buyer: { name: 'Hug ' + i }, buyerMessage: '', staffNote: '', messages: [],
          lines: [{ transactionId: rid + '1', listingId: '1999101', sku: '', title: 'Bunny charm', quantity: 1, variations: [{ name: 'Metal', value: '14k Gold Filled' }, { name: 'Charm Type', value: type }], metalKey: 'gold', metalLabel: 'GF', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
        const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); return key; };
      const neck = mk(0, 'Necklace CHARM'), hug = mk(1, 'Huggie CHARM SET');
      Orders.interpretAll(); Review.syncOrderItems();
      const its = Review.items().filter(it => it.kind === 'unmatchedSku' && (it.rows || [it.row]).some(r => r.key === neck || r.key === hug));
      return { neck, hug, cards: its.map(it => ({ key: it.key, rows: (it.rows || [it.row]).map(r => r.key), why: it.why })) };
    });
    assert.equal(h.cards.length, 2, 'the necklace charm and the huggie set are two Unknown SKU cards: ' + JSON.stringify(h.cards));
    const hc = h.cards.find(x => x.rows.includes(h.hug));
    assert(hc && !hc.rows.includes(h.neck), 'the huggie set\'s card is its own');
    assert.match(hc.why, /Huggie CHARM SET: pick its huggie design/);
    await page.evaluate(k => { const it = Review.items().find(x => x.key === k); const c = Review.card(it); c.id = 'hugCard'; document.body.appendChild(c);
      const f = c.querySelector('[data-f=sku]'); f.value = 'BUNNY_42980 (HUGGIE)'; f.dispatchEvent(new Event('input')); c.querySelector('[data-a=alias]').click(); }, hc.key);
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.designSku === 'BUNNY_42980 (HUGGIE)', h.hug, { timeout: 10000 }).catch(() => {});
    const after = await page.evaluate(([n, g]) => ({ neck: B.orders.byKey.get(n).spec.designSku, hug: B.orders.byKey.get(g).spec.designSku, alias: B.maps.aliases['1999101'] || null }), [h.neck, h.hug]);
    assert.deepEqual(sent.map(b => [b.listingId, b.sku, b.fromSku || '', !!b.huggie]), [['1999101', 'BUNNY_42980 (HUGGIE)', '', true]], 'saved as the huggie set\'s answer: ' + JSON.stringify(sent));
    assert.equal(after.hug, 'BUNNY_42980 (HUGGIE)', 'the huggie set takes its answer');
    assert.notEqual(after.neck, 'BUNNY_42980 (HUGGIE)', 'the necklace charm is not given the huggie design: ' + JSON.stringify(after));
    console.log('  ✓ Review: a Huggie CHARM SET with no SKU is its own card, and its answer is the huggie set\'s alone');

    // (wave 4) "Nothing to cut" on a no-SKU Huggie CHARM SET's card is the huggie set's alone: the listing's no-SKU
    // necklace lines (the same title) are still asked about
    const nd = []; page.on('request', q => { if (/charmNestLibrary/.test(q.url()) && q.method() === 'POST') { try { const b = JSON.parse(q.postData() || '{}'); if (b.op === 'noDesignPut') nd.push(b); } catch (_) {} } });
    const f = await page.evaluate(() => {
      const mk = (i, type) => { const rid = String(4300000200 + i), order = { receiptId: rid, orderNumber: rid, createTs: 1789000000, updateTs: 1789000000, shipBy: 1790000000, buyer: { name: 'Fox ' + i }, buyerMessage: '', staffNote: '', messages: [],
          lines: [{ transactionId: rid + '1', listingId: '1999102', sku: '', title: 'Fox charm (gift) for her', quantity: 1, variations: [{ name: 'Metal', value: '14k Gold Filled' }, { name: 'Charm Type', value: type }], metalKey: 'gold', metalLabel: 'GF', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
        const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); return key; };
      const neck = mk(0, 'Necklace CHARM'), hug = mk(1, 'Huggie CHARM SET');
      Orders.interpretAll(); Review.syncOrderItems();
      const it = Review.items().find(x => x.kind === 'unmatchedSku' && (x.rows || [x.row]).some(r => r.key === hug));
      const c = Review.card(it); c.id = 'foxCard'; document.body.appendChild(c); c.querySelector('[data-a=nodesign]').click();
      return { neck, hug };
    });
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.noDesign === true, f.hug, { timeout: 10000 }).catch(() => {});
    const fx = await page.evaluate(([n, g]) => { Orders.interpretAll(); const a = B.orders.byKey.get(n).spec, b = B.orders.byKey.get(g).spec; return { neck: a.noDesign, neckProblems: a.problems.map(p => p.kind), hug: b.noDesign }; }, [f.neck, f.hug]);
    assert.equal(nd.length, 1, 'one rule saved: ' + JSON.stringify(nd));
    assert.equal(fx.hug, true, 'the huggie set is nothing to cut: ' + JSON.stringify(fx));
    assert.equal(fx.neck, false, 'the listing\'s no-SKU necklace charm is still to be cut (rule ' + JSON.stringify(nd[0] && nd[0].pattern) + ')');
    assert.deepEqual(fx.neckProblems, ['unmatchedSku'], 'and still asked about');
    console.log('  ✓ Review: "Nothing to cut" on a no-SKU Huggie CHARM SET leaves the listing\'s necklace lines alone');
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); await srv.close(); }
}

(async () => {
  let failed = 0;
  for (const [name, fn] of [['part 1', part1], ['part 3', part3], ['part 2', part2]]) { try { await fn(); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } }
  console.log(failed ? `adv-skus: ${failed} failed` : 'adv-skus: all passed'); process.exit(failed ? 1 : 0);
})();
