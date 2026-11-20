// CLOUDWORLD (Paul, 20 Nov 2026: "Everything that was production related was supposed to be in the cloud ... Only the cloud can decide").
// The same orders read by the sorter (as every computer reads them today) and by the server (_charmNestWorld.js, once, for every computer): every line must
// read the same — its state, its design, its metal, size and quantity, its questions with every detail — and the server must keep the full line (the buyer's
// message, the staff note, the options with their Etsy ids) that the run's own record leaves out.
// The real sorter and the framed Design Station in Chromium against the repo's fake site (day-sim/harness.cjs); the server builds over the same in-memory
// Firestore, with a stand-in for Etsy that answers from the same receipts. Nothing here touches anything live.
//   PW_DIR=<dir holding playwright-core> CHROMIUM=<chrome> node tests/charm-nest/cloud-world.cjs
const assert = require('assert'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open, repo } = require('./day-sim/harness.cjs');
const W = require('./_autorunWorld.cjs');

const day = Math.floor(Date.now() / 1000);
const GF = W.GF, SS = W.SS;
/** A receipt with several lines and words, as Etsy sends it. */
const rich = (rid, lines, extra = {}) => Object.assign({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: extra.msg || '', update_timestamp: day, updated_timestamp: day, create_timestamp: day - 7200, status: 'Paid', is_paid: true, is_shipped: false, is_gift: !!extra.gift, gift_message: extra.giftMsg || '',
  transactions: lines.map((l, i) => ({ transaction_id: Number(`${rid}${i + 1}`), listing_id: l.listing || 1718099, receipt_id: rid, sku: l.sku, title: l.title || `${l.sku || 'no sku'} charm`, quantity: l.qty || 1, expected_ship_date: day + 3 * 86400, is_personalized: !!l.pers,
    variations: [].concat(l.metal ? [{ formatted_name: 'Metal', formatted_value: l.metal, property_id: 200, value_id: 9000 + i }] : [], l.opt ? [{ formatted_name: l.opt[0], formatted_value: l.opt[1], property_id: 513, value_id: 7000 + i }] : [], l.pers ? [{ formatted_name: 'Personalization', formatted_value: l.pers, property_id: 54, value_id: null }] : []) })) }, extra.raw || {});

const pick = (s) => s ? { designSku: s.designSku || null, material: s.material || null, size: s.size || null, form: s.form || null, quantity: s.quantity || 1, noDesign: !!s.noDesign, special: s.special ? s.special.kind || true : null, problems: (s.problems || []).map(p => JSON.stringify(p)).sort() } : null;

(async () => {
  const receipts = W.wave(3940000001, 3).concat([
    rich(3940100001, [{ sku: 'BR-TST-01', metal: GF }, { sku: 'BR-TST-02', metal: SS, qty: 2 }], { msg: 'Please wrap it &amp; thank you' }),
    rich(3940100002, [{ sku: 'NOPE-UNKNOWN-SKU', metal: GF }]),                         // a SKU no master holds
    rich(3940100003, [{ sku: 'BR-TST-03', opt: ['Chain Length', '18 inch'] }]),         // no metal read
    rich(3940100004, [{ sku: '', metal: SS, title: 'Custom name charm', pers: 'Ava &quot;Rose&quot;' }], { gift: true, giftMsg: 'Happy birthday' }),
    rich(3940100005, [{ sku: 'BR-TST-06', metal: 'Rose Gold Filled' }, { sku: 'BR-TST-07', metal: GF, opt: ['Style', 'Huggie'] }])
  ]);
  const H = await open({ receipts, masters: W.MASTERS, settings: { budgetS: 2, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10 } });
  const { page, st } = H;
  const t0 = Date.now(), log = (...a) => console.log(Math.round((Date.now() - t0) / 1000) + 's', ...a);
  let failed = 0; const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (detail ? ' :: ' + String(detail).slice(0, 900) : '')); if (!cond) failed++; };
  try {
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    const v = await W.rest(page, { log, timeoutMs: 420000 });
    ok('the sorter ran the orders to rest', !v.timeout, JSON.stringify(v && v.run));
    await page.evaluate(() => Session.flush && Session.flush());
    const rows = await page.evaluate(() => Orders.rows().map(r => ({ key: r.key, state: r.state, spec: r.spec ? { designSku: r.spec.designSku, material: r.spec.material, size: r.spec.size, form: r.spec.form, quantity: r.spec.quantity, noDesign: r.spec.noDesign, special: r.spec.special, problems: r.problems || [] } : null, line: r.line, order: r.order && { receiptId: r.order.receiptId, buyerMessage: r.order.buyerMessage, isGift: r.order.isGift, giftMessage: r.order.giftMessage, shipBy: r.order.shipBy, updateTs: r.order.updateTs, createTs: r.order.createTs, buyer: r.order.buyer } })));
    log('sorter rows:', rows.length, JSON.stringify(rows.reduce((m, r) => (m[r.state] = (m[r.state] || 0) + 1, m), {})));

    // the server, over the same in-memory Firestore (the harness's fake firebase-admin is in place for every require below)
    const fnDir = path.join(repo, 'netlify/functions');
    const Lib = require(path.join(fnDir, 'charmNestLibrary.js'));
    const World = require(path.join(fnDir, '_charmNestWorld.js'));
    const asV3 = r => Object.assign({}, r, { updated_timestamp: r.updated_timestamp || r.update_timestamp, created_timestamp: r.created_timestamp || r.create_timestamp, is_paid: r.is_paid !== false });
    const etsyCalls = [];
    const etsy = { SHOP_ID: 'shop', etsyFetch: async (p, o = {}) => { etsyCalls.push(['list', o.query]); const q = o.query || {}, off = +q.offset || 0, open = st.receipts.filter(r => !r.is_shipped && !/cancel/i.test(r.status || '')).map(r => Object.assign(asV3(r), { transactions: undefined })); return { results: open.slice(off, off + 100), count: open.length }; },
      getShopReceiptFull: async id => { etsyCalls.push(['full', id]); const r = st.receipts.find(x => String(x.receipt_id) === String(id)); return r ? asV3(JSON.parse(JSON.stringify(r))) : null; } };
    // the mail system's copy of Etsy's receipts (etsyMailReceiptsMirrorCron writes EtsyMail_Receipts/<id> with the whole receipt as Etsy's list gives it, items included)
    const mirror = r => { const v = asV3(JSON.parse(JSON.stringify(r))); st.put('EtsyMail_Receipts', String(r.receipt_id), { receipt_id: String(r.receipt_id), status: v.status, is_paid: v.is_paid, is_shipped: !!v.is_shipped, updated_timestamp: v.updated_timestamp, raw: v }, false); };
    for (const r of st.receipts) if (String(r.receipt_id) !== '3940100005') mirror(r);   // (one receipt not copied yet: the only one Etsy is asked for)
    const out = await World.build({ db: st.db, admin: st.admin, Lib, etsy, sweep: true, log: t => process.env.CN_VERBOSE && console.log('  ' + t) });
    ok('0a · Etsy is asked only for the one receipt the copy does not hold, and never for a list', JSON.stringify(etsyCalls) === JSON.stringify([['full', '3940100005']]), JSON.stringify(etsyCalls));
    ok('0b · the call is counted against the day\'s 500 in the cloud', (st.doc('Charm_World', 'etsy') || {}).used === 1 && (st.doc('Charm_World', 'etsy') || {}).cap === 500, JSON.stringify(st.doc('Charm_World', 'etsy')));
    log('server world:', JSON.stringify({ live: out.live, states: out.states, writes: out.writes, errors: out.errors }));
    ok('the server built the world with no error', !(out.errors || []).length, JSON.stringify(out.errors));
    const docs = new Map(st.list('Charm_World_Lines').filter(d => !d.gone).map(d => [d.key, World.lineOf(d)]));

    /* 1 · the same lines */
    const live = rows.filter(r => r.state !== 'gone');
    ok('1 · the server holds every line the sorter holds, and no other', JSON.stringify(live.map(r => r.key).sort()) === JSON.stringify([...docs.keys()].sort()), `sorter ${live.length} · server ${docs.size} · only sorter ${live.map(r => r.key).filter(k => !docs.has(k))} · only server ${[...docs.keys()].filter(k => !live.some(r => r.key === k))}`);

    /* 2 · every line read the same */
    const diffs = [];
    for (const r of live) { const d = docs.get(r.key); if (!d) continue;
      const a = pick(Object.assign({}, r.spec, { problems: r.spec && r.spec.problems })), b = pick(Object.assign({}, d.spec, { problems: d.problems }));
      if (JSON.stringify(a) !== JSON.stringify(b)) diffs.push({ key: r.key, sorter: a, server: b });
      if (r.state !== d.state) diffs.push({ key: r.key, sorterState: r.state, serverState: d.state }); }
    ok('2 · every line reads the same on the server as on the sorter (design, metal, size, quantity, questions with their details, state)', !diffs.length, JSON.stringify(diffs.slice(0, 4)));

    /* 3 · the full line is kept */
    const ldiffs = [];
    for (const r of live) { const d = docs.get(r.key); if (!d || !d.line) continue; if (JSON.stringify(r.line) !== JSON.stringify(d.line)) ldiffs.push({ key: r.key, sorter: r.line, server: d.line });
      for (const k of ['receiptId', 'buyerMessage', 'isGift', 'giftMessage', 'shipBy', 'updateTs', 'createTs']) if (JSON.stringify(r.order[k]) !== JSON.stringify(d.order[k])) ldiffs.push({ key: r.key, field: k, sorter: r.order[k], server: d.order[k] }); }
    ok('3 · the server keeps the full Etsy line and order exactly as the station gives them (options with their Etsy ids, personalization, buyer message, gift)', !ldiffs.length, JSON.stringify(ldiffs.slice(0, 3)));
    const wrap = docs.get('3940100001_39401000011');
    ok('3b · the buyer message (as Etsy sends it, as the sorter keeps it), which the run record leaves out, is in the cloud line', wrap && wrap.order && /wrap it &amp; thank you/.test(wrap.order.buyerMessage) && !(st.doc('Charm_Nest_Runs', Object.keys(Object.fromEntries(st.list('Charm_Nest_Runs').map(r => [r._id, 1])))[0]) || {}).buyerMessage, wrap && wrap.order && wrap.order.buyerMessage);

    /* 4 · a second build with nothing changed writes nothing and reads nothing from Etsy again */
    const n0 = etsyCalls.filter(c => c[0] === 'full').length;
    const again = await World.build({ db: st.db, admin: st.admin, Lib, etsy, log: () => {} });
    if (again.writes) { const after = new Map(st.list('Charm_World_Lines').map(d => [d.key, World.lineOf(d)])); for (const [k, d] of docs) { const e = after.get(k); for (const f of Object.keys(Object.assign({}, d, e))) if (!['at', 'sig'].includes(f) && JSON.stringify(d[f]) !== JSON.stringify(e && e[f])) { console.log('  changed between builds:', k, f, JSON.stringify(d[f]).slice(0, 300), '→', JSON.stringify(e && e[f]).slice(0, 300)); break; } } }
    ok('4 · a build with nothing new is skipped (no line written) and reads no receipt whole again', again.skipped === true && etsyCalls.filter(c => c[0] === 'full').length === n0, JSON.stringify({ writes: again.writes, full: etsyCalls.filter(c => c[0] === 'full').length - n0 }));
    const forced = await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
    ok('4b · a forced build with nothing new reads every line again and writes none', forced.writes === 0 && !forced.skipped && etsyCalls.filter(c => c[0] === 'full').length === n0, JSON.stringify({ writes: forced.writes, skipped: forced.skipped }));
    /* 4c · a decision made on a computer (a person's Change SKU on one line, saved to the run's record as the run saves it) reaches the cloud line at the next build */
    const one = live.find(r => r.state === 'unmatched');
    if (one) {
      await page.evaluate(k => { const row = Orders.rows().find(r => r.key === k); row.skuOverride = { sku: 'BR-TST-01', by: 'Tester', at: Date.now(), was: row.line.sku }; Orders.interpretAll(); return RunCtl.save && RunCtl.save(); }, one.key);
      await new Promise(r => setTimeout(r, 1500));
      const b3 = await World.build({ db: st.db, admin: st.admin, Lib, etsy, log: () => {} });
      const d3 = World.lineOf(st.doc('Charm_World_Lines', one.key)), pr = await page.evaluate(k => { const r = Orders.rows().find(x => x.key === k); return { sku: r.spec.designSku, problems: (r.problems || []).map(p => p.kind) }; }, one.key);
      ok('4c · a person\'s decision on one computer reaches the cloud line at the next build, read the same as the sorter reads it', !b3.skipped && d3 && d3.spec && d3.spec.designSku === pr.sku && JSON.stringify((d3.problems || []).map(p => p.kind)) === JSON.stringify(pr.problems) && d3.run && d3.run.skuOverride && d3.run.skuOverride.sku === 'BR-TST-01', JSON.stringify({ skipped: b3.skipped, cloud: d3 && d3.spec && d3.spec.designSku, sorter: pr }));
    }

    /* 5 · the summary says what a computer must show */
    const sum = st.doc('Charm_World', 'summary');
    ok('5 · the summary counts the live lines and names the held and unmatched ones', sum && sum.live === live.length && JSON.stringify(sum.held) === JSON.stringify(live.filter(r => r.state === 'held').map(r => r.key).sort()), JSON.stringify(sum && { live: sum.live, held: sum.held, unmatched: sum.unmatched }));

    /* 5b · the cloud's Review is the sorter's Review */
    const pageRv = await page.evaluate(() => ({ openCount: Review.openCount(), reviewCount: Review.count(), openOrders: [...Review.openOrders().keys()].map(String).sort() }));
    const cloudRv = Lib.ops && (await Lib.ops.worldGet({})).review;
    ok('5b · the cloud builds Review and it is the sorter\'s (Open count, decisions, the orders in Open)', cloudRv && cloudRv.counts && cloudRv.counts.openCount === pageRv.openCount && cloudRv.counts.reviewCount === pageRv.reviewCount && JSON.stringify((cloudRv.openOrders || []).map(String).sort()) === JSON.stringify(pageRv.openOrders), JSON.stringify({ page: pageRv, cloud: cloudRv && { counts: { openCount: cloudRv.counts.openCount, reviewCount: cloudRv.counts.reviewCount }, openOrders: cloudRv.openOrders } }));

    /* 5c · the day's 500 spent: Etsy is not asked at all, and the summary says so */
    { const was = st.doc('Charm_World', 'etsy'); st.put('Charm_World', 'etsy', { day: was.day, used: 500, cap: 500 }, false);
      st.docs.delete('Charm_World_Etsy/3940100005'); const n1 = etsyCalls.length;
      const b5 = await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
      ok('5c · with the day\'s 500 Etsy calls spent, nothing is asked of Etsy and the summary says why', etsyCalls.length === n1 && (b5.errors || []).some(e => /500 calls are spent/.test(e)), JSON.stringify({ calls: etsyCalls.slice(n1), errors: b5.errors }));
      mirror(st.receipts.find(r => String(r.receipt_id) === '3940100005'));
      const b6 = await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
      ok('5d · once the copy holds it, the receipt is read from the copy, still with no Etsy call', etsyCalls.length === n1 && !!World.lineOf(st.doc('Charm_World_Lines', '3940100005_39401000051')).spec && !(b6.errors || []).length, JSON.stringify({ calls: etsyCalls.slice(n1), errors: b6.errors })); }

    /* 6 · worldGet answers it */
    const g = await Lib.ops.worldGet({ lines: true });
    ok('6 · worldGet answers the summary and every line', g.summary && g.lines && g.lines.length === live.length, JSON.stringify({ n: g.lines && g.lines.length }));
    ok('no page error', !H.errors.filter(e => !/HTTP 404|messagesFor=|status of 400/.test(e)).length, JSON.stringify(H.errors.slice(0, 3)));
  } catch (e) { console.log('FAIL threw: ' + (e && e.stack || e)); failed++; }
  finally { await H.close(); }
  console.log(failed ? 'FAIL cloud-world' : 'PASS cloud-world');
  process.exit(failed ? 1 : 0);
})();
