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

/** A browser context that is another computer (second-computer.cjs): the harness's routes and start-up script, and storage of its own. */
async function otherComputer(H) {
  const fs = require('fs'), st = H.st, ctx = await H.browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: process.env.SIM_TZ || 'America/Toronto' });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(repo, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: '' }); return r.fulfill({ status: 200, body: b.data || b, headers: { 'Access-Control-Allow-Origin': '*' } }); });
  await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Second'); window.confirm = () => true; window.prompt = () => 'Second'; window.alert = () => {}; }, { sorter: H.sorterOrigin });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${H.sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && CN.S.cloud.ok !== null, null, { timeout: 90000 });
  return { ctx, page, errors };
}

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
    // (CW_SNAPSHOT=<file>: the fake cloud as the sorter left it and the sorter's rows, saved once, so the server checks run again in seconds with no browser: cloud-world-fast.cjs)
    if (process.env.CW_SNAPSHOT) require('fs').writeFileSync(process.env.CW_SNAPSHOT, JSON.stringify({ docs: [...st.docs.entries()], receipts: st.receipts, rows }));
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
    // (what the sorter asked the cloud for while it read, a listing's SKU table, is stored by then: the cloud reads it at its next build, as it would in the shop)
    await new Promise(r => setTimeout(r, 3000)); await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
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

    /* 5e · a line's state is where its pieces are: a run record that still says "pooled" for a piece the cloud has on a written set reads "written" (order 4228567858, 20 Nov) */
    { const pooledRow = live.find(r => r.state === 'pooled'); const doc0 = World.lineOf(st.doc('Charm_World_Lines', pooledRow.key)); const pid = (doc0.run.poolIds || [])[0];
      const pr = st.doc('Charm_Pool', pid); st.put('Charm_Pool', pid, Object.assign({}, pr, { state: 'written', sheetId: pr.sheetId || 'gold-test-sheet' }), false);
      await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
      const d5 = World.lineOf(st.doc('Charm_World_Lines', pooledRow.key));
      ok('5e · a line whose piece is on a written set reads "written" though the run record still says "pooled"', d5.state === 'written' && d5.run.state === 'pooled', JSON.stringify({ state: d5.state, run: d5.run.state }));
      st.put('Charm_Pool', pid, pr, false); }

    /* 5f · Claude's stored record of a line is read by the server (Charm_Nest_CustomRead: the reading by the run's hash or the latest, and a person's decision, which always wins) */
    { const unKey = (st.doc('Charm_World', 'summary').unmatched || [])[0], un = { key: unKey }; const h = 'hlatest01';
      st.put('Charm_Nest_CustomRead', un.key, { reads: { [h]: { kind: 'addOnToOrder', summary: 'test add-on', confidence: 0.9, hash: h, key: un.key, at: Date.now() } }, latest: h, decided: { kind: 'addOnToOrder', by: 'Tester', at: Date.now() } }, false);
      await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
      const d6 = World.lineOf(st.doc('Charm_World_Lines', un.key));
      ok('5f · the stored record is used: the line reads as an add-on to an order', d6.spec && d6.spec.special && d6.spec.special.kind === 'addOn', JSON.stringify(d6.spec && d6.spec.special && d6.spec.special.kind));
      st.docs.delete('Charm_Nest_CustomRead/' + un.key);
      await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} }); }

    /* 6b · paged answers skip nothing: every line, page by page (whole, and only what changed since) */
    { const got = []; let cursor = null, pages = 0; do { const g = await Lib.ops.worldGet(Object.assign({ lines: true, maxBytes: 2000 }, cursor ? { cursor } : {})); got.push(...g.lines.map(l => l.key)); cursor = g.next; pages++; } while (cursor && pages < 50);
      ok('6b · the whole list in small pages: every line once, none skipped', pages > 2 && JSON.stringify([...new Set(got)].sort()) === JSON.stringify(live.map(r => r.key).sort()) && got.length === live.length, JSON.stringify({ pages, got: got.length, live: live.length }));
      const got2 = []; cursor = null; pages = 0; do { const g = await Lib.ops.worldGet(Object.assign({ lines: true, sinceAt: 1, maxBytes: 2000 }, cursor ? { cursor } : {})); got2.push(...g.lines.filter(l => !l.gone).map(l => l.key)); cursor = g.next; pages++; } while (cursor && pages < 50);
      ok('6c · what changed since, in small pages, lines written in the same build included: none skipped', pages > 2 && JSON.stringify([...new Set(got2)].sort()) === JSON.stringify(live.map(r => r.key).sort()), JSON.stringify({ pages, got: got2.length, live: live.length })); }

    /* 7 · STAGE B: a second computer (carries no run) takes every order line from the cloud world, reads none itself, asks neither Etsy nor Claude, and shows the same Review */
    await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
    const cloudNow = new Map(st.list('Charm_World_Lines').filter(d => !d.gone).map(d => { const l = World.lineOf(d); return [l.key, l]; }));
    const n7 = st.calls.length;
    const two = await otherComputer(H), p2 = two.page;
    await p2.waitForFunction(() => window.CloudWorld && CloudWorld.state().applied && CloudWorld.state().applied.size > 0, null, { timeout: 90000, polling: 500 });
    const seen2 = await p2.evaluate(() => ({ drives: CloudWorld.drives(), rows: Orders.rows().filter(r => r.state !== 'gone').map(r => ({ key: r.key, state: r.state, cloud: !!r.cloud, sku: r.spec && r.spec.designSku, problems: (r.problems || []).map(p => JSON.stringify(p)).sort() })),
      rv: { openCount: Review.openCount(), reviewCount: Review.count(), openOrders: [...Review.openOrders().keys()].map(String).sort() } }));
    const bad7 = []; for (const r of seen2.rows) { const c = cloudNow.get(r.key); if (!c) { bad7.push(r.key + ' not in the cloud'); continue; } if (r.state !== c.state || (r.sku || null) !== ((c.spec && c.spec.designSku) || null) || JSON.stringify(r.problems) !== JSON.stringify((c.problems || []).map(p => JSON.stringify(p)).sort())) bad7.push(r.key); }
    ok('7a · the second computer is driven by the cloud and holds exactly the cloud\'s lines, each as the cloud read it', seen2.drives && seen2.rows.length === cloudNow.size && seen2.rows.every(r => r.cloud) && !bad7.length, JSON.stringify({ drives: seen2.drives, n: seen2.rows.length, cloud: cloudNow.size, bad: bad7.slice(0, 5) }));
    const cloudRv2 = (await Lib.ops.worldGet({})).review, pageRv2 = await page.evaluate(() => ({ openCount: Review.openCount(), reviewCount: Review.count(), openOrders: [...Review.openOrders().keys()].map(String).sort() }));
    ok('7b · its Review is the cloud\'s and the first computer\'s (Open count, decisions, the orders in Open)', JSON.stringify(seen2.rv) === JSON.stringify(pageRv2) && seen2.rv.openCount === cloudRv2.counts.openCount && seen2.rv.reviewCount === cloudRv2.counts.reviewCount, JSON.stringify({ second: seen2.rv, first: pageRv2, cloud: cloudRv2.counts && { openCount: cloudRv2.counts.openCount, reviewCount: cloudRv2.counts.reviewCount } }));
    const asked = st.calls.slice(n7).filter(c => /^(listOpenOrders|etsyOrderProxy)$/.test(c.name) || (c.op === 'startAgent' && /customRead/.test(JSON.stringify(c.body || {}))));
    /* 7g · what each SCREEN shows (the Review tab's Open / Resolved, every chip, the two tab badges) is the cloud's, on both computers */
    const screenOf = pg => pg.evaluate(async () => { await CloudWorld.refresh('test'); CN.setMode('review'); Review.render({ still: true }); Ladder.now(); await new Promise(r => setTimeout(r, 600));
      const bar = document.querySelector('#reviewView .ordBar'), n = sel => { const e = bar && bar.querySelector(sel); return e ? +(e.querySelector('b') || {}).textContent || 0 : 0; };
      const chips = {}; for (const b of (bar ? bar.querySelectorAll('.egTab[data-k]') : [])) chips[b.dataset.k] = +((b.querySelector('b') || {}).textContent || 0);
      return { open: n('[data-cseg=open]'), resolved: n('[data-cseg=done]'), chips, review: +(document.getElementById('tabReviewN') || {}).textContent || 0, design: +(document.getElementById('tabEngraveN') || {}).textContent || 0 }; });
    { const rvDoc = (await Lib.ops.worldGet({})).review; ok('7h · the cloud keeps its Resolved list (resolvedKeys) as well as its count, so every computer lists the same Resolved cards', Array.isArray(rvDoc.resolvedKeys) && rvDoc.resolvedKeys.length === rvDoc.counts.resolved && Array.isArray(rvDoc.openKeys), JSON.stringify({ keys: rvDoc.resolvedKeys && rvDoc.resolvedKeys.length, count: rvDoc.counts.resolved })); }
    const rvNow = (await Lib.ops.worldGet({})).review.counts, s1 = await screenOf(page), s2 = await screenOf(p2);
    const want7 = { open: rvNow.openCount, resolved: rvNow.resolved, review: rvNow.reviewOrders, design: rvNow.designOrders || 0 };
    const sameScreen = x => x.open === want7.open && x.resolved === want7.resolved && x.review === want7.review && x.design === want7.design && Object.entries(x.chips).every(([k, v]) => v === (k === 'customOrder' ? rvNow.customOrders : rvNow[k] || 0));
    ok('7g · both screens show the cloud\'s numbers: Open, Resolved, every chip, the Review and Design & Engrave badges', sameScreen(s1) && sameScreen(s2) && JSON.stringify(s1) === JSON.stringify(s2), JSON.stringify({ cloud: want7, first: s1, second: s2 }));
    /* 7i · SCREENCHECK: each computer reports what its screen shows, and the cloud says whether it is its own (Settings → Computers, computerList) */
    await page.evaluate(() => CharmNestDrift.check('test')); await p2.evaluate(() => CharmNestDrift.check('test'));
    const cl7 = await Lib.ops.computerList({}), withScreen = cl7.computers.filter(c => c.screen);
    ok('7i · both computers report their screen, and the cloud finds both the same as its own (badges, Open, Resolved, every chip)', withScreen.length >= 2 && withScreen.every(c => c.screenSame === true), JSON.stringify({ cloud: cl7.cloudScreen, computers: withScreen.map(c => ({ by: c.by, same: c.screenSame, diff: c.screenDiff, screen: c.screen })) }).slice(0, 900));
    ok('7c · it asked neither Etsy nor Claude for anything', !asked.length, JSON.stringify(asked.map(c => c.name + ':' + c.op)));
    await p2.reload(); await p2.waitForFunction(() => window.CN && CN.S && CN.S.cloud.ok !== null && window.Orders, null, { timeout: 90000 });
    const early = await p2.evaluate(() => Orders.rows().filter(r => r.state !== 'gone' && !r.cloud).length);
    await p2.waitForFunction(() => window.CloudWorld && CloudWorld.state().applied && Orders.rows().some(r => r.cloud), null, { timeout: 90000, polling: 500 }).catch(() => {});
    const after = await p2.evaluate(() => ({ n: Orders.rows().filter(r => r.state !== 'gone').length, own: Orders.rows().filter(r => r.state !== 'gone' && !r.cloud).length }));
    ok('7d · after a reload it shows no order line of its own saved copy: only the cloud\'s, as soon as they are read', early === 0 && after.own === 0 && after.n === cloudNow.size, JSON.stringify({ early, after }));
    ok('7e · no page error on the second computer', !two.errors.length, JSON.stringify(two.errors.slice(0, 3)));
    /* 7f · STAGE F: what an older page kept in this browser is backed up to the cloud first, then the production copies are removed; display settings and sign-in stay */
    const f7 = await p2.evaluate(async () => {
      localStorage.removeItem('cn.backup.v'); localStorage.setItem('cn.takeover.cloudsh', JSON.stringify({ big: 'x'.repeat(400000) })); localStorage.setItem('cn.baseline.20261113', '{"a":1}'); localStorage.setItem('cn.customRead.production', '{"k":{"kind":"custom"}}'); localStorage.setItem('cn.listingSkus.v1', '{"v":1}'); localStorage.setItem('ce.view', 'grid');
      const r = await CloudWorld.backupAndClear();
      return { r, left: ['cn.takeover.cloudsh', 'cn.baseline.20261113', 'cn.customRead.production', 'cn.listingSkus.v1', 'ce.view', 'cn.employee'].map(k => [k, localStorage.getItem(k) != null]) };
    });
    const allParts7 = st.list('Charm_Browser_Backup'), lastStamp7 = Math.max(...allParts7.map(x => +x.stamp || 0)), parts7 = allParts7.filter(x => +x.stamp === lastStamp7), joined = parts7.sort((x, y) => x.part - y.part).map(x => x.json).join(''); let back7 = null; try { back7 = JSON.parse(joined); } catch (_) {}
    ok('7f · the browser\'s storage is in the cloud whole (every key, in parts), then the leftovers and the cloud-held copies are removed; display and sign-in stay', back7 && back7.keys && back7.keys['cn.takeover.cloudsh'] && back7.keys['ce.view'] === 'grid' && parts7.length >= 2 && JSON.stringify(f7.left) === JSON.stringify([['cn.takeover.cloudsh', false], ['cn.baseline.20261113', false], ['cn.customRead.production', false], ['cn.listingSkus.v1', false], ['ce.view', true], ['cn.employee', true]]), JSON.stringify({ parts: parts7.length, left: f7.left, r: f7.r && { parts: f7.r.parts, removed: f7.r.removed } }));
    await two.ctx.close();

    /* 8 · STAGE C: the computer that carries the run takes each line's reading from the cloud, and its new orders from the cloud world, not from its own Etsy pull */
    await page.evaluate(() => CloudWorld.refresh('test'));
    await page.evaluate(() => Orders.interpretAll());
    const c8 = await page.evaluate(() => { const live = Orders.rows().filter(r => r.state !== 'gone'); return { wanted: CloudWorld.wanted(), drives: CloudWorld.drives(), n: live.length, cloud: live.filter(r => r.readBy === 'cloud').length, here: live.filter(r => r.readBy === 'here').map(r => r.key) }; });
    ok('8a · the run computer reads no line itself: every reading is the cloud\'s', c8.wanted && !c8.drives && c8.n > 0 && c8.cloud === c8.n, JSON.stringify(c8));
    // a new order: on Etsy, copied by the mail system, read by the cloud, then taken in by the run computer's next check
    const nr = rich(3940200001, [{ sku: 'BR-TST-08', metal: GF }]); st.receipts.push(nr); mirror(nr);
    await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
    const n8 = st.calls.length;
    await W.due(page);
    await page.waitForFunction(() => Orders.rows().some(r => r.key === '3940200001_39402000011'), null, { timeout: 120000, polling: 500 }).catch(() => {});
    const got8 = await page.evaluate(() => { const r = Orders.rows().find(x => x.key === '3940200001_39402000011'); return r ? { state: r.state, readBy: r.readBy, sku: r.spec && r.spec.designSku } : null; });
    const etsy8 = st.calls.slice(n8).filter(c => /^(listOpenOrders|etsyOrderProxy)$/.test(c.name)).map(c => c.name);
    ok('8b · a new order reaches the run computer from the cloud world, read by the cloud, with no Etsy call from the browser', got8 && got8.readBy === 'cloud' && got8.sku === 'BR-TST-08' && !etsy8.length, JSON.stringify({ got8, etsy8 }));
    const v8 = await W.rest(page, { log: () => {}, timeoutMs: 240000 });
    const placed8 = await page.evaluate(() => { const r = Orders.rows().find(x => x.key === '3940200001_39402000011'); return r && r.state; });
    ok('8c · the run places it as it places any order', !v8.timeout && ['pooled', 'written', 'committed'].includes(placed8), JSON.stringify({ placed8, run: v8 && v8.run }));
    // a person's decision on the run computer: read here at once (the cloud has not seen it yet), the cloud's reading again once the cloud has it
    const one8 = (st.doc('Charm_World', 'summary').unmatched || [])[0];
    if (one8) {
      const at8 = await page.evaluate(k => { const row = Orders.rows().find(r => r.key === k); row.skuOverride = { sku: 'BR-TST-02', by: 'Tester', at: Date.now(), was: row.line.sku }; Orders.interpretAll(); RunCtl.save && RunCtl.save(); return { readBy: row.readBy, sku: row.spec.designSku }; }, one8);
      await new Promise(r => setTimeout(r, 1500));
      await World.build({ db: st.db, admin: st.admin, Lib, etsy, force: true, log: () => {} });
      await page.evaluate(() => CloudWorld.refresh('test'));
      await page.evaluate(() => new Promise(r => setTimeout(r, 300)));
      const back8 = await page.evaluate(k => { Orders.interpretAll(); const row = Orders.rows().find(r => r.key === k); return { readBy: row.readBy, sku: row.spec.designSku }; }, one8);
      ok('8d · a decision made a moment ago is read here at once, and the cloud\'s reading (with that decision) takes over as soon as the cloud has it', at8.readBy === 'here' && at8.sku === 'BR-TST-02' && back8.readBy === 'cloud' && back8.sku === 'BR-TST-02', JSON.stringify({ at8, back8 }));
    }

    /* 6 · worldGet answers it */
    const g = await Lib.ops.worldGet({ lines: true });
    ok('6 · worldGet answers the summary and every line', g.summary && g.lines && g.lines.length === g.summary.live, JSON.stringify({ n: g.lines && g.lines.length }));
    ok('no page error', !H.errors.filter(e => !/HTTP 404|messagesFor=|status of 400/.test(e)).length, JSON.stringify(H.errors.slice(0, 3)));
  } catch (e) { console.log('FAIL threw: ' + (e && e.stack || e)); failed++; }
  finally { await H.close(); }
  console.log(failed ? 'FAIL cloud-world' : 'PASS cloud-world');
  process.exit(failed ? 1 : 0);
})();
