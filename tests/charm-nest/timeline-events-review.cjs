// The order timeline from the sorter's own browser code (Paul, 28 Sep, D1-D3): what happens to an order only in the
// sorter's page is recorded through OrderTimeline.record — an order entering the pull, each line read, each question a
// line raises and a person's answer to it (decided, skipped, held), a held line restored, the back engraving read and
// its words decided, and a custom order's designs dropped and sent to the sheets. The sorter's timeline is configured
// as the sorter (its sandbox, the passcode api() sends, the employee on duty, kept current when the name changes).
// Opens the sorter in headless Chromium against the local fake site (bridge-server.cjs): every request that is not to
// the loopback is aborted. A spy wraps OrderTimeline.record; nothing is sent anywhere but the fake functions.
//   node tests/charm-nest/timeline-events-review.cjs [playwright-core dir]
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.now() / 1000) + 5 * DAY;
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 8 * DAY, updateTs: SHIP - 8 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {});
const silver = { metalKey: 'silver', metalLabel: 'Sterling Silver' };
const ORDERS = [
  order(4179000001, [line(41790000011, 'SEA_TURTLE', 'Sea Turtle Charm', [['Metal', 'Mystery alloy']])]),        // A: material → decided
  order(4179000002, [line(41790000021, 'SEA_TURTLE', 'Sea Turtle Charm', [['Metal', 'Mystery alloy']])]),        // B: Skip line
  order(4179000003, [line(41790000031, 'SEA_TURTLE', 'Sea Turtle Charm', [['Metal', 'Mystery alloy']])]),        // C: Hold order, released
  order(4179000004, [line(41790000041, 'UNKNOWN_77', 'Mystery Charm', [['Metal', 'Sterling Silver']], silver)]), // D: Nothing to cut
  order(4175423829, [line(41754238291, 'CUSTOM-N-001-665441', 'Custom Name Necklace, Personalized Gold Charm Necklace', [['Metal', '14k Gold Filled']], { metalKey: 'gold', metalLabel: 'GF 14/20' })]), // E: designs
  order(4179000006, [line(41790000061, 'BUNNY5', 'Bunny Charm', [['Metal', 'Sterling Silver']], silver)]),       // F: skipped in its order window
  order(4179000007, [line(41790000071, 'BUNNY5', 'Bunny Charm', [['Metal', 'Sterling Silver']], Object.assign({ personalization: ['Ann'] }, silver))]) // G: engraving
];
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.Arrivals && window.OrderTimeline && window.CNTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // 1 · the setup: the sorter writes as itself, and a name set anywhere is the one the next event carries
    const cfg = await page.evaluate(() => { const a = OrderTimeline.config(); B.employee = 'Bea'; const b = OrderTimeline.config().by; B.employee = 'Test Operator'; return { a, b, scriptOrder: [...document.scripts].map(s => (s.getAttribute('src') || '').split('?')[0]).filter(s => /order-timeline|charm-nest-bridge/.test(s)) }; });
    assert.deepEqual(cfg.scriptOrder, ['order-timeline.js', 'charm-nest-bridge.js'], 'the timeline is loaded before the bridge');
    assert.equal(cfg.a.mode, 'sorter'); assert.equal(cfg.a.sandbox, false); assert.equal(cfg.a.by, 'Test Operator'); assert.equal(cfg.a.passcode, '');
    assert.equal(cfg.b, 'Bea', 'the employee is kept current');
    console.log('  ✓ OrderTimeline configured as the sorter, kept current');

    // the spy, and the page's own slow neighbours stubbed: no station here to write staff notes to
    await page.evaluate(async () => {
      await Master.load(true).catch(() => {});   // (the library the page read at boot, then two charms of our own in it)
      window.__tl = []; const real = OrderTimeline.record;
      OrderTimeline.record = e => { const out = real(e); window.__tl.push(JSON.parse(JSON.stringify(out || e))); return out; };   // (as queued: who from the config)
      const call = DesignLink.call; DesignLink.call = (type, args, o) => type === 'notes.set' ? Promise.resolve({ ok: true }) : call(type, args, o);
      for (const s of ['SEA_TURTLE', 'BUNNY5']) B.master.entries.set(s, { sku: s });
    });
    const events = () => page.evaluate(async () => { for (let i = 0; i < 100 && CNTimeline.pending(); i++) await new Promise(r => setTimeout(r, 30)); return window.__tl; });
    const of = (list, type, rid) => list.filter(e => e.type === type && (!rid || e.orderId === rid));
    const until = (type, rid, n = 1) => page.waitForFunction(({ type, rid, n }) => !CNTimeline.pending() && window.__tl.filter(e => e.type === type && e.orderId === rid).length >= n, { type, rid, n }, { timeout: 30000 });

    // 2 · the orders enter the pull (as an arrival does) and are read
    await page.evaluate(async orders => { await Orders.loadMaps(true); await Arrivals.merge(orders); CN.setMode('review'); Review.render(); }, ORDERS);
    let ev = await events();
    for (const o of ORDERS) {
      assert.equal(of(ev, 'pulled', o.receiptId).length, 1, `${o.receiptId} pulled once`);
      assert.equal(of(ev, 'interpreted', o.receiptId).length, 1, `${o.receiptId} read once`);
    }
    const readA = of(ev, 'interpreted', '4179000001')[0];
    assert.equal(readA.lineKey, '4179000001_41790000011'); assert.equal(readA.transactionId, '41790000011'); assert.equal(readA.data.sku, 'SEA_TURTLE'); assert.deepEqual(readA.data.questions, ['needsMaterial']);
    assert.match(readA.text, /^Read as SEA_TURTLE · metal not read · 1 question$/);
    for (const rid of ['4179000001', '4179000002', '4179000003']) assert.deepEqual(of(ev, 'needsDecision', rid).map(e => e.data.kind), ['needsMaterial'], rid + ' needs its material');
    assert.deepEqual(of(ev, 'needsDecision', '4179000004').map(e => e.data.kind), ['unmatchedSku']);
    assert(of(ev, 'needsDecision', '4175423829').every(e => e.data.custom), 'a custom order line asks under Custom Orders');
    assert.equal(of(ev, 'needsDecision', '4179000006').length, 0, 'a clean line asks nothing');
    assert(ev.every(e => e.by === 'Test Operator' && e.at > 1e12 && e.id), 'who, when and a stable id');
    console.log(`  ✓ pulled, read and questions raised: ${ev.length} events`);

    // 3 · re-rendering, re-reading and the same arrivals again record nothing new
    const n0 = ev.length;
    await page.evaluate(async orders => { for (let i = 0; i < 3; i++) { Orders.interpretAll(); Review.syncOrderItems(); Review.render(); Orders.render(); } await Arrivals.merge(orders); }, ORDERS);
    ev = await events(); assert.equal(ev.length, n0, 'no events on a re-render: ' + JSON.stringify(ev.slice(n0).map(e => [e.type, e.orderId])));

    // 4 · the answers, through the cards' own buttons
    const act = (rid, kind, action, set) => page.evaluate(({ rid, kind, action, set }) => {
      const it = Review.items().find(x => x.kind === kind && (x.rows && x.rows.length ? x.rows : [x.row]).some(r => r.order.receiptId === rid));
      if (!it) throw new Error(`no ${kind} card for ${rid}`);
      const c = Review.card(it); c.style.display = 'none'; document.body.appendChild(c);
      for (const [f, v] of Object.entries(set || {})) { const n = c.querySelector(`[data-f=${f}]`); n.value = v; n.dispatchEvent(new Event('input')); n.dispatchEvent(new Event('change')); }
      c.querySelector(`[data-a=${action}]`).click();
    }, { rid, kind, action, set });
    await act('4179000001', 'needsMaterial', 'mat', { mat: 'gold' });
    await until('decided', '4179000001');
    await act('4179000002', 'needsMaterial', 'skip');
    await until('skipped', '4179000002');
    await act('4179000003', 'needsMaterial', 'hold');
    await until('held', '4179000003');
    await page.evaluate(() => Review.repool(B.orders.byKey.get('4179000003_41790000031')));   // Release hold (Orders › On hold)
    await until('restored', '4179000003');
    await act('4179000004', 'unmatchedSku', 'nodesign');
    await until('decided', '4179000004');
    // skipped from the order window (no card): the line is skipped and the queue synced, as toggleSkip does
    await page.evaluate(() => { const r = B.orders.byKey.get('4179000006_41790000061'); r.state = 'skipped'; r.reason = 'piece skipped by Test Operator'; r.problems = []; r.hold = r.reason; Review.syncOrderItems(); });
    await until('skipped', '4179000006');
    ev = await events();
    const dA = of(ev, 'decided', '4179000001')[0];
    assert.equal(dA.data.kind, 'needsMaterial'); assert.equal(dA.data.material, 'gold'); assert.equal(dA.lineKey, '4179000001_41790000011'); assert.equal(dA.by, 'Test Operator');
    assert.match(dA.text, /^Decided by Test Operator · Options · metal: material /);
    const sB = of(ev, 'skipped', '4179000002')[0]; assert.equal(sB.data.kind, 'needsMaterial'); assert.equal(sB.data.from, 'review');
    const hC = of(ev, 'held', '4179000003')[0]; assert.equal(hC.data.kind, 'needsMaterial'); assert.equal(hC.data.answer, 'Hold order');
    const rC = of(ev, 'restored', '4179000003')[0]; assert.equal(rC.data.was, 'held by Test Operator');
    const dD = of(ev, 'decided', '4179000004')[0]; assert.equal(dD.data.kind, 'unmatchedSku'); assert.equal(dD.data.answer, 'Nothing to cut'); assert.equal(dD.data.noDesign, true);
    assert.equal(of(ev, 'skipped', '4179000006')[0].data.from, 'order');
    for (const t of ['decided', 'skipped', 'held']) assert.equal(of(ev, t, '4179000006').concat(of(ev, t, '4179000007')).filter(e => t !== 'skipped' || e.orderId !== '4179000006').length, 0, 'no answer where none was given');
    console.log('  ✓ decided (material, nothing to cut), skipped (Review, order window), held and restored');

    // 5 · the back engraving: read as needed, then its words decided
    await page.evaluate(async () => { const r = B.orders.byKey.get('4179000007_41790000071'); r.state = 'pooled'; await Engrave.classify(r); });
    await until('engraveNeeded', '4179000007');
    await page.evaluate(async () => { await Engrave.decideWords(Engrave.jobOf(B.orders.byKey.get('4179000007_41790000071')), { text: 'Anna', note: 'edited' }); });
    await until('engraveChanged', '4179000007');
    ev = await events();
    const eg = of(ev, 'engraveNeeded', '4179000007')[0]; assert.equal(eg.data.text, 'Ann'); assert.equal(eg.lineKey, '4179000007_41790000071');
    const ec = of(ev, 'engraveChanged', '4179000007')[0]; assert.equal(ec.data.text, 'Anna'); assert.equal(ec.data.was, 'Ann'); assert.equal(ec.data.note, 'edited');
    console.log('  ✓ engraveNeeded and the words decision (engraveChanged)');

    // 6 · a custom order's design dropped on its card and sent to the sheets
    await page.evaluate(dxf => { const it = Review.customItemFor('4175423829_41754238291'); CustomSheet.add(it, [new File([dxf], 'heart-name.dxf')]); }, DESIGN_DXF);
    await page.waitForFunction(() => { const e = Object.values(CustomSheet.entries())[0]; return e && e.files.every(F => F.state === 'ready'); }, null, { timeout: 30000 });
    await page.evaluate(async () => { const it = Review.customItemFor('4175423829_41754238291'); await CustomSheet.send(it); CustomSheet.shut(); });
    await until('designSent', '4175423829');
    ev = await events();
    const dd = of(ev, 'designDropped', '4175423829'); assert.equal(dd.length, 1); assert.equal(dd[0].data.files[0].name, 'heart-name.dxf'); assert.match(dd[0].text, /^Design added: heart-name\.dxf$/);
    const ds = of(ev, 'designSent', '4175423829'); assert.equal(ds.length, 1); assert.equal(ds[0].lineKey, '4175423829_41754238291'); assert.equal(ds[0].data.pieces, 1); assert.equal(ds[0].data.files[0].metal, 'gold');
    console.log('  ✓ designDropped and designSent');

    // 7 · no duplicates, however often the page draws again; nothing the server stamps; the events reach the cloud
    const n1 = ev.length;
    await page.evaluate(() => { for (let i = 0; i < 3; i++) { Orders.interpretAll(); Review.syncOrderItems(); Review.render(); Orders.render(); } });
    ev = await events(); assert.equal(ev.length, n1, 'no events on a re-render: ' + JSON.stringify(ev.slice(n1).map(e => [e.type, e.orderId])));
    const ids = ev.map(e => `${e.orderId}~${e.type}~${e.id}`); assert.equal(new Set(ids).size, ids.length, 'every event once');
    const SERVER = ['sealPrinted', 'sealCompleted', 'laserDone', 'engraveApproved', 'removed', 'moved', 'customDecided', 'roseCut', 'arrived', 'cancelled', 'etsyCancelled', 'cancelRestored'];
    assert.deepEqual(ev.filter(e => SERVER.includes(e.type)).map(e => e.type), [], 'no server-stamped type from the page');
    await page.evaluate(() => OrderTimeline.flush());
    await page.waitForFunction(() => !OrderTimeline.pending(), null, { timeout: 30000 });
    // (the server stamps its own events in the same store: placed, arrived, removed …; count the page's)
    const saved = srv.st.list('Order_Timeline').filter(d => !SERVER.includes(d.type) && !['placed', 'setCommitted'].includes(d.type));
    assert.equal(saved.length, ids.length, `every event written once (${saved.length} of ${ids.length})`);
    assert(saved.some(d => d.type === 'decided' && d.orderId === '4179000001' && d.by === 'Test Operator'), 'a decision in the cloud with who made it');
    assert.equal(srv.st.list('Sandbox_Order_Timeline').length, 0, 'production, not the sandbox');
    assert.deepEqual(errors, [], 'no page errors');
    const count = {}; for (const e of ev) count[e.type] = (count[e.type] || 0) + 1;
    console.log('  ✓ no duplicates, none of the server\'s types, written to Order_Timeline: ' + JSON.stringify(count));
  } finally { await browser.close(); srv.close(); }
  console.log('Timeline events (Review, engraving, custom designs) OK');
})().catch(e => { console.error(e); process.exit(1); });
