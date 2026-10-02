// Station tracking, part I (Paul, 28 Sep 23:51): the sorter's rail, seals and step card read who did each step and where,
// from fake station events only. Server: whereOf never moves the rail on a label printed at Sorting or the Design Station
// (it used to reach Assembled), a shipping label stays with Shipped, a scan is "seen at", never a milestone. Browser: a
// blank page with order-timeline.js and charm-nest-timeline-ui.js (OrderTimeline.get stubbed): the rail's steps name the
// person and the station, a completion with nobody signed in says "not signed in", the Sorted card says where the label
// was printed and by whom (or "Label not printed yet"), and the rail moves on live when a station's event arrives.
// Nothing leaves the machine: a loopback server for the two scripts, every other request aborted.
//   node tests/charm-nest/st-i.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), http = require('http'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

const A = '4300000001', B = '4300000002', H = 36e5, T0 = Date.now() - 3 * 24 * H;
const ev = (o, type, h, x) => Object.assign({ id: `${o}~${type}~${h}`, orderId: o, type, at: T0 + h * H, by: '', source: 'station', station: '', device: '', text: '' }, x || {});
// A: a stud order, nested, cut, its QR label printed at the Design Station, seen and sorted at Sorting with nobody signed
// in, its label printed there again by Ana
const evA = [
  ev(A, 'arrived', 0, { by: 'Etsy', source: 'etsy' }),
  ev(A, 'placed', 2, { by: 'Paul', source: 'sorter', station: 'sorter', sheet: 'GF Sheet 2', sheetId: 'shG2' }),
  ev(A, 'laserDone', 20, { by: 'Marco R.', source: 'sorter', station: 'laser', sheet: 'GF Sheet 2', sheetId: 'shG2' }),
  ev(A, 'labelPrinted', 21, { by: 'Yuki S.', station: 'design', device: 'design-1', data: { label: 'orderQR', employeeId: 'emp-7' } }),
  ev(A, 'scan', 30, { by: 'Ana P.', station: 'sorting', device: 'sorting', text: 'Scanned at Sorting' }),
  ev(A, 'sorted', 30.2, { by: '', station: 'sorting', device: 'sorting', data: { signedIn: false } }),
  ev(A, 'labelPrinted', 30.3, { by: 'Ana P.', station: 'sorting', device: 'sorting', data: { label: 'orderQR', printer: 'Zebra' } })
];
// B: sorted by Ana, no label printed anywhere
const evB = [ev(B, 'arrived', 0, { by: 'Etsy', source: 'etsy' }), ev(B, 'laserDone', 10, { by: 'Marco R.', source: 'sorter', station: 'laser' }), ev(B, 'sorted', 12, { by: 'Ana P.', station: 'sorting', device: 'sorting' })];

// ── the server's rail ──
{
  const lbl = [evA[0], evA[1], evA[2], evA[3]];
  let w = Timeline.whereOf(lbl, null);
  assert.equal(w.step, 3, 'a label printed at the Design Station moves the rail nowhere (it reached Assembled): ' + w.step);
  assert.equal(w.stage, 'cut');
  w = Timeline.whereOf(lbl.concat(ev(A, 'labelPrinted', 22, { by: 'Ana P.', station: 'sorting', data: { label: 'sheetQR' } })), null);
  assert.equal(w.step, 3, 'nor one at the Sorting station');
  w = Timeline.whereOf(lbl.concat(ev(A, 'labelPrinted', 40, { by: 'Dana K.', station: 'shipping', data: { label: 'shipping' } })), null);
  assert.equal(w.step, 6, 'a shipping label: the order is at Shipping, past Assembled'); assert.equal(w.stage, 'packed');
  assert.equal(Timeline.labelStepOf({ station: 'shipping', data: { label: 'shipping' } }), 'shipped');
  assert.equal(Timeline.labelStepOf({ station: 'design', data: {} }), 'sorted');
  w = Timeline.whereOf(evA.slice(0, 3).concat(evA[4]), null);
  assert.equal(w.step, 3, 'a scan is not a step'); assert.match(w.text, /seen at sorting/, w.text);
  assert.equal(Timeline.whereOf(evA, null).step, 4, 'sorted: step 4');
  assert.equal(Timeline.clean({ orderId: A, type: 'scan', milestone: true }).doc.milestone, false, 'a scan is never a milestone');
  assert.equal(Timeline.clean({ orderId: A, type: 'labelPrinted', station: 'design' }).doc.milestone, false, 'a label print is a detail, not a milestone');
  assert.equal(Timeline.clean({ orderId: A, type: 'labelPrinted', station: 'design', by: '', data: { signedIn: false } }).doc.station, 'design');
  console.log('  ✓ server: label prints at Sorting/Design stay the Sorted step\'s detail (rail stays at Laser cut), a shipping label is past Assembled, a scan is "seen at", neither a milestone');
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const files = { '/order-timeline.js': 'order-timeline.js', '/charm-nest-motion.js': 'charm-nest-motion.js', '/charm-nest-timeline-ui.js': 'charm-nest-timeline-ui.js' };   // (the seal zoom lives in the motion module)
  const srv = http.createServer((q, r) => {
    const u = q.url.split('?')[0];
    if (u === '/') { r.writeHead(200, { 'Content-Type': 'text/html' }); r.end('<!doctype html><meta charset="utf-8"><body style="margin:0"><div id="host" style="width:1400px;min-height:700px;padding-top:200px"></div><script src="/order-timeline.js"></script><script src="/charm-nest-motion.js"></script><script src="/charm-nest-timeline-ui.js"></script>'); return; }
    if (files[u]) { r.writeHead(200, { 'Content-Type': 'text/javascript' }); r.end(fs.readFileSync(path.join(root, files[u]))); return; }
    r.writeHead(404); r.end('{}');
  });
  await new Promise(ok => srv.listen(0, '127.0.0.1', ok));
  const origin = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    await ctx.route(() => true, r => { const h = new URL(r.request().url()).hostname; return h === '127.0.0.1' || h === 'localhost' ? r.continue() : r.abort(); });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(origin + '/');
    await page.waitForFunction(() => window.OrderTimelineUI && window.OrderTimeline);
    await page.evaluate(({ A, B, evA, evB }) => {
      window.__fx = { [A]: evA, [B]: evB }; window.__more = [];
      OrderTimeline.get = async id => ({ orderId: id, events: JSON.parse(JSON.stringify(window.__fx[id].concat(id === Object.keys(window.__fx)[0] ? window.__more : []))), cancelled: null, where: null });
      window.__tl = OrderTimelineUI.mount(document.getElementById('host'), { orderId: A, live: true, pollMs: 400, stages: () => OrderTimelineUI.stagesFor([{ title: 'Stud earrings' }]) });
    }, { A, B, evA, evB });
    await page.waitForFunction(() => document.querySelectorAll('.tlStop.d').length >= 5);
    const rail = () => page.evaluate(() => Object.fromEntries([...document.querySelectorAll('.tlStop')].map(n => [n.dataset.stage, { d: n.classList.contains('d'), say: n.getAttribute('aria-label') }])));
    let r = await rail();
    assert(r.sorted.d && !r.welded.d && !r.assembled.d, 'sorted, not welded, not assembled (no label print reaches Assembled): ' + JSON.stringify(r));
    assert.match(r.laser.say, /^Laser cut · Marco R\. · Laser · done /, r.laser.say);
    assert.match(r.sheet.say, /^Nested · Paul · Sorter · done /, r.sheet.say);
    assert.match(r.sorted.say, /^Sorted · Not signed in · Sorting · done /, r.sorted.say);

    // the Sorted seal, rested on (it grows where it stands): its face says SORTED, its accessible name the person (nobody
    // signed in) and the station; its card says where the label was printed and by whom
    await page.hover('.tlStop[data-stage="sorted"]'); await page.waitForTimeout(1100);
    r = await page.evaluate(() => { const S = document.querySelector('.tlStop[data-stage="sorted"] .tlSeal'), N = S.closest('.tlStop'); return { grown: !!S.dataset.sealZoom, copy: !!document.querySelector('.tlLoupe,.tlNowZoom,.sealLens'), face: [...S.querySelectorAll('text')].map(t => t.textContent).join(' | '), aria: N.getAttribute('aria-label') || '', card: (document.querySelector('.tlExp.on') || {}).textContent || '' }; });
    assert(r.grown && !r.copy, 'the seal itself grows'); assert.match(r.face, /SORTED/); assert.match(r.aria, /Not signed in/); assert.match(r.aria, /Sorting/);
    assert.match(r.card, /Sorted · Not signed in · Sorting/, r.card);
    assert.match(r.card, /Label printed at the Sorting station by Ana P\./, r.card);
    assert.match(r.card, /Label printed at the Design Station by Yuki S\./, r.card);
    assert.match(r.card, /order QR label/); assert.doesNotMatch(r.card, /not printed yet|Part done/, r.card);
    console.log('  ✓ rail: each step says who and where ("Laser cut · Marco R. · Laser"); Sorted with nobody signed in is named Not signed in · Sorting for assistive technology; its card: label printed at the Sorting station by Ana P. and at the Design Station by Yuki S.');

    // the pure step card: no label anywhere; a shipping label under Shipped; a scan is "Seen at …"
    r = await page.evaluate(({ B, evA }) => {
      const q = OrderTimelineUI.requirementsOf('sorted', { events: window.__fx[B] });
      const ship = OrderTimelineUI.requirementsOf('shipped', { events: evA.concat({ id: 'x~labelPrinted~s', orderId: evA[0].orderId, type: 'labelPrinted', at: evA[6].at + 3e6, by: 'Dana K.', source: 'station', station: 'shipping', data: { label: 'shipping' } }) });
      const full = OrderTimelineUI.requirementsOf('sorted', { events: evA });
      return { state: q.state, need: q.need.map(n => n.t), done: q.done.map(d => d.t), ship: ship.done.map(d => d.t), facts: full.facts };
    }, { B, evA });
    assert.equal(r.state, 'done'); assert.deepEqual(r.need, ['Label not printed yet']); assert.deepEqual(r.done, ['Sorted · Ana P. · Sorting']);
    assert.deepEqual(r.ship, ['Shipping label printed at the Shipping station by Dana K.'], JSON.stringify(r.ship));
    assert(r.facts.some(f => /^Seen at Sorting · Ana P\. · /.test(f)), 'a scan: seen at its station, under its own step: ' + JSON.stringify(r.facts));
    console.log('  ✓ card: "Label not printed yet" (the step stays Done), a shipping label under Shipped, a scan is "Seen at Sorting · Ana P."');

    // live: a welding station's event and an assembly scan arrive with the next read; the rail moves on to Welded, and
    // the scan does not make it Assembled
    await page.mouse.move(5, 5);
    await page.evaluate(A => { window.__more = [
      { id: `${A}~welded~w`, orderId: A, type: 'welded', at: Date.now() - 6e5, by: 'Marco R.', source: 'station', station: 'welding', device: 'weld-1', data: { employeeId: 'emp-3' } },
      { id: `${A}~scan~a`, orderId: A, type: 'scan', at: Date.now() - 3e5, by: 'Luisa T.', source: 'station', station: 'assembly', device: 'assembly-2' }]; }, A);
    await page.waitForFunction(() => document.querySelector('.tlStop[data-stage="welded"]').classList.contains('d'), null, { timeout: 5000 });
    r = await rail();
    assert.match(r.welded.say, /^Welded · Marco R\. · Welding · done /, r.welded.say); assert(!r.assembled.d, 'a scan at Assembly is not Assembled');
    assert.equal(await page.evaluate(() => document.querySelectorAll('.tlSt[data-key^="scan"]').length), 0, 'no seal for a scan');
    // (every QR label printed is a seal of its own, Paul 29 Sep 02:08: the Design Station's and the Sorting station's)
    assert.equal(await page.evaluate(() => document.querySelectorAll('.tlSt[data-key^="labelPrinted"]').length), 2, 'a seal for each QR label printed');
    console.log('  ✓ live: the next read brings Welded · Marco R. · Welding onto the rail; the Assembly scan stays a fact, no seal');
    await page.evaluate(() => window.__tl.destroy());
    assert.deepEqual(errors, []);
  } finally { await browser.close(); srv.close(); }
  console.log('st-i: all passed');
})().catch(e => { console.error(e); process.exit(1); });
