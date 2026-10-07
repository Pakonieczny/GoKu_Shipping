// The Sorting station is tracked end to end (stations round 3, ST-SORT; Paul, 7 Oct 2026: "I do not see any record for the sorting application in
// terms of any login data and any QR code print that should be recorded per each order when the QR code is printed, and this should actually update
// the whole system as to the status of a given order"). The REAL sorting.html and sorting-2.html run in Chromium over the all-stations fake shop
// (tests/stations/all-stations: the real station door firebaseOrders, the real efficiency reader employeeEfficiency and the sorter's own timelineGet over
// one in-memory Firestore). Nothing leaves this machine, no Etsy, no paid call, no printer (the QR Printer's PDF maker is a stand-in).
//   1 · sorting.html, nobody signed in: Print Row and a sticker's Print press are held (no label, nothing written, the chip's field opens); Escape drops
//       the held press; a 6-digit number typed into the field signs in (a Station_Sessions record: person, station sorting, device sorting-1, no number
//       anywhere) and the held sticker prints by itself: ONE print and ONE complete (orders 1) event with that person at Sorting, and the order's
//       timeline carries `sorted` and `labelPrinted` (label orderQR) by that person at station sorting; the sorter's timelineGet says the order is Sorted
//       by that person; the console reader (live and overview) shows the person at Sorting and the order and its pieces under Sorting.
//   2 · the same sticker again: a second print event, no second complete, the Sorting pieces and completions stay 1 (nothing counted twice).
//   3 · sorting-2.html: the same hold and resume with another person; and the sticker now stamps the order's timeline (it stamped nothing before):
//       `sorted` and `labelPrinted` at device sorting-2.
//   4 · no PIN in any request, response or record; no request outside the fake; nothing paid; the writes of one printed order are one activity batch and
//       one timeline batch (nothing per keystroke).
//   node tests/stations/sorting-tracking.cjs        (CHROMIUM=... PW_DIR=...)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || '';
const pwCore = (() => { for (const d of [pwDir, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules']) { if (!d) continue; try { return require(path.join(d, 'playwright-core')); } catch (_) {} } return require('playwright-core'); })();
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : undefined; } catch (_) { return undefined; } })();
const { World, sleep } = require('./all-stations/world.cjs');
const { Apps, until } = require('./all-stations/apps.cjs');
const { makeCast, rosterOf, PEOPLE } = require('./all-stations/cast.cjs');
const FX = require('./all-stations/fixtures.cjs');
const R = FX.R;
const refuseNested = require('../charm-nest/_noNestedArrays.cjs');
const say = s => process.stdout.write(s + '\n');

(async () => {
  const browser = await pwCore.chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const W = await World.start({ now: '2026-10-07T13:00:00.000Z', browser });      // 09:00 in Toronto
  let failed = null;
  try {
    const cast = makeCast();
    await W.post('/__ctl/roster', rosterOf(cast));
    const A = Apps(W, cast);
    const hasPin = text => Object.values(cast.pins).some(p => new RegExp('(^|\\D)' + p + '(\\D|$)').test(text));
    const docs = async c => { const l = await W.list(c); l.forEach(d => refuseNested(d, c + '/' + d._id)); return l; };
    const eventsOf = async rid => (await docs('Station_Activity')).filter(e => e.orderId === rid).sort((a, b) => (a.at || 0) - (b.at || 0));
    const stampsOf = async rid => (await docs('Order_Timeline')).filter(e => e.orderId === rid && /^(sorted|labelPrinted)$/.test(e.type));
    const waitFor = (fn, what, ms) => until(fn, what, ms || 25000);
    const frames = page => page.evaluate(() => [...document.querySelectorAll('iframe')].filter(f => /QR/.test(f.src)).length);
    const cellOf = (page, rid) => page.evaluate(r => window.cachedOrderItems.findIndex(t => String(t.typedOrderNumber || t.receipt_id) === r), rid);
    const chipOpen = page => page.evaluate(() => { const i = document.querySelector('#sortingAsChip .st-as-input'); return !!i && !i.hidden; });
    const flush = async page => { await page.evaluate(() => Promise.all([window.StationActivity && StationActivity.flush(), window.OrderTimeline && OrderTimeline.flush()])).catch(() => {}); await sleep(400); };
    const countCalls = async (from, op) => (await W.calls(from)).calls.filter(c => c.op === op).length;
    const sorterTimeline = async rid => (await fetch(W.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ op: 'timelineGet', orderId: rid }) })).json();
    const typeInChip = async (page, who) => { await page.keyboard.type(cast.pins[who]); await page.keyboard.press('Enter'); };     // (the six digits are typed into the page's own field and never printed)
    const overview = async () => W.eff({ op: 'overview', day: '2026-10-07', days: 1 });

    /* ───────────────────────── 1 · sorting.html ───────────────────────── */
    const maya = PEOPLE.sort, omar = PEOPLE.sortSmoke;
    const ctx1 = await W.context({ label: 'sorting-1' });
    const p1 = await A.sorting(ctx1, 'sorting.html', maya, { signIn: false });
    await A.sortBatch(p1, [R.sort1, R.sort3]);      // (a batch of two: one is printed, so the batch stays in hand on the live board)
    const i1 = await cellOf(p1, R.sort1);
    assert(i1 >= 0, 'the order is on screen');
    const mark0 = (await W.calls(0)).total;

    // Print Row with nobody signed in: held (no batch handed to the printer), the chip's field opens
    await p1.evaluate(() => { window.__rowDone = false; handlePrintRowClick().then(() => { window.__rowDone = true; }); });
    await sleep(900);
    assert.strictEqual(await p1.evaluate(() => localStorage.getItem('qrPrintBatch')), null, 'Print Row with nobody signed in hands nothing to the printer');
    assert.strictEqual(await frames(p1), 0, 'and opens no printer frame');
    assert(await chipOpen(p1), 'the name field is open, asking');
    // Escape drops the held press (a toast says so); nothing was recorded
    await p1.keyboard.press('Escape');
    await sleep(300);
    assert(!(await chipOpen(p1)), 'Escape closes the field');
    assert((await p1.evaluate(() => (window.__toasts || []).concat([...document.querySelectorAll('.toast')].map(t => t.textContent)).join(' | '))).includes('Not printed'), 'and says the sticker was not printed');
    // a sticker's Print press with nobody signed in: held the same way
    await p1.evaluate(n => openIframePrinterForListing(n), i1);
    await sleep(900);
    assert.strictEqual(await frames(p1), 0, 'a sticker with nobody signed in is not printed');
    assert(await chipOpen(p1), 'the name field is open again, asking');
    assert.strictEqual((await docs('Station_Sessions')).length, 0, 'nobody signed in: no session yet');
    assert.deepStrictEqual(await eventsOf(R.sort1), [], 'nothing recorded for the order yet');
    assert.deepStrictEqual(await stampsOf(R.sort1), [], 'and nothing on its timeline');
    say('sorting.html: Print Row and Print with nobody signed in are held, the chip asks, Escape drops the press');

    // the person types the six digits: signed in, and the held sticker prints by itself
    await p1.focus('#sortingAsChip .st-as-input');
    await typeInChip(p1, maya);
    await waitFor(() => frames(p1).then(n => n >= 1), 'the held sticker to print');
    await waitFor(() => p1.evaluate(n => !!StationActivity.who(), 0), 'the sign-in');
    await flush(p1);
    const ses = (await docs('Station_Sessions')).filter(s => s.person === maya);
    assert.strictEqual(ses.length, 1, 'one sign-in session for the person');
    assert.deepStrictEqual([ses[0].station, ses[0].device, ses[0].endAt == null], ['sorting', 'sorting-1', true], 'at Sorting, device sorting-1, still open');
    assert(!hasPin(JSON.stringify(ses)), 'the session holds no number');
    await waitFor(async () => (await eventsOf(R.sort1)).length >= 3 && (await stampsOf(R.sort1)).length >= 2, 'the print records');
    let ev = await eventsOf(R.sort1);
    assert.deepStrictEqual(ev.map(e => [e.action, e.person, e.station, e.device, e.parts, e.orders]), [['scan', maya, 'sorting', 'sorting-1', 1, 0], ['print', maya, 'sorting', 'sorting-1', 1, 0], ['complete', maya, 'sorting', 'sorting-1', 1, 1]],
      'the batch typed before the sign-in is the person\'s scan, then one print and one completion of the order, by that person at Sorting');
    const stamps = await stampsOf(R.sort1);
    assert.deepStrictEqual(stamps.map(e => e.type).sort(), ['labelPrinted', 'sorted'], 'the order timeline got sorted and labelPrinted');
    assert(stamps.every(e => e.by === maya && e.station === 'sorting' && e.device === 'sorting-1'), 'both by that person, at station sorting');
    assert.strictEqual(stamps.find(e => e.type === 'labelPrinted').data.label, 'orderQR', 'the label is the order QR sticker');
    assert(stamps.every(e => !(e.data && e.data.signedIn === false)), 'and not marked "not signed in"');
    await waitFor(async () => ((await A.qrFrame(p1)) || {}).made >= 1, 'the QR Printer frame to make the label', 15000);
    say('sorting.html: sign-in session, one print + one complete event, sorted + labelPrinted stamped by the person');

    // the sorter's own read of the order says Sorted, by that person
    const tl = await sorterTimeline(R.sort1);
    assert.strictEqual(tl.where.stage, 'sorted', 'the order status is Sorted');
    assert.strictEqual(tl.where.label, 'Sorted');
    assert.strictEqual(tl.where.by, maya, 'by the person at the station');
    assert.strictEqual(tl.where.station, 'sorting');
    assert(tl.where.step >= 4, 'the rail has moved to the Sorted step');
    say('sorter: timelineGet → stage sorted by ' + tl.where.by + ' at ' + tl.where.station);

    // the console reader: the person is at Sorting now and the order and pieces are under Sorting
    let ov = await overview();
    assert.strictEqual(ov.ok, true);
    const pm = (ov.people || []).find(p => p.name === maya);
    assert(pm, 'the person is in the console\'s People');
    assert.strictEqual(pm.status, 'on'); assert(pm.nowAt.includes('sorting'), 'signed in now at Sorting');
    const st1 = (pm.stations || []).find(s => s.station === 'sorting');
    assert(st1 && st1.orders === 2 && st1.parts === 1 && st1.prints === 1 && st1.completes === 1 && st1.scans === 2, 'Sorting row of the person: 2 orders worked (the batch), 1 piece made, 1 print, 1 completion: ' + JSON.stringify(st1));
    const live = await W.eff({ op: 'live' });
    assert.strictEqual(live.ok, true);
    const lst = (live.stations || []).find(s => s.key === 'sorting');
    assert(lst, 'the live board has the Sorting station');
    assert(JSON.stringify(lst).includes(maya), 'and shows the person signed in at Sorting');
    assert(JSON.stringify(lst).includes('sorting-1'), 'at its page sorting-1');
    await waitFor(async () => JSON.stringify(((await W.eff({ op: 'live' })).stations || []).find(s => s.key === 'sorting')).includes('Batch of 2 orders'), 'the batch in hand on the live board (Now working on)', 30000);
    say('console: ' + maya + ' signed in at Sorting, 2 orders worked / 1 piece made / 1 print under Sorting, the batch in hand on the live board');

    /* ───────────────────────── 2 · the same sticker again ───────────────────────── */
    const markAgain = (await W.calls(0)).total;
    await p1.evaluate(n => openIframePrinterForListing(n), i1);
    await waitFor(() => frames(p1).then(n => n >= 2), 'the second printer frame');
    await flush(p1);
    await waitFor(async () => (await eventsOf(R.sort1)).length >= 4, 'the second print event');
    ev = await eventsOf(R.sort1);
    assert.deepStrictEqual(ev.map(e => e.action), ['scan', 'print', 'complete', 'print'], 'a reprint is a print only');
    assert.strictEqual((await eventsOf(R.sort1)).filter(e => e.action === 'complete').length, 1, 'the order is completed once');
    const sortingOf = async who => { ov = await overview(); const pp = (ov.people || []).find(p => p.name === who); return pp && (pp.stations || []).find(s => s.station === 'sorting'); };
    await waitFor(async () => { const x = await sortingOf(maya); return x && x.prints === 2; }, 'the reader to count the second print (it keeps an answer a few seconds)', 30000);
    const st2 = await sortingOf(maya);
    assert(st2.orders === 2 && st2.parts === 1 && st2.prints === 2 && st2.completes === 1 && st2.scans === 2, 'the rollup: still 1 piece made and 1 completion, 2 prints, the batch\'s 2 orders worked: ' + JSON.stringify(st2));
    const writes = (await W.calls(markAgain)).calls;
    assert(writes.filter(c => c.op === 'activity').length <= 1 && writes.filter(c => c.op === 'timeline').length <= 1, 'a reprint is at most one activity batch and one timeline batch');
    say('sorting.html: the same sticker again = a second print, no second completion, the order count stays 1');

    /* ───────────────────────── 3 · sorting-2.html ───────────────────────── */
    const ctx2 = await W.context({ label: 'sorting-2' });
    const p2 = await A.sorting(ctx2, 'sorting-2.html', omar, { signIn: false });
    await A.sortBatch(p2, [R.sort2]);
    const i2 = await cellOf(p2, R.sort2);
    await p2.evaluate(n => openIframePrinterForListing(n), i2);
    await sleep(900);
    assert.strictEqual(await frames(p2), 0, 'sorting-2: a sticker with nobody signed in is not printed');
    assert(await chipOpen(p2), 'sorting-2: the chip asks');
    assert.deepStrictEqual(await eventsOf(R.sort2), [], 'sorting-2: nothing recorded yet');
    await p2.focus('#sortingAsChip .st-as-input');
    await typeInChip(p2, omar);
    await waitFor(() => frames(p2).then(n => n >= 1), 'sorting-2: the held sticker to print');
    await waitFor(() => p2.evaluate(() => !!StationActivity.who()), 'sorting-2: the sign-in');
    await flush(p2);
    await waitFor(async () => (await eventsOf(R.sort2)).length >= 3 && (await stampsOf(R.sort2)).length >= 2, 'sorting-2: the print records');
    ev = await eventsOf(R.sort2);
    assert.deepStrictEqual(ev.map(e => [e.action, e.person, e.station, e.device, e.parts, e.orders]), [['scan', omar, 'sorting', 'sorting-2', 2, 0], ['print', omar, 'sorting', 'sorting-2', 2, 0], ['complete', omar, 'sorting', 'sorting-2', 2, 1]], 'sorting-2: the batch, one print and one completion, two pieces');
    const s2 = await stampsOf(R.sort2);
    assert.deepStrictEqual(s2.map(e => e.type).sort(), ['labelPrinted', 'sorted'], 'sorting-2: the order timeline now gets sorted and labelPrinted');
    assert(s2.every(e => e.by === omar && e.station === 'sorting' && e.device === 'sorting-2'), 'sorting-2: by that person, station sorting, device sorting-2');
    assert.strictEqual((await sorterTimeline(R.sort2)).where.stage, 'sorted', 'sorting-2: the order status is Sorted');
    const ses2 = (await docs('Station_Sessions')).filter(s => s.person === omar);
    assert(ses2.length === 1 && ses2[0].device === 'sorting-2' && ses2[0].station === 'sorting', 'sorting-2: its sign-in session');
    await waitFor(async () => { const x = await sortingOf(omar); return x && x.orders === 1; }, 'sorting-2: the reader to count the order', 30000);
    const so = await sortingOf(omar), po = (ov.people || []).find(p => p.name === omar);
    assert(po && po.status === 'on' && po.nowAt.includes('sorting') && so.orders === 1 && so.parts === 2, 'sorting-2: in the console at Sorting with 1 order and 2 pieces: ' + JSON.stringify(so));
    say('sorting-2.html: held, signed in, one print + one complete, sorted + labelPrinted now stamped at sorting-2');

    /* ───────────────────────── 4 · safety, cost ───────────────────────── */
    const stats = await W.stats();
    assert.strictEqual(stats.pinLeaks, 0, 'no number left a request or a response (but to the login door)');
    assert.deepStrictEqual(stats.egress, [], 'nothing left the machine');
    assert.strictEqual(stats.paid, 0, 'nothing paid');
    assert.strictEqual(W.outside.length, 0, 'no request outside the fake shop: ' + JSON.stringify(W.outside.slice(0, 3)));
    const all = JSON.stringify([await docs('Station_Sessions'), await docs('Station_Activity'), await docs('Order_Timeline')]);
    assert(!hasPin(all), 'no number in any stored record');
    const posts = (await W.calls(mark0)).calls;
    const per = op => posts.filter(c => c.op === op).length;
    say('door requests since the first press: activity ' + per('activity') + ', timeline ' + per('timeline') + ', session ' + per('session') + ', live ' + per('live') + ', pinLogin ' + per('pinLogin'));
    assert(per('live') >= 1, 'the batch in hand reached the live board');
    assert(per('timeline') <= 6 && per('activity') <= 8, 'a handful of writes for three printed stickers, none per keystroke');
    for (const p of W.pages) assert.deepStrictEqual(p.__errs.page, [], 'no page error on ' + p.__file);
    say('PASS: the Sorting station is tracked end to end');
  } catch (e) { failed = e; }
  try { await browser.close(); } catch (_) {}
  W.stop();
  if (failed) { process.stderr.write('FAIL: ' + (failed && failed.stack || failed) + '\n'); process.exit(1); }
  process.exit(0);
})();
