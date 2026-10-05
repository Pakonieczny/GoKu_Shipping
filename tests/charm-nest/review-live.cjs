// The Review tab follows the cloud (Paul, 5 Oct 2026, round 6 point 2: "everything correctly, and in real time reflected in the
// review tab … If there's another user looking at the review tab while this is happening, then there should be a stamp
// animation shown and the animated order flying to the Completed tab"). TWO simulated computers, A and B, are two browser
// contexts (own storage, own name) on ONE in-memory backend (bridge-server.cjs: the real charmNestLibrary handler over a fake
// Firestore); every request off the loopback is aborted, nothing is printed, no Etsy call, no paid AI.
//   0 · before: the same page without the feed (its script not loaded) never sees B's completion; Orders.loadMaps is the only
//       way it learns (the Etsy order check, at least ten minutes apart)
//   1 · B presses Complete Order on a card while A watches Review → Open: A has it within the time budget, sees the stamp on the
//       card (the record's own seal: who, when), then the card flies to Completed, with the note; the count, the card in
//       Completed and its seal are right; B (who pressed) sees ONE stamp (no second one for their own change); three kinds of
//       card: a custom order that asks questions, a chain-only line that asks none, an Unknown SKU line (Complete Order on a
//       card of another tab)
//   2 · Reopen from B while A watches Completed: the card leaves Completed for the Open switch, its seal kept, with a note;
//       a print from another computer shows the print seal; a print again shows a new seal on the card, no flight
//   3 · hidden tab / another tab on screen: no read while hidden, no animation when it is shown again, the list and counts
//       are right; one quiet read when another tab is shown; the order window's timeline feed brings a change in at once
//   4 · a page load does not replay what is already done
//   5 · double completion (A and B press Complete Order together) ends in one card, the same seals on both
//   6 · a failed write rolls the UI back with the existing error toast and A sees nothing; a failing feed backs off, says
//       nothing and catches up quietly
//   7 · a rapid complete, reopen, complete from B (and a Reopen from A against it) never ends in a stuck or doubled card
//   8 · the cost: reads per hour while watched, none hidden, nothing drawn by an idle read; 300 cards still draw fast
//   node tests/charm-nest/review-live.cjs [playwright-core dir]   (CHROMIUM=<chrome>)
const fs = require('fs'), os = require('os'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, i, sku, title, price, extra) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY - i * 3600, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + i }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [Object.assign({ transactionId: String(rid) + '1', listingId: String(1800000100 + i), sku, title, quantity: 1, expectedShipDate: SHIP, variations: price ? [{ name: 'Price', value: String(price) }] : [{ name: 'Metal Choice', value: 'Gold Filled' }], metalKey: price ? '' : 'gold', metalLabel: price ? '' : 'GF 14/20', personalization: [], buyerMessage: '' }, extra || {})] });
const REWORK = i => order(4176576270 + i, i, 'RE_54' + (60 + i), 'MODIFICATION REWORK FREE SHIPPING', 100 + i);
const ORDERS = [0, 1, 2, 3, 4, 5, 6, 7].map(REWORK).concat([
  order(4179000001, 20, '', 'CABLE CHAIN ONLY RG', 18),                                    // chain only: asks nothing, never cut
  order(4175892473, 21, 'DAVID STAR', 'David Star Necklace', 0)                           // Unknown SKU: a card of another tab
]);
const KEY = rid => rid + '_' + rid + '1';
const R = i => String(4176576270 + i);
const CHAIN = '4179000001', STAR = '4175892473';
const CUSTOM = 'Charm_Custom_Orders';

// the time budgets are the app's own (3.5 s from the cloud's write to the other page holding it); a machine shared with other
// jobs (load above 16 on 4 cores) is given proportionally more, up to 4 times, and the figure measured is always printed
const K = Math.min(4, Math.max(1, os.loadavg()[0] / os.cpus().length / 4));
const T0 = Date.now();
const log = (...a) => console.log('  ·', ...a);
const sleep = ms => new Promise(r => setTimeout(r, ms)), nap = ms => sleep(Math.round(ms * K));   // (nap: a wait for things to settle, longer on a loaded machine)

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [], outside = [], seen = { gets: new Map() };
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  /** One computer: its own context (storage, name), its own page, its own recorder of what is drawn and moved. */
  async function client(name, o = {}) {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy|anthropic/i.test(u)) outside.push(u);
      return r.abort();
    });
    if (o.noLive) await context.route(/charm-nest-review-live\.js/, r => r.abort());
    await context.addInitScript(who => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', who); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} }, name);
    const page = await context.newPage(), c = { name, context, page, reads: [], writes: [] };
    page.setDefaultTimeout(30000 * K);
    page.on('pageerror', e => { errors.push(name + ': ' + e.message); console.error(name, 'page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (!/charmNestLibrary/.test(rq.url()) || rq.method() !== 'POST') return; let b = {}; try { b = JSON.parse(rq.postData() || '{}'); } catch (_) {} if (b.op === 'customGet' && b.since != null) c.reads.push(Date.now()); else if (/^custom(Put|Reopen)$/.test(b.op)) c.writes.push({ op: b.op, at: Date.now() }); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.Motion && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 90000 });
    c.load = async (orders, mode = 'review') => {
      await page.evaluate(async ({ orders, mode }) => {
        await Orders.loadMaps(true);
        const at = Date.now();
        for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: at, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode(mode); Review.render();
        // what is drawn and moved, with the time: a stamp coming down, a card lifted off, a switch pulsed, a note
        window.__rec = []; const log = (k, d) => __rec.push({ k, t: Date.now(), d });
        new MutationObserver(ms => { for (const m of ms) {
          if (m.type === 'attributes') { const n = m.target; if (n.classList && n.classList.contains('mGot') && n.dataset && n.dataset.cseg) log('arrive', n.dataset.cseg); continue; }
          for (const n of m.addedNodes) { if (n.nodeType !== 1) continue;
            if (n.matches('.sealTool') || n.querySelector('.sealTool')) log('tool');
            if (n.matches('.mGhost')) log('ghostIn', (n.querySelector('[data-rid]') || {}).dataset && n.querySelector('[data-rid]').dataset.rid);
            if (n.matches('.mNote')) log('note', n.textContent); }
          for (const n of m.removedNodes) { if (n.nodeType !== 1) continue; if (n.matches('.sealTool')) log('toolOut'); if (n.matches('.mGhost')) log('ghostOut'); } } }).observe(document.documentElement, { childList: true, subtree: true, attributes: true, attributeFilter: ['class'] });
      }, { orders, mode });
    };
    c.rec = () => page.evaluate(() => (window.__rec || []).slice());
    c.mark = async () => (await c.rec()).length;
    c.since = async n => (await c.rec()).slice(n);
    c.state = () => page.evaluate(() => ({ open: +(document.querySelector('#reviewView .rvSeg [data-cseg="open"] b') || {}).textContent, done: +(document.querySelector('#reviewView .rvSeg [data-cseg="done"] b') || {}).textContent, cseg: Review.view().cseg }));
    c.seg = async s => { await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close())); await page.click(`#reviewView .rvSeg [data-cseg="${s}"]`); await page.waitForFunction(s => Review.view().cseg === s, s); };
    c.cards = () => page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid));
    c.calm = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 30000 * K });
    c.has = (k) => page.evaluate(k => !!B.maps.customDone[k], k);
    c.close = async () => { await context.close(); };
    return c;
  }
  const dueIn = async (c, fn, what, ms = 9000) => { ms = Math.round(ms * K); try { await c.page.waitForFunction(fn.f, fn.a, { timeout: ms }); } catch (e) { throw new Error(`${c.name}: ${what} did not happen in ${ms} ms`); } };
  // the cloud's own write, as another computer's page makes it (the real handler)
  const cloud = (op, b) => srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({ op }, b)), queryStringParameters: {} }).then(o => JSON.parse(o.body));
  const target = (rid, extra) => Object.assign({ key: KEY(rid), receiptId: String(rid), transactionId: rid + '1', sku: 'RE_5460', title: 'MODIFICATION REWORK FREE SHIPPING', category: 'Custom order', kind: 'rework' }, extra || {});

  try {
    /* ── 0 · before: without the feed the page never sees it ── */
    {
      const A0 = await client('Paul A', { noLive: true }), B0 = await client('Maria B');
      await A0.load(ORDERS); await B0.load(ORDERS);
      assert.equal(await A0.page.evaluate(() => typeof window.ReviewLive), 'undefined', 'the page without the feed script has no ReviewLive');
      const rid = R(5);
      await B0.page.click(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-complete]`);
      await dueIn(B0, { f: k => B.maps.customDone[k], a: KEY(rid) }, 'B completing');
      await sleep(7000);
      assert.equal(await A0.has(KEY(rid)), false, 'before: seven seconds on, the page of the other computer still does not hold the completion');
      assert.deepEqual(await A0.cards().then(l => l.includes(rid)), true, 'before: its card is still under Open on A');
      await A0.page.evaluate(() => Orders.loadMaps(true));   // (what the Etsy order check does, every ten minutes at the least)
      assert.equal(await A0.has(KEY(rid)), true, 'before: only the maps load (page load, Etsy check, reconnect) brings it in');
      log('before: A learns of B\'s completion only at Orders.loadMaps (page load, reconnect, or the Etsy order check, 10 minutes at the least); 7 s after, nothing');
      await A0.close(); await B0.close();
    }
    // (the same cloud keeps B0's record; the new computers below start from it: it is a completed order, nothing to replay)

    const A = await client('Paul A'), B = await client('Maria B');
    await A.load(ORDERS); await B.load(ORDERS);
    await A.calm(); await B.calm();
    const alive = await A.page.evaluate(() => ReviewLive.state());
    assert(alive.cursor > 0, 'A seeded from its maps read: ' + JSON.stringify(alive));

    /* ── 1 · B completes; A, watching Open, sees the stamp, the flight and the card in Completed ── */
    const completeOnB = async (rid, tab) => {
      if (tab) await B.page.click(`#reviewView .egTab[data-k="${tab}"]`);
      const t = Date.now();
      await B.page.click(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-complete]`);
      return t;
    };
    const seen1 = async (rid, label, tab, how) => {
      const before = await A.state(), a0 = await A.mark(), b0 = await B.mark();
      // (the card is on A's screen: a card scrolled out of sight only pulses the Completed switch, see section 1b)
      await A.page.evaluate(rid => { document.querySelectorAll('.mNote').forEach(n => n.close && n.close()); const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`); if (n) n.scrollIntoView({ block: 'center' }); }, rid);
      const t = await completeOnB(rid, tab);
      await dueIn(A, { f: k => B.maps.customDone[k], a: KEY(rid) }, label + ': A holding the record');
      // (late: from the cloud's own write, the record's updatedAtMs, to A holding it: what the feed costs; the press takes its own time on B)
      const heldAt = await A.page.evaluate(() => Date.now()), pressed = heldAt - t, late = heldAt - srv.st.doc(CUSTOM, KEY(rid)).updatedAtMs;
      lastPress.push(pressed);
      // the stamp, then the flight (the card's copy leaves and the Completed switch answers), in that order
      await dueIn(A, { f: n => __rec.slice(n).some(e => e.k === 'arrive' && e.d === 'done'), a: a0 }, label + ': the Completed switch answering the flight', 15000);
      await A.calm();
      const ev = await A.since(a0), at = k => ev.find(e => e.k === k), tool = at('tool'), ghost = at('ghostIn'), out = ev.filter(e => e.k === 'ghostOut').pop(), got = ev.find(e => e.k === 'arrive' && e.d === 'done'), note = at('note');
      assert(tool && ghost && out && got, label + ': a stamp, a card lifted off and a Completed pulse: ' + JSON.stringify(ev.map(e => e.k)));
      assert(ghost.t <= tool.t + 50, label + ': the copy of the card is lifted first, the stamp comes down on it');
      assert(tool.t < got.t && got.t <= out.t + 400, label + ': the stamp, then the flight lands on Completed: ' + JSON.stringify(ev.map(e => [e.k, e.t - ev[0].t])));
      assert.equal(ev.filter(e => e.k === 'tool').length, 1, label + ': one stamp on A, no second one');
      assert(note && new RegExp(`Order ${rid} moved to Completed · ${how === 'print' ? 'QR label printed' : 'completed'} by Maria B`).test(note.d), label + ': the note names who and what: ' + (note && note.d));
      // B, who pressed, saw one stamp of its own, and nothing came after it for the same change
      await B.calm(); await nap(3200);
      const bev = (await B.since(b0));
      assert.equal(bev.filter(e => e.k === 'tool').length, 1, label + ': B (who pressed) sees exactly one stamp: ' + JSON.stringify(bev.map(e => e.k)));
      assert.equal(bev.filter(e => e.k === 'ghostIn').length, 1, label + ': and one flight');
      const after = await A.state();
      assert.deepEqual([after.open, after.done], [before.open - 1, before.done + 1], label + ': A\'s counts follow: ' + JSON.stringify([before, after]));
      const bs = await B.state(); assert.deepEqual([bs.open, bs.done], [after.open, after.done], label + ': and are the same on B');
      assert((await A.cards()).indexOf(rid) < 0, label + ': the card has left Open');
      return { late, tool, got };
    };
    const lat = [], lastPress = [];
    {
      const x = await seen1(R(2), 'custom order', null, 'button'); lat.push(x.late);
      // Completed: the card, once, with the record's seal (who, when)
      await A.seg('done');
      const card = await A.page.evaluate(rid => { const ns = [...document.querySelectorAll(`#rvList .reviewListRow[data-rid="${rid}"]`)]; const n = ns[0]; return { count: ns.length, seals: n ? [...n.querySelectorAll('.seal')].map(s => ({ cls: s.className, at: +s.dataset.at, label: s.getAttribute('aria-label') })) : [], btns: n ? [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()) : [] }; }, R(2));
      const rec = srv.st.doc(CUSTOM, KEY(R(2)));
      assert.equal(card.count, 1, 'the card is in Completed once');
      assert.equal(card.seals.length, 1, 'one seal on it: ' + JSON.stringify(card.seals));
      assert.equal(card.seals[0].at, rec.stamps[0].at, 'the seal is the record\'s own: its time');
      assert.match(card.seals[0].label, /Maria B/, 'and its person');
      assert(card.seals[0].cls.includes('seal-button') && !card.seals[0].cls.includes('pending'), 'a Complete Order seal, at rest');
      assert.deepEqual(card.btns, ['Print QR label', 'Reopen'], 'the buttons of a completed card');
      await A.seg('open');
    }
    {
      const x = await seen1(CHAIN, 'chain only', 'customOrder', 'button'); lat.push(x.late);
      await B.page.click('#reviewView .egTab[data-k="customOrder"]').catch(() => {});   // (the chip may be gone: it held no other card)
    }
    {
      await B.page.evaluate(() => { Review.view().filter = null; Review.render(); });
      await B.page.evaluate(() => { const b = document.querySelector('#reviewView .egTab[data-k="unmatchedSku"]'); if (b) b.click(); });
      const x = await seen1(STAR, 'Unknown SKU', null, 'button'); lat.push(x.late);
      await B.page.evaluate(() => { Review.view().filter = null; Review.render(); });
    }
    log(`B → A, Complete Order on three kinds of card: in ${lat.map(x => (x / 1000).toFixed(2) + ' s').join(', ')} from the cloud write to A holding it (${lastPress.map(x => (x / 1000).toFixed(2) + ' s').join(', ')} from B's press; budget 3.5 s from the write, load factor ${K.toFixed(1)}); one stamp on A, then the flight, one on B`);
    for (const x of lat) assert(x <= 3500 * K, 'within the time budget: ' + x + ' ms (budget ' + Math.round(3500 * K) + ' ms, machine load factor ' + K.toFixed(1) + ')');

    /* ── 1b · the card is scrolled out of sight on A: the Completed switch answers with its note; nothing is stamped or flown ── */
    {
      const rid = R(7), a0 = await A.mark();
      await A.page.setViewportSize({ width: 1440, height: 420 });
      const hidden = await A.page.evaluate(rid => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`), sc = n.closest('.scroll') || n.parentElement; const out = () => { const r = n.getBoundingClientRect(), c = sc.getBoundingClientRect(); return r.bottom <= c.top || r.top >= c.bottom; }; sc.scrollTop = 0; if (out()) return true; sc.scrollTop = sc.scrollHeight; return out(); }, rid);
      assert(hidden, 'the card is out of A\'s sight');
      await A.page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close()));
      await B.page.click(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-complete]`);
      await dueIn(A, { f: k => B.maps.customDone[k], a: KEY(rid) }, 'A holding the record');
      await dueIn(A, { f: n => __rec.slice(n).some(e => e.k === 'arrive' && e.d === 'done'), a: a0 }, 'the Completed switch answering', 15000);
      await A.calm(); await B.calm();
      const ev = await A.since(a0);
      assert(!ev.some(e => e.k === 'tool' || e.k === 'ghostIn'), 'nothing is stamped or flown over a card nobody sees: ' + JSON.stringify(ev.map(e => e.k)));
      assert(ev.some(e => e.k === 'note' && new RegExp(`Order ${rid} moved to Completed · completed by Maria B`).test(e.d)), 'the note says who: ' + JSON.stringify(ev.filter(e => e.k === 'note')));
      await A.page.setViewportSize({ width: 1440, height: 900 });
      await A.page.evaluate(() => { document.querySelectorAll('.mNote').forEach(n => n.close && n.close()); const sc = document.querySelector('#rvList').closest('.scroll'); if (sc) sc.scrollTop = 0; });
      const sa = await A.state(), sb = await B.state(); assert.deepEqual(sa, sb, 'the counts agree: ' + JSON.stringify([sa, sb]));
      log('a card out of sight on A: the Completed switch answers and the note names who; no stamp, no flight; counts right');
    }

    /* ── 2 · Reopen from B while A watches Completed; a print from another computer; a print again ── */
    {
      await A.seg('done'); await B.seg('done');
      const rid = R(2), a0 = await A.mark();
      await B.page.click(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-reopen]`);
      await dueIn(A, { f: k => !B.maps.customDone[k] && B.maps.customKept[k], a: KEY(rid) }, 'A holding the reopen');
      await dueIn(A, { f: n => __rec.slice(n).some(e => e.k === 'arrive' && e.d === 'open'), a: a0 }, 'the Open switch answering', 15000);
      await A.calm();
      const ev = await A.since(a0);
      assert(ev.some(e => e.k === 'ghostIn'), 'the card was lifted off Completed and flew to Open: ' + JSON.stringify(ev.map(e => e.k)));
      assert(!ev.some(e => e.k === 'tool'), 'no stamp on a reopen');
      assert(ev.some(e => e.k === 'note' && new RegExp(`Order ${rid} moved back to Open · reopened by Maria B`).test(e.d)), 'its note says who: ' + JSON.stringify(ev.filter(e => e.k === 'note')));
      const st = await A.state(), bs = await B.state(); assert.deepEqual(st, bs, 'the same counts on both: ' + JSON.stringify([st, bs]));
      assert.equal((await A.cards()).includes(rid), false, 'gone from Completed');
      await A.seg('open');
      const kept = await A.page.evaluate(rid => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`); return n && [...n.querySelectorAll('.rowActions [data-cu-complete][data-seal-btn] + .sealRow .seal')].length; }, rid);
      assert.equal(kept, 1, 'back under Open with its Complete Order seal kept on the button that made it');
      assert.equal(srv.st.doc(CUSTOM, KEY(rid)).stamps.length, 1, 'the cloud keeps the seal');
      log('Reopen from B: the card flies from Completed to Open on A with its note; the seal is kept');
    }
    {
      // a print made on another computer (the printer's own answer is a customPut how:print): the print seal, then the flight
      const rid = R(3), a0 = await A.mark();
      await A.seg('open');
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', label: { files: [] } }));
      await dueIn(A, { f: k => B.maps.customDone[k], a: KEY(rid) }, 'A holding the print');
      await dueIn(A, { f: n => __rec.slice(n).some(e => e.k === 'arrive' && e.d === 'done'), a: a0 }, 'the flight landing', 15000);
      await A.calm();
      const ev = await A.since(a0);
      assert.equal(ev.filter(e => e.k === 'tool').length, 1, 'one stamp for the print');
      assert(ev.some(e => e.k === 'note' && /QR label printed by Maria B/.test(e.d)), 'the note says a label was printed: ' + JSON.stringify(ev.filter(e => e.k === 'note')));
      await A.seg('done');
      const seal = await A.page.evaluate(rid => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`); return [...n.querySelectorAll('.seal')].map(s => s.className); }, rid);
      assert(seal.length === 1 && seal[0].includes('seal-print'), 'a QR label seal on the completed card: ' + seal);
      // a print again: a second seal on the card, stamped where it stands, and no flight
      const a1 = await A.mark(), n1 = await A.page.evaluate(rid => document.querySelectorAll(`#rvList .reviewListRow[data-rid="${rid}"] .seal`).length, rid);
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', label: { files: [] } }));
      await dueIn(A, { f: ([rid, n]) => document.querySelectorAll(`#rvList .reviewListRow[data-rid="${rid}"] .seal`).length > n, a: [rid, n1] }, 'the new seal on the card');
      await dueIn(A, { f: n => __rec.slice(n).some(e => e.k === 'tool'), a: a1 }, 'the stamp coming down on it', 8000);
      await A.calm();
      const ev1 = await A.since(a1);
      assert(!ev1.some(e => e.k === 'ghostIn'), 'no flight for a print again: ' + JSON.stringify(ev1.map(e => e.k)));
      assert.equal(ev1.filter(e => e.k === 'tool').length, 1, 'one stamp for it');
      const seals = await A.page.evaluate(rid => [...document.querySelectorAll(`#rvList .reviewListRow[data-rid="${rid}"] .seal`)].map(s => +s.dataset.at), rid);
      assert.deepEqual(seals, srv.st.doc(CUSTOM, KEY(rid)).stamps.map(s => s.at).sort((a, b) => a - b), 'the seals on the card are the record\'s, none twice');
      log('a print from another computer: the print seal stamped on the card, the flight; a print again: a new seal stamped in place, no flight');
      await A.seg('open');
    }

    /* ── 3 · hidden, or another tab on screen: nothing read while it is, nothing moves when it is shown ── */
    {
      const rid = R(4), a0 = await A.mark(), n0 = (await A.page.evaluate(() => ReviewLive.state())).polls;
      await A.page.evaluate(() => { Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); document.dispatchEvent(new Event('visibilitychange')); });
      await sleep(400); const r0 = A.reads.length;
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' }));
      await sleep(5500);
      assert.equal(A.reads.length, r0, 'no read while the tab is hidden (5.5 s)');
      assert.equal(await A.has(KEY(rid)), false, 'and nothing is held yet');
      assert.deepEqual(await A.since(a0), [], 'nothing moved while hidden');
      await A.page.evaluate(() => { delete document.hidden; delete document.visibilityState; document.dispatchEvent(new Event('visibilitychange')); });
      await dueIn(A, { f: k => B.maps.customDone[k], a: KEY(rid) }, 'A catching up once shown', 5000);
      await nap(2200); await A.calm();
      assert.deepEqual((await A.since(a0)).filter(e => e.k !== 'note'), [], 'shown again: the change is read with nothing stamped or flown: ' + JSON.stringify(await A.since(a0)));
      const st = await A.state(), bs = await B.state();
      await B.page.evaluate(() => { Review.render(); }); await sleep(400);
      assert.deepEqual([st.open, st.done], [(await B.state()).open, (await B.state()).done], 'the counts follow: ' + JSON.stringify(st));
      assert.equal((await A.cards()).includes(rid), false, 'the card is not under Open');
      log('hidden tab: 0 reads in 5.5 s, nothing moved; shown again: caught up in one read, no stamp, no flight, counts right');
    }
    {
      // another tab (Orders) on screen: no read; the Review tab shown again is caught up, nothing moves
      const rid = R(5), a0 = await A.mark();
      await A.page.evaluate(() => CN.setMode('orders')); await sleep(900);
      const r0 = A.reads.length;
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' }));
      await sleep(4500);
      assert.equal(A.reads.length, r0, 'no feed read while another tab is on screen (4.5 s)');
      await A.page.evaluate(() => CN.setMode('review')); await dueIn(A, { f: k => B.maps.customDone[k], a: KEY(rid) }, 'A catching up on Review', 5000);
      await nap(2000); await A.calm();
      assert.deepEqual((await A.since(a0)).filter(e => e.k !== 'note'), [], 'no animation for what happened while it was away');
      assert.equal((await A.cards()).includes(rid), false, 'the list is right');
      // one quiet read as another tab is shown, once the last read is more than 5 s old (the Orders tab, the Library's issues and
      // the rail agree when they are looked at)
      const rid2 = R(6);
      await A.page.evaluate(() => CN.setMode('orders')); await sleep(5800);
      const r1 = A.reads.length;
      await cloud('customPut', Object.assign(target(rid2), { by: 'Maria B', how: 'button' }));
      await sleep(700); assert.equal(A.reads.length, r1, 'no read while on the Orders tab');
      await A.page.evaluate(() => CN.setMode('nest'));
      await dueIn(A, { f: k => B.maps.customDone[k], a: KEY(rid2) }, 'one read as another tab is shown', 4000);
      assert.equal(A.reads.length - r1, 1, 'one read, not a poller');
      await A.page.evaluate(() => CN.setMode('review')); await sleep(800); await A.calm();
      assert.deepEqual((await A.since(a0)).filter(e => e.k !== 'note'), [], 'still nothing moved');
      log('another tab on screen: no feed read; Review shown again or another tab shown: one read, nothing moves, state right');
    }
    {
      // the order window open on the Orders tab: its timeline feed (2.5 s) brings a change in, and the window's Custom Orders bar follows
      const rid = R(1), key = KEY(rid);
      await A.page.evaluate(k => { CN.setMode('orders'); OrderWin.open(k); }, key);
      await A.page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && !document.querySelector('#orderWin').classList.contains('owLoading'), key);
      await A.page.waitForFunction(() => !document.getElementById('owCustom').hidden && document.querySelector('#owCustom [data-cu-complete]'), null, { timeout: 15000 });
      const t = Date.now(); await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' }));
      await dueIn(A, { f: () => document.querySelector('#owCustom [data-cu-reopen]'), a: null }, 'the window\'s bar showing Completed', 6000);
      const took = Date.now() - t;
      assert(took <= 5000, 'the window followed within seconds: ' + took);
      assert.equal(await A.has(key), true, 'the page holds the record');
      await A.page.evaluate(() => document.getElementById('owClose').click()); await A.page.waitForFunction(() => !OrderWin.isOpen());
      await A.page.evaluate(() => CN.setMode('review')); await A.calm();
      log(`order window open on the Orders tab: its Custom Orders bar followed B's completion in ${(took / 1000).toFixed(1)} s (the window's timeline feed asks for one read)`);
    }

    /* ── 4 · a page load replays nothing ── */
    {
      const C = await client('Dana C');
      const mark = Date.now();
      await C.load(ORDERS);
      await nap(4500); await C.calm();
      const ev = await C.rec(), st = await C.page.evaluate(() => ReviewLive.state());
      assert.deepEqual(ev, [], 'a page loaded after the completions shows no stamp, flight or note: ' + JSON.stringify(ev));
      assert.equal(st.loud, 0, 'and the feed announced nothing: ' + JSON.stringify(st));
      assert((await C.state()).done >= 5, 'the list shows what is done: ' + JSON.stringify(await C.state()));
      log('page load: ' + JSON.stringify(await C.state()) + ' already done, nothing stamped or flown');
      await C.close();
    }

    /* ── 5 · double completion ── */
    {
      const rid = R(0), key = KEY(rid);
      await A.seg('open'); await B.seg('open');
      assert((await A.cards()).includes(rid) && (await B.cards()).includes(rid), 'the card is under Open on both');
      await Promise.all([A, B].map(c => c.page.evaluate(sel => document.querySelector(sel).click(), `#rvList .reviewListRow[data-rid="${rid}"] [data-cu-complete]`)));   // (both at the same instant: a click, not a pointer that waits for the page to be still)
      await dueIn(A, { f: k => B.maps.customDone[k], a: key }, 'A done'); await dueIn(B, { f: k => B.maps.customDone[k], a: key }, 'B done');
      await nap(4500); await A.calm(); await B.calm();
      const rec = srv.st.doc(CUSTOM, key), stamps = rec.stamps.map(s => s.at).sort((a, b) => a - b);
      for (const c of [A, B]) {
        await c.seg('open'); assert.equal((await c.cards()).filter(r => r === rid).length, 0, c.name + ': not under Open');
        await c.seg('done');
        const on = await c.page.evaluate(rid => { const ns = [...document.querySelectorAll(`#rvList .reviewListRow[data-rid="${rid}"]`)]; return { n: ns.length, seals: ns[0] ? [...ns[0].querySelectorAll('.seal')].map(s => +s.dataset.at) : [] }; }, rid);
        assert.equal(on.n, 1, c.name + ': one card in Completed');
        assert.deepEqual(on.seals.sort((a, b) => a - b), stamps, c.name + ': the seals are the record\'s (' + stamps.length + ')');
        await c.seg('open');
      }
      const sa = await A.state(), sb = await B.state(); assert.deepEqual(sa, sb, 'the same counts: ' + JSON.stringify([sa, sb]));
      log(`double completion: one card on both, ${stamps.length} real seal(s) as the record keeps them, counts ${JSON.stringify(sa)}`);
    }

    /* ── 6 · a failed write rolls back; a failing feed backs off quietly ── */
    {
      const rid = R(6), key = KEY(rid);
      // R(6) was completed above; reopen it on the cloud first, as B, so it is open on both
      await cloud('customReopen', { key, by: 'Maria B' });
      await dueIn(A, { f: k => !B.maps.customDone[k], a: key }, 'A holding the reopen'); await dueIn(B, { f: k => !B.maps.customDone[k], a: key }, 'B holding the reopen');
      await A.seg('open'); await B.seg('open'); await sleep(600); await A.calm(); await B.calm();
      assert((await B.cards()).includes(rid), 'the card is under Open on B');
      const sA = await A.state(), sB = await B.state(), a0 = await A.mark();
      const failPut = /charmNestLibrary/, abortIt = r => (/"op":"customPut"/.test(r.request().postData() || '') ? r.abort() : r.fallback());
      await B.context.route(failPut, abortIt);
      await B.page.click(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-complete]`);
      await dueIn(B, { f: () => [...document.querySelectorAll('#toasts .toast')].some(t => /was not completed|not completed/i.test(t.textContent)), a: null }, 'the error toast', 20000);
      await B.calm(); await nap(3200);
      assert.deepEqual(await B.state(), sB, 'B: nothing changed: the card is where it was');
      assert((await B.cards()).includes(rid), 'B: the card is still under Open and can be pressed again');
      assert.equal(await B.page.evaluate(sel => !document.querySelector(sel).disabled, `#rvList .reviewListRow[data-rid="${rid}"] [data-cu-complete]`), true, 'its button is usable again');
      assert.equal(srv.st.doc(CUSTOM, key).state, 'open', 'the cloud still says open');
      assert.deepEqual(await A.state(), sA, 'A saw nothing'); assert.deepEqual(await A.since(a0), [], 'and nothing moved on A');
      await B.context.unroute(failPut, abortIt);
      log('failed write: B shows the error toast, the card stays under Open and can be pressed again, the cloud and A unchanged');
    }
    {
      // the feed failing on A: it backs off, says nothing, and catches up quietly when it is back
      const rid = R(6), key = KEY(rid), a0 = await A.mark();
      const match = /charmNestLibrary/, route = r => (/"since"/.test(r.request().postData() || '') ? r.abort() : r.fallback());
      await A.context.route(match, route);
      const r0 = A.reads.length; await sleep(300);
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' }));
      await sleep(9000);
      const tried = A.reads.length - r0, st = await A.page.evaluate(() => ReviewLive.state());
      assert(tried >= 2 && tried <= 5, 'it tried again, each time later (9 s: ' + tried + ' reads)');
      assert(st.fails >= 2, 'and counted the failures: ' + JSON.stringify(st));
      assert.equal(await A.page.evaluate(() => [...document.querySelectorAll('#toasts .toast')].length), 0, 'and said nothing');
      assert.equal(await A.has(key), false, 'nothing held while it fails');
      await A.context.unroute(match, route);
      await dueIn(A, { f: k => B.maps.customDone[k], a: key }, 'A catching up once the feed answers', 40000);
      await nap(2000); await A.calm();
      assert.deepEqual((await A.since(a0)).filter(e => e.k !== 'note'), [], 'the catch-up after a failure moves nothing');
      assert.equal((await A.page.evaluate(() => ReviewLive.state())).fails, 0, 'the failures are cleared');
      log(`feed failing: ${tried} reads in 9 s (backing off), no toast; back: caught up in one quiet read`);
    }

    /* ── 7 · a rapid complete, reopen, complete ── */
    {
      const rid = R(6), key = KEY(rid);
      await cloud('customReopen', { key, by: 'Maria B' });
      await dueIn(A, { f: k => !B.maps.customDone[k], a: key }, 'A holding the reopen'); await dueIn(B, { f: k => !B.maps.customDone[k], a: key }, 'B holding it');
      await A.seg('open'); await B.seg('open'); await A.calm(); await B.calm();
      // B (the cloud, as its page writes) completes, reopens, completes; A presses Reopen on the card meanwhile, as soon as it has it
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' })); await sleep(250);
      await cloud('customReopen', { key, by: 'Maria B' }); await sleep(250);
      await cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' }));
      await nap(4200); await A.calm(); await B.calm();
      const rec = srv.st.doc(CUSTOM, key);
      assert.equal(rec.state, 'completed', 'the cloud: completed');
      for (const c of [A, B]) {
        await c.seg('done'); const inDone = (await c.cards()).filter(r => r === rid).length;
        await c.seg('open'); const inOpen = (await c.cards()).filter(r => r === rid).length;
        assert.deepEqual([inDone, inOpen], [1, 0], c.name + ': one card, in Completed only');
        assert.equal(await c.page.evaluate(() => document.querySelectorAll('#motionLayer .mGhost, .cuStat, .btn.working').length), 0, c.name + ': nothing stuck');
      }
      // A presses Reopen against B writing complete at the same moment: both settle on what the cloud says
      await A.seg('done'); await sleep(300);
      await Promise.all([A.page.evaluate(sel => document.querySelector(sel).click(), `#rvList .reviewListRow[data-rid="${rid}"] [data-cu-reopen]`), cloud('customPut', Object.assign(target(rid), { by: 'Maria B', how: 'button' }))]);
      await nap(5200); await A.calm(); await B.calm();
      const final = srv.st.doc(CUSTOM, key).state, want = final === 'completed' ? [1, 0] : [0, 1];
      for (const c of [A, B]) {
        await c.seg('done'); const inDone = (await c.cards()).filter(r => r === rid).length;
        await c.seg('open'); const inOpen = (await c.cards()).filter(r => r === rid).length;
        assert.deepEqual([inDone, inOpen], want, c.name + ': one card where the cloud (' + final + ') puts it: ' + JSON.stringify([inDone, inOpen]));
      }
      const sa = await A.state(), sb = await B.state(); assert.deepEqual(sa, sb, 'the same counts on both after the race: ' + JSON.stringify([sa, sb]));
      log(`rapid complete → reopen → complete, then a Reopen against a Complete: both clients end with one card (${final}), same counts ${JSON.stringify(sa)}, nothing stuck`);
    }

    /* ── 8 · cost ── */
    {
      await A.seg('open');
      const r0 = A.reads.length, t0 = Date.now(); await sleep(20000);
      const n = A.reads.length - r0, per = (Date.now() - t0) / n, perHour = Math.round(3600000 / per);
      assert(n >= 6 && n <= 11, 'about one read every 2 s while watched: ' + n + ' in 20 s');
      const mut = await A.page.evaluate(() => new Promise(res => { let n = 0; const o = new MutationObserver(m => { n += m.length; }); o.observe(document.getElementById('rvList'), { childList: true, subtree: true, attributes: true, characterData: true }); setTimeout(() => { o.disconnect(); res(n); }, 8000); }));
      assert.equal(mut, 0, 'idle reads change nothing on screen (8 s)');
      const q0 = srv.st.queries || 0, d0 = srv.st.reads || 0;
      await A.page.evaluate(() => ReviewLive.poll({}));
      assert.equal((srv.st.queries || 0) - q0, 1, 'one query per read'); assert.equal((srv.st.reads || 0) - d0, 0, 'no document when nothing changed (Firestore bills one)');
      assert.deepEqual(outside, [], 'no Etsy or AI call');
      log(`cost: ${n} reads in 20 s while watched = ${perHour} a hour for one open Review tab (one query, no document read when idle), 0 hidden, nothing drawn`);
    }
    {
      // 300 cards in Review still draw fast, an idle read draws nothing, and a change among them is applied at the cost of about one
      // draw (every figure is read in the one page, back to back: the machine's load moves them all together)
      await A.close(); await B.close();
      const many = Array.from({ length: 300 }, (_, i) => order(4170000000 + i, 100 + i, 'RE_70' + String(i).padStart(2, '0'), 'MODIFICATION REWORK FREE SHIPPING', 200 + (i % 40)));
      const D = await client('Eve D');
      await D.load(many);
      await D.page.waitForFunction(() => document.querySelectorAll('#rvList .reviewListRow').length > 0);
      const draw = await D.page.evaluate(() => { Review.view().limit = 300; Review.render({ still: true }); const t = performance.now(); Review.render({ still: true }); return performance.now() - t; });
      await D.page.evaluate(() => {
        window.__ms = 0; window.__net = 0; window.__settles = 0;
        for (const [o, k] of [[Orders, 'takeCustom'], [CustomPrint, 'settle']]) { const f = o[k]; o[k] = function (...a) { if (k === 'settle') window.__settles++; const t = performance.now(); try { return f.apply(this, a); } finally { window.__ms += performance.now() - t; } }; }
        const f = window.api; window.api = async function (fn, body) { const t = performance.now(); try { return await f.apply(this, arguments); } finally { if (body && body.since != null) window.__net += performance.now() - t; } };
      });
      // an idle read: its own work in the page (the read's wait for the cloud taken off), and whether it set anything going
      // (the join of the records, the settle that redraws: neither is called when nothing changed; the page's own thumbnails and
      // timers draw into the list meanwhile, which is why the list is not watched here: the 9-card list above is)
      const idle = await D.page.evaluate(async () => {
        const sleep = ms => new Promise(r => setTimeout(r, ms));
        while (ReviewLive.state().busy) await sleep(20);
        window.__net = 0; window.__settles = 0; window.__ms = 0; const applied0 = ReviewLive.state().applied, a = performance.now();
        await ReviewLive.poll({}); const wall = performance.now() - a, js = wall - window.__net;
        return { wall, js, settles: window.__settles, applied: ReviewLive.state().applied - applied0 };
      });
      const rid = String(4170000000 + 150), key = rid + '_' + rid + '1';
      await cloud('customPut', { key, receiptId: rid, transactionId: rid + '1', sku: 'RE_7050', title: 'MODIFICATION REWORK FREE SHIPPING', category: 'Custom order', kind: 'rework', by: 'Maria B', how: 'button' });
      await D.page.waitForFunction(k => B.maps.customDone[k], key, { timeout: 90000 });
      await sleep(500);
      const apply = await D.page.evaluate(() => window.__ms);
      assert(await D.has(key), 'the change among 300 was applied');
      const cards = await D.page.evaluate(() => document.querySelectorAll('#rvList .reviewListRow').length);
      log(`300 cards: a full draw of ${cards} cards ${draw.toFixed(0)} ms; an idle read's own work ${idle.js.toFixed(0)} ms (${idle.wall.toFixed(0)} ms with its wait for the cloud), ${idle.settles} redraws set going; a change brought in and everything that follows it ${apply.toFixed(0)} ms (machine under load: compare, do not read as absolute)`);
      assert.equal(idle.settles + idle.applied, 0, 'an idle read changes and redraws nothing among 300 cards: ' + JSON.stringify(idle));
      assert(idle.js < Math.max(150, draw), 'an idle read costs less than one draw of the list: ' + idle.js + ' vs ' + draw);
      assert(apply < 4 * draw + 1000, 'a change among 300 cards costs about a draw or two: ' + apply + ' vs ' + draw);
      await D.close();
    }

    assert.deepEqual(outside, [], 'no Etsy or AI call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log(`Review live OK (${((Date.now() - T0) / 1000).toFixed(0)} s)`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
