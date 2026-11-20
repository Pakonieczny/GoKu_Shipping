// REVIEWCORE · the page side of tests/charm-nest/review-core.cjs: the REAL sorter page (charm-nest-1.html) in headless Chromium over the repo's fake site
// (bridge-server.cjs, an in-memory Firestore), every request off the loopback refused, made-up buyers only. Nothing here is written anywhere real.
//
//   const W = require('./_reviewCoreWorld.cjs');
//   const w = await W.open();                 // null without playwright-core
//   await W.lines(w, orders, opts)            // orders in, through the real intake (Orders.interpretAll), then Review.syncOrderItems
//   const snap = await W.snapshot(w, now)     // ONLY what the core needs, as plain data (what the cloud holds or will hold)
//   const got  = await W.core(w, snap)        // CharmNestReviewCore.build(snap), run in the page (the module injected as is)
//   const want = await W.truth(w)             // what the page's own Review says: Review.count(), openCount(), openOrders(), the tab's chips and lists
//   await w.close();
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

async function open(opts = {}) {
  const pwDir = opts.pwDir || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { return null; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1600, height: 950 }, timezoneId: 'America/Toronto' });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  const outside = [];
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob([""])); } }; } };'));
    if (/vfs_fonts/.test(u)) return r.fulfill(js(''));
    if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    if (/etsy/i.test(u)) outside.push(u);
    return r.abort();
  });
  await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.CustomSheet && window.Cancelled && window.CharmNestReviewMark && CN.S.cloud.ok === true, null, { timeout: 60000 });
  await page.waitForFunction(() => !window.Session || typeof Session.ready !== 'function' || Session.ready() === true || (Session.failure && Session.failure()), null, { timeout: 60000 });
  await page.evaluate(async () => { await Orders.loadMaps(true); });
  const corePath = path.join(root, 'charm-nest-review-core.js');
  if (fs.existsSync(corePath)) await page.addScriptTag({ path: corePath });
  return { srv, browser, context, page, errors, outside, close: async () => { await browser.close(); srv.close(); } };
}

/** A fresh day: no lines, no Review items, no marks, no custom records, no sends, nothing settled (the page's own state, emptied in place). */
async function clear(w) {
  await w.page.evaluate(() => {
    B.orders.rows = []; B.orders.byKey = new Map(); B.review.items = [];
    const s = Review.settled(); s.length = 0;
    B.maps.customDone = {}; B.maps.customKept = {}; B.maps.reviewMarks = {}; B.maps.customSent = {};
    for (const k of Object.keys(B.customDesigns || {})) delete B.customDesigns[k];
    try { CustomSheet.prune && CustomSheet.prune(); } catch (_) {}
  });
}

/** Lines in, as the pull brings them (no pool, no run), then the real intake reads them and Review syncs. */
async function lines(w, orders) {
  await w.page.evaluate(orders => {
    for (const order of orders) for (const line of order.lines) {
      const key = CharmNestOrders.lineKey(order, line);
      if (B.orders.byKey.has(key)) continue;
      const row = { key, order, line, arrivedAt: order.createTs * 1000 + 3600000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row);
    }
    Orders.interpretAll();
  }, orders);
}

/** Exactly the data the core reads, from where the page keeps it (each documented in charm-nest-review-core.js). */
async function snapshot(w, now) {
  return w.page.evaluate(now => {
    const plain = v => v == null ? v : JSON.parse(JSON.stringify(v));
    const PP = window.PiecePlacement;
    const lines = Orders.rows().map(r => {
      let placement = null; try { const p = PP && PP.ofRow ? PP.ofRow(r) : null; if (p && p.state && p.state !== 'loading') placement = { state: p.state }; } catch (_) {}
      const chg = (B.review.items || []).find(x => x.key === 'chg:' + r.key);
      const sp = r.spec || null;
      return {
        key: r.key, state: r.state, reason: r.reason || null, hold: r.hold || null, heldAt: +r.heldAt || 0, holdSeen: !!r.holdSeen, placement,
        poolIds: (r.poolIds || []).slice(), frontAt: +r.frontAt || 0, releasing: r.releasing ? { at: +r.releasing.at || 0, by: r.releasing.by || '' } : null,
        arrivedAt: +r.arrivedAt || 0, activityAt: +r.activityAt || 0,
        // (CLOUD: the "chg:" card is this computer's; its words and time are what the computer that saw the change would write with changePending)
        changePending: !!r.changePending && !!chg, changedAt: chg ? +chg.t || 0 : 0, changedWhy: chg ? String(chg.why || '') : '',
        order: { receiptId: String(r.order.receiptId), orderNumber: String(r.order.orderNumber || r.order.receiptId), createTs: +r.order.createTs || 0, updateTs: +r.order.updateTs || 0, shipBy: +r.order.shipBy || 0, buyer: (r.order.buyer && r.order.buyer.name) || '' },
        line: { transactionId: String(r.line.transactionId || ''), listingId: String(r.line.listingId || ''), sku: String(r.line.sku || ''), title: String(r.line.title || '') },
        spec: sp ? { designSku: sp.designSku || null, special: sp.special ? { label: sp.special.label || '', monogram: !!sp.special.monogram, notCut: !!sp.special.notCut } : null, material: sp.material || null } : null,
        problems: plain(r.problems || [])
      };
    });
    // Charm_Custom_Sheet: a card's send (phase "sent") or a send pressed and not answered yet (phase "pending")
    const customSheet = [];
    for (const e of Object.values(B.customDesigns || {})) { if (!e) continue; const s = e.sent || e.sendIntent; if (!s || !s.lines) continue; customSheet.push({ ck: e.ck, rid: String(e.rid || ''), phase: e.sent ? 'sent' : 'pending', sent: { at: +s.at || 0, by: s.by || '', id: s.id || '', lines: Object.fromEntries(Object.keys(s.lines).map(k => [k, []])) } }); }
    const customSent = Object.assign({}, plain(B.maps.customSent || {}));
    for (const r of Orders.rows()) if (r._customSentDecision) customSent[r.key] = plain(r._customSentDecision);
    const settled = Review.settled().map(d => ({ key: d.key, kind: d.kind, why: d.why || '', by: d.by || '', t: +d.t || 0, orders: (d.orders || []).map(String), lines: +d.lines || 0, rowKey: d.row ? d.row.key : '' }));
    return { now, lines, customDone: plain(B.maps.customDone || {}), customKept: plain(B.maps.customKept || {}), reviewMarks: plain(B.maps.reviewMarks || {}), customSheet, customSent, cancelled: Cancelled.ids().map(String), settled };
  }, now);
}

/** The core, run in the page on the snapshot (no page state reaches it but what the snapshot carries). */
async function core(w, snap) { return w.page.evaluate(s => CharmNestReviewCore.build(JSON.parse(s)), JSON.stringify(snap)); }

/** What the page's own Review says, every way it says it: its counts, its Open and Resolved lists and their chips, its open orders, the tab badge. */
async function truth(w) {
  return w.page.evaluate(async () => {
    Review.syncOrderItems();
    CN.setMode('review');
    const V = Review.view();
    const read = seg => {
      V.cseg = seg; V.filter = null; V.q = ''; V.keep = 5000; V.limit = 5000; V.want = null;
      Review.render({ still: true });
      const segN = [...document.querySelectorAll('#reviewView .rvSeg b')].map(b => +b.textContent);
      const chips = {}; for (const b of document.querySelectorAll('#reviewView .ordBar .egTab[data-k]')) { const n = b.querySelector('b'); chips[b.dataset.k] = n ? +n.textContent : 0; }
      const cards = [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.mkey).filter(Boolean);
      return { segN, chips, cards };
    };
    const open = read('open'), done = read('done');
    V.cseg = 'open'; Review.render({ still: true });
    // the top nav's badge: with the production flow it is the flow's "In review / On hold" orders (Ladder navBadges), worked out from Review.openOrders(), which the
    // flow keeps for 1.5 s (Orders queues): asked once that has passed, then drawn
    await new Promise(r => setTimeout(r, 1600));
    let flowReview = null; try { Orders.flowStale(); const f = Orders.flowNow(0); flowReview = f && f.counts ? f.counts.review || 0 : null; } catch (_) {}
    try { LiveStrip.render(); } catch (_) {}
    await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
    try { if (window.Ladder) Ladder.now(); } catch (_) {}
    const badge = (document.getElementById('tabReviewN') || {}).textContent || '';
    return { count: Review.count(), openCount: Review.openCount(), openOrders: [...Review.openOrders().keys()].sort(), open, done, badge, flowReview, items: Review.items().map(x => x.key) };
  });
}

module.exports = { open, clear, lines, snapshot, core, truth, root };
