/* FEED (Paul, 28 Oct 2026: "every press and every window to feel like a native app, and fewer cloud calls"; plan: the local-first design, phase 2, one changes feed
   per computer): charm-nest-sync-worker.js and op changes (charmNestLibrary.js FEED) on the real page over bridge-server.cjs (the real handlers on the in-memory
   Firestore). No network, no live record, no credential.
     A · the cloud's side: what each write names (s: p: c: o: x:), the cancel counter, a watched Order_Rev, reset, the sandbox's own count, a list still
         `unchanged` when only a pool row moved
     B · leader election: two tabs of one computer, one leader; the other hears every answer (broadcast), at the same count
     C · the feed: another computer's write reaches the surfaces of both tabs (placement, Review) within seconds, and nothing else is read
     D · the windows: an order window hears a station's scan (its Order_Rev), a sheet window its sheet's save
     E · handover: the leader hidden, then closed: another tab leads within a few seconds, and the closed one stops nobody
     F · worlds apart: a sandbox tab leads its own world (its lock, channel, count); production's writes never reach it
     G · calls per minute: two tabs (Orders active + Library) with nothing changing; an older cloud (no `changes`) leaves every surface on its own timer
   node tests/charm-nest/sync-worker.cjs   (through the slot queue: slot.sh timeout 600 node tests/charm-nest/sync-worker.cjs) */
'use strict';
const path = require('path'), fs = require('fs'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const F = require('../perf/scale-fixture.cjs');
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return Date.now() - t0; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
const out = [];
const say = s => { out.push(s); console.log('  ' + s); };

(async () => {
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  const lib = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return Object.assign({ status: r.status }, JSON.parse(await r.text())); };
  const handler = st.handlers.charmNestLibrary, direct = async b => JSON.parse((await handler.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} })).body);
  let browser = null;
  try {
    /* ── A · the cloud's side ── */
    const stats = handler.feedStats, parts = n => String(n).split('.').map(Number);
    let c = await lib({ op: 'getChanges' });
    assert(/^\d+\.\d+\.\d+\.\d+$/.test(c.n) && !c.keys && !c.reset && c.cg === 0, 'the first question: the count now (its four shards: lib.p.o.x), nothing else: ' + c.n);
    const step = async (b, want, how) => {
      const n0 = c.n, b0 = stats.bumps; const w = await (how || lib)(b); c = await lib({ op: 'getChanges', since: n0, cg: c.cg });
      assert.equal(stats.bumps - b0, 1, b.op + ': one bumpRev');
      for (const k of want) assert(c.keys.includes(k) && (k.endsWith('*') || c.kn[k] > 0), `${b.op} names ${k}, with its count: ${c.keys}`);
      const moved = parts(c.n).map((x, i) => x - parts(n0)[i]);
      assert(moved.every(x => x === 0 || x === 1), `${b.op}: each shard it named moved once (${moved})`);
      return w; };
    const sw = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ op: 'putSheet', sheet: { id: 'pending-sheet', note: 'w', orders: ['3700000102'] } }) });   // (OUTBOX, 28 Oct 2026: a save that carries what the sheet's pieces are read from names their orders; a field patch alone (a note, its release mark, its label) names the sheet only, outbox.cjs A)
    const hdr = sw.headers.get('x-cn-feed') || ''; await sw.text();
    assert(/s:pending-sheet/.test(hdr) && /p:3700000102/.test(hdr) && /;(.*,)?lib=\d+/.test(hdr) && /[;,]p=\d+/.test(hdr), 'a write\'s answer names what it wrote, with the shards\' counts (X-CN-Feed): ' + hdr);
    c = await lib({ op: 'getChanges' });
    await step({ op: 'putSheet', sheet: { id: 'pending-sheet', note: 'x', orders: ['3700000102'] } }, ['s:pending-sheet', 'p:3700000102']);
    await step({ op: 'poolUpdate', poolIds: ['3700000101_12345_1'], patch: { note: 'y' } }, ['p:3700000101']);
    await step({ op: 'reviewMarkPut', rid: '3700000101', action: 'mark', by: 'Ana_M' }, ['c:3700000101_review']);
    await step({ op: 'timelineAdd', events: [{ orderId: '3700000100', type: 'note', at: Date.now(), by: 'Bea', text: 'a' }] }, ['o:3700000100'], direct);   // (the fixture answers timelineAdd itself; the real handler is called here)
    await step({ op: 'cancelPut', orderId: '3700000109', by: 'Paul K', record: { why: 'test' } }, ['x:3700000109', 'x:*']);
    let w = await lib({ op: 'getChanges', since: c.n, watch: { 'o:3700000100': '' } });
    assert(!w.keys && w.orders && w.orders['3700000100'], 'a watched order: its Order_Rev, no name the first time');
    st.put('Order_Rev', '3700000100', { n: 7, at: Date.now() });   // (a station's scan, firebaseOrders: its own batch raises Order_Rev)
    w = await lib({ op: 'getChanges', since: c.n, watch: { 'o:3700000100': w.orders['3700000100'] } });
    assert.deepEqual(w.keys, ['o:3700000100'], 'a watched Order_Rev that moved is named');
    const ahead = i => parts(c.n).map((x, j) => x + (j === i ? 3 : 0)).join('.');
    assert((await lib({ op: 'getChanges', since: ahead(0) })).reset, 'the Library\'s shard ahead of the cloud\'s (a sandbox reset began it again): reset');
    const pAhead = await lib({ op: 'getChanges', since: ahead(1) });
    assert(!pAhead.reset && pAhead.keys.includes('p:*'), 'the pool rows\' shard ahead: p:* only (those surfaces ask their own question), no reset: ' + JSON.stringify(pAhead.keys));
    const sb = await lib({ op: 'getChanges', sandbox: true });
    assert(sb.n === '0.0.0.0' && sb.cg === undefined, 'the sandbox has its own counts (and no cancel counter)');
    const ls = await lib({ op: 'listSheets', excludeDone: true, wantRev: true, since: '0.0' });
    await lib({ op: 'poolUpdate', poolIds: ['3700000101_12345_1'], patch: { note: 'z' } });
    st.reads = 0; const ls2 = await lib({ op: 'listSheets', excludeDone: true, ifRev: ls.rev, since: ls.upTo });
    const listReads = st.reads;
    assert(ls2.unchanged && listReads === 1, `a list read by revision after only a pool row moved: unchanged for 1 read, the Library's shard did not move (${listReads})`);
    st.reads = 0; await lib({ op: 'getChanges', since: c.n });
    const askReads = st.reads;
    say(`A · cloud: each write names what it touched (s: p: c: o: x:) in its shard, one bumpRev each, its answer says so (X-CN-Feed: ${hdr}); a question reads ${askReads} documents (4 shards + the cancel counter, one getAll); watched Order_Rev named; reset / p:* ahead; sandbox apart; list unchanged after a pool row in ${listReads} read`);

    /* ── the browser: one computer (one context), its tabs ── */
    const { chromium } = require(path.join(pwDir, 'playwright-core'));
    browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
    const settings = { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' };
    const computer = async (opts) => {
      const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
      await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      if (opts && opts.noFeed) await ctx.route(/charmNestLibrary/, r => /"op":"getChanges"/.test(r.request().postData() || '') ? r.fulfill({ status: 400, contentType: 'application/json', body: JSON.stringify({ error: 'unknown op' }) }) : r.continue());
      await ctx.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { if (!localStorage.getItem('cn.settings')) localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, settings);
      return ctx;
    };
    const calls = [];
    const orders = F.openOrders(24), rids = orders.map(o => String(o.receiptId));
    const toRows = `(orders => orders.flatMap(o => o.lines.map(l => ({ key: CharmNestOrders.lineKey(o, l), order: o, line: l, arrivedAt: o.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }))))`;
    const tab = async (ctx, tag, mode) => {
      const page = await ctx.newPage(); page.setDefaultTimeout(60000);
      page.on('request', q => { const m = /\/\.netlify\/functions\/([\w-]+)/.exec(q.url()); if (m) calls.push({ t: Date.now(), tag, fn: m[1], op: (/"op":"(\w+)"/.exec(q.postData() || '') || [])[1] || '', body: q.postData() || '' }); });
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true && window.CNSync && document.querySelector('#modeSeg button[data-mode="orders"]'));
      await page.evaluate(async ({ orders, toRows }) => { B.orders.rows = eval(toRows)(orders); B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r])); await Orders.interpretAll(); }, { orders, toRows });
      if (mode) await page.evaluate(m => document.querySelector(`#modeSeg button[data-mode="${m}"]`).click(), mode);
      return page;
    };
    const state = p => p.evaluate(() => CNSync.state());
    const since = (t0, f) => calls.filter(x => x.t >= t0 && (!f || f(x)));
    const A = await computer();

    /* ── B · leader election and broadcast ── */
    const A1 = await tab(A, 'A1', 'orders');
    await until(() => state(A1).then(s => s.leader && s.live), 15000, 'tab 1 leads');
    const A2 = await tab(A, 'A2', 'review');
    await until(() => state(A2).then(s => s.live && s.heard > 0), 15000, 'tab 2 hears the leader');
    let s1 = await state(A1), s2 = await state(A2);
    assert(s1.leader && !s2.leader, 'one leader per computer and world');
    await until(async () => (await state(A1)).n === (await state(A2)).n, 6000, 'the same count in both tabs');
    s2 = await state(A2);
    say(`B · two tabs: A1 leads (${s1.polls} questions), A2 follows (heard ${s2.heard}, count ${s2.n} = the leader's)`);

    /* ── C · another computer's write reaches the surfaces of both tabs ── */
    await A1.waitForFunction(rids => rids.some(r => document.querySelector(`[data-rid="${r}"]`)), rids);
    const shown = await A1.evaluate(() => PlacementFeed.watched());
    assert(shown.length, 'orders on screen in tab 1');
    const rid = shown[0];
    await A1.mouse.move(300, 300);   // (somebody is at this computer: the active pace)
    await new Promise(r => setTimeout(r, 2500));
    let t0 = Date.now();
    await lib({ op: 'poolUpdate', poolIds: [`${rid}_55555_1`], patch: { note: 'held elsewhere' } });   // (computer B: a pool row of an order on A's screen)
    const tPieces = await until(() => since(t0, x => x.tag === 'A1' && x.op === 'getOrderPieces' && x.body.includes(rid)).length > 0, 8000, 'tab 1 reads the order that moved');
    t0 = Date.now();
    await lib({ op: 'customPut', key: `${rids[1]}_77777`, receiptId: rids[1], how: 'button', by: 'Ana_M' });   // (computer B: a Complete Order)
    const tReview1 = await until(() => since(t0, x => x.tag === 'A1' && x.op === 'customGet').length > 0, 8000, 'tab 1 reads the custom records');
    const tReview2 = await until(() => since(t0, x => x.tag === 'A2' && x.op === 'customGet').length > 0, 8000, 'tab 2 reads the custom records');
    await new Promise(r => setTimeout(r, 1500));
    const extra = since(t0, x => !['getChanges', 'customGet', 'getOrderPieces', 'cancelList', 'masterGet', 'listingPhotos', 'timelineAdd'].includes(x.op));
    say(`C · another computer's pool row → tab 1 reads that order in ${tPieces} ms; its Complete Order → customGet in tab 1 ${tReview1} ms, tab 2 ${tReview2} ms (other calls: ${extra.map(x => x.op).join(' ') || 'none'})`);
    assert(tPieces < 5000 && tReview1 < 5000 && tReview2 < 5000, 'a change on another computer within the active pace and a read');
    // a write made in tab 1 reaches tab 2 at once (the answer's X-CN-Feed, told on the channel: wrote), not at the next question; and that question reads it no more
    await A2.evaluate(m => document.querySelector(`#modeSeg button[data-mode="${m}"]`).click(), 'orders');
    await A2.waitForFunction(rid => PlacementFeed.watched().includes(rid), rid);
    await new Promise(r => setTimeout(r, 2500));
    t0 = Date.now();
    const tw = await A1.evaluate(async rid => { await api('charmNestLibrary', { op: 'poolUpdate', poolIds: [`${rid}_66666_1`], patch: { note: 'held here' } }, { quiet: true }); return Date.now(); }, rid);
    const tSib = await until(() => since(t0, x => x.tag === 'A2' && x.op === 'getOrderPieces' && x.body.includes(rid)).length > 0, 5000, 'tab 2 reads what tab 1 wrote');
    const sib = since(t0, x => x.tag === 'A2' && x.op === 'getOrderPieces' && x.body.includes(rid))[0].t - tw;
    await new Promise(r => setTimeout(r, 3500));
    const again = since(t0, x => x.tag === 'A2' && x.op === 'getOrderPieces' && x.body.includes(rid)).length;
    say(`C · a write in tab 1 → tab 2 reads it ${sib} ms after tab 1 had its answer (told on the channel, wrote); the next feed answer read it again ${again - 1} time(s)`);
    assert(sib <= 500, 'a sibling tab reads a write of its own computer within 0.5 s: ' + sib + ' ms');
    assert(again <= 1, 'the feed\'s next answer does not send it to read the same write again: ' + again);

    /* ── D · the windows: an order window hears a station's scan, a sheet window its sheet's save ── */
    const key = await A1.evaluate(rid => Orders.rows().find(r => String(r.order.receiptId) === rid).key, rid);
    await A1.evaluate(k => OrderWin.open(k), key);
    await until(() => state(A1).then(s => s.watched.includes('o:' + rid)), 8000, 'the leader watches the open order window\'s Order_Rev');
    await new Promise(r => setTimeout(r, 4000));
    t0 = Date.now();
    await fetch(srv.sorterOrigin + '/.netlify/functions/firebaseOrders', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ timeline: [{ orderId: rid, type: 'scan', at: Date.now(), by: 'Bea', station: 'shipping', id: 'scan-' + Date.now() }] }) });
    const tTl = await until(() => since(t0, x => x.tag === 'A1' && x.op === 'timelineGet' && x.body.includes(rid)).length > 0, 8000, 'the order window reads its timeline');
    await A1.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); });
    await A1.evaluate(() => SheetWin.open('ready-sheet'));
    await A1.waitForFunction(() => document.querySelector('dialog[open]') && window.CNSync);
    await new Promise(r => setTimeout(r, 4000));
    t0 = Date.now();
    await lib({ op: 'putSheet', sheet: { id: 'ready-sheet', note: 'saved elsewhere' } });
    const tSw = await until(() => since(t0, x => x.tag === 'A1' && x.op === 'laserStatus' && x.body.includes('ready-sheet')).length > 0, 8000, 'the sheet window asks after its sheet');
    say(`D · a station's scan → the open order window reads its timeline in ${tTl} ms; another computer's save → the sheet window asks after its sheet in ${tSw} ms`);
    await A1.keyboard.press('Escape').catch(() => {});

    /* ── E · handover: hidden, then closed ── */
    const A3 = await tab(A, 'A3', 'library');
    await until(() => state(A3).then(s => s.live), 15000, 'tab 3 hears the leader');
    const hide = p => p.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' }); Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); document.dispatchEvent(new Event('visibilitychange')); });
    t0 = Date.now(); await hide(A1);
    const tHide = await until(async () => (await state(A2)).leader || (await state(A3)).leader, 8000, 'a visible tab leads once the leader is hidden');
    assert(!(await state(A1)).leader, 'a hidden tab does not lead');
    const L = (await state(A2)).leader ? A2 : A3, other = L === A2 ? A3 : A2;
    t0 = Date.now(); await L.close();
    const tClose = await until(() => state(other).then(s => s.leader), 8000, 'the last visible tab leads once the leader is closed');
    const n0 = (await state(other)).n;
    await lib({ op: 'putSheet', sheet: { id: 'cut-sheet', note: 'after the handover' } });
    await until(() => state(other).then(s => s.n !== n0), 8000, 'the new leader goes on asking');
    say(`E · the leader hidden → another tab leads in ${tHide} ms; that one closed → the last leads in ${tClose} ms and goes on from count ${n0}`);
    assert(tHide < 5000 && tClose < 5000, 'a handover within a few seconds');

    /* ── F · worlds apart: a sandbox tab of the same computer ── */
    await other.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.sandbox = 'on'; localStorage.setItem('cn.settings', JSON.stringify(s)); });
    const SB = await tab(A, 'SB', 'orders');
    await other.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); delete s.sandbox; localStorage.setItem('cn.settings', JSON.stringify(s)); });
    await until(() => state(SB).then(s => s.leader && s.live), 15000, 'the sandbox tab leads its own world');
    const sbs = await state(SB), ps = await state(other);
    assert(sbs.world === 'sandbox' && ps.world === 'production' && ps.leader, 'one leader per world on one computer');
    const sbN = sbs.n, pN = ps.n;
    await lib({ op: 'putSheet', sheet: { id: 'pending-sheet', note: 'production only' } });
    await until(() => state(other).then(s => s.n !== pN), 8000, 'production hears production');
    assert.equal((await state(SB)).n, sbN, 'the sandbox never hears production');
    st.put('Sandbox_Charm_Nest_Sheets', 'sb-sheet', { id: 'sb-sheet', metal: 'gold', day: '2026-10-28' });
    await lib({ op: 'putSheet', sandbox: true, sheet: { id: 'sb-sheet', note: 'sandbox' } });
    await until(() => state(SB).then(s => s.n !== sbN), 8000, 'the sandbox hears its own');
    const sbCalls = since(0, x => x.tag === 'SB' && x.op === 'getChanges'), mixed = sbCalls.filter(x => !/"sandbox":true/.test(x.body));
    assert.equal(mixed.length, 0, 'every question of the sandbox tab is the sandbox\'s');
    say(`F · worlds apart: production leads at count ${(await state(other)).n}, the sandbox tab leads its own at ${(await state(SB)).n}; ${sbCalls.length} sandbox questions, none to production`);
    await SB.close();

    /* ── G · calls per minute: two tabs, nothing changing; and an older cloud ── */
    const G2 = await tab(A, 'G2', 'library');
    await other.evaluate(m => document.querySelector(`#modeSeg button[data-mode="${m}"]`).click(), 'orders');
    await new Promise(r => setTimeout(r, 5000));
    t0 = Date.now();
    for (let i = 0; i < 6; i++) { await other.mouse.move(300 + i * 10, 300); await new Promise(r => setTimeout(r, 5000)); }
    const g = since(t0), per = op => g.filter(x => x.op === op).length, mins = (Date.now() - t0) / 60000;
    const polls = per('getChanges'), feeds = ['getOrderPieces', 'cancelList', 'customGet', 'laserStatus', 'timelineGet'].map(op => `${op}×${per(op)}`).join(' ');
    say(`G · two tabs (Orders active + Library), nothing changing, ${Math.round(mins * 60)} s: ${g.length} calls (${(g.length / mins).toFixed(1)}/min) — getChanges×${polls} ${feeds}`);
    assert(polls / mins <= 24, `≤ 20 questions a minute for the computer (+ a margin): ${(polls / mins).toFixed(1)}`);
    assert(['getOrderPieces', 'cancelList', 'customGet', 'timelineGet'].every(op => per(op) <= 1) && per('laserStatus') <= 2, 'nothing changing: the surfaces read nothing (' + feeds + ')');
    await G2.close();
    const O = await computer({ noFeed: true });
    const O1 = await tab(O, 'O1', 'orders');
    await until(() => O1.evaluate(() => CNSync.state().dead), 15000, 'an older cloud: the feed stops');
    t0 = Date.now();
    for (let i = 0; i < 3; i++) { await O1.mouse.move(300 + i * 10, 300); await new Promise(r => setTimeout(r, 4000)); }
    const old = since(t0, x => x.tag === 'O1' && x.op === 'getOrderPieces').length;
    say(`G · an older cloud (no changes op): the feed stops, the placement feed reads on its own timer (${old} getOrderPieces in 12 s)`);
    assert(old >= 2, 'without the feed every surface keeps its own timer');
    await O.close(); await A.close();
    console.log('sync-worker: OK');
  } catch (e) { console.error(e); process.exitCode = 1; }
  finally { if (browser) await browser.close().catch(() => {}); srv.close(); }
})();
