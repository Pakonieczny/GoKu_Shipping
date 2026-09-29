// The Send to Sheet tour holds the nest of the pieces it flies until each one has landed (Paul, 29 Sep 16:09 UTC: "I also
// noticed that the nesting of the moved charm had already started prior to the completion of the animation"). The press
// still puts the pieces in the pool and on their sheet's list at once (and the record is written); what waits is the
// sheet's nest (its status, its progress, the search drawn, the sheet write), the run's restart ("Nesting · sheet 1 of 1")
// and the sheet's card, which is drawn without them; each is released at the landing beat (the tour's "tour:land": the
// coin sets down, NestFocus land), sheet by sheet in the tour's order. In a real Chromium on the fake site (bridge-server.cjs),
// a run that is resting ("processed", so a line sent to a sheet restarts it: RunCtl.poke, the Auto path):
//   1 · one piece, from the designs window: until the landing beat there is no nest start, no run step "Nesting", no
//       sheet status but the one it had, no change to the sheet's card, no picture drawn with the piece, no sheet write;
//       the pool record is written at once; at the landing beat the nest starts (the run's banner says Nesting), and the piece is placed;
//   2 · one order on two sheets (GF, SS): GF's nest starts at its landing and not before, SS's at its own landing, in the
//       animation's order; SS is untouched until then;
//   3 · a click mid-flight, and 4 · Esc: the nest of everything still flying starts at once;
//   5 · the tour off (tourOk() false: no SendTour on the page) and 6 · reduced motion: nothing is ever held;
//   7 · a tour that never ends: the safety timer lets the nest go, a few seconds past the tour's own longest length;
//   8 · Manual (the run is paused for a person): nothing asked the sheet to nest, so the piece waits in the card's queue and
//       "Queued on Sheet 1 · press Nest" is said, as before; nothing is held once the tour is home.
// PROFILE: the nest of a single piece takes the worker some seconds on a quiet machine.
//   node tests/charm-nest/tour-nest-holds.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = (w, h = 20) => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, h, 10, 0, 20, h,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, h - 4, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, n) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });

/** In the page, from the press: the nest starts, the tour's marks, and, every frame, what the run, the sheets and their cards say. */
function instrument() {
  const H = window.__H = { rec: false }, T = () => Math.round(performance.now() - H.t0);
  // (a start is a call that took: one the hold turned away leaves the sheet as it was)
  const sn = window.startNest; window.startNest = function (sh) { const was = sh.status, out = sn.apply(this, arguments); if (H.rec && (sh.status !== was || ['queued', 'nesting'].includes(sh.status))) H.starts.push([T(), sh.metal, sh.status, sh.charms.length]); return out; };
  const pp = window.paintPreview; window.paintPreview = function (cv, sh, clean, R) { if (H.rec && !clean && cv && cv.dataset && cv.dataset.r === 'canvas') H.paints.push([T(), sh.metal, sh.charms.length]); return pp.apply(this, arguments); };
  new PerformanceObserver(l => { if (H.rec) for (const e of l.getEntries()) if (/^tour:/.test(e.name)) H.marks.push([e.name.slice(5), Math.round(e.startTime - H.t0)]); }).observe({ entryTypes: ['mark'] });
  // the sheet card as the person reads it: its pill, its line, its queue, its fill, its buttons, its tabs (not what the tour lays over it)
  const card = m => { const c = CN.S.sheets[m].cardEl, q = r => c.querySelector(`[data-r="${r}"]`);
    return [q('pill').hidden ? '' : q('pill').textContent, q('stage').textContent, q('queue').querySelectorAll('img').length, q('sat').textContent.replace(/\s+/g, ' '), q('hint').classList.contains('hidden'), q('nest').disabled, q('nest').classList.contains('hidden'), q('prog').hidden, q('overlay').textContent, q('prov').textContent, q('tabs').textContent].join('|'); };
  let last = '';
  const tick = () => {
    if (H.rec) {
      const run = B.run || {}, page = m => CN.S.sheets[m].pages[CN.S.sheets[m].active] || CN.S.sheets[m], banner = (document.querySelector('#runBanner .rbText') || {}).textContent || '';
      const s = JSON.stringify([window.NestHold.active(), run.step, run.status, banner, page('gold').status, page('silver').status, CN.S.mode]);
      if (s !== last) { last = s; H.line.push([T(), ...JSON.parse(s)]); }
      for (const m of ['gold', 'silver']) { const c = card(m); if (c !== H.cardNow[m]) { H.cardNow[m] = c; H.cards.push([T(), m, c]); } }
    }
    requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
  H.start = () => { Object.assign(H, { rec: true, t0: performance.now(), starts: [], marks: [], line: [], cards: [], paints: [], cardNow: { gold: card('gold'), silver: card('silver') } }); H.base = Object.assign({}, H.cardNow); last = ''; return H.t0; };
  H.stop = () => { H.rec = false; return { starts: H.starts, marks: H.marks, line: H.line, cards: H.cards, paints: H.paints, base: H.base }; };
}

(async () => {
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const wall = [];   // the fake cloud's calls, each with the time it came
  const push = srv.st.calls.push; srv.st.calls.push = function (c) { c.t = Date.now(); return push.call(this, c); };
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
  const errors = [], only = process.env.ONLY ? process.env.ONLY.split(',') : null, on = n => !only || only.includes(String(n));

  /** A fresh page on the fake site, its lines pulled, a run in the given state; each scene has its own orders. */
  async function scene(rids, { run = 'processed', reducedMotion = false } = {}) {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: reducedMotion ? 'reduce' : 'no-preference' });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.SendTour && window.NestFocus && window.NestHold && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(instrument);
    await page.evaluate(async ([orders, run]) => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const day = new Date().toISOString().slice(0, 10), id = `run-${day}-hold${Math.random().toString(36).slice(2, 6)}`;
      // resting: a line sent to a sheet restarts the run (RunCtl.poke); manual: paused for a person, nothing restarts
      B.run = run === 'processed'
        ? { runId: id, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'complete', status: 'processed', processingComplete: true, processingSignature: '', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }
        : { runId: id, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, [rids.map((r, i) => order(r, 5 - i)), run]);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    // designs dropped on a card, each on its metal
    const drop = async (rid, metals) => {
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`; await page.waitForSelector(card);
      await page.evaluate(({ sel, files }) => { const dt = new DataTransfer(); for (const f of files) dt.items.add(new File([f.text], f.name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); },
        { sel: card, files: metals.map((m, i) => ({ name: `design-${i + 1}.dxf`, text: DXF(18 + i * 4) })) });
      await page.waitForFunction(n => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === n, metals.length, { timeout: 30000 });
      for (const [i, m] of metals.entries()) await page.click(`#cuDlg .cuFile:nth-child(${i + 1}) .cuM[data-m="${m}"]`);
      await page.waitForTimeout(500);
    };
    // the press, recorded from the frame it is made in; t0 in the wall clock, to set the cloud's calls beside the page's
    const press = () => page.evaluate(() => { const t0 = __H.start(); document.querySelector('#cuDlg [data-send]').click(); return performance.timeOrigin + t0; });
    // (the tour starts a moment after the press, once the pieces are pooled: wait for its end mark, then for its layer to be gone)
    const done = async (t0, over = 30000) => { await page.waitForFunction(() => __H.marks.some(m => m[0] === 'end') && !SendTour.playing() && !document.querySelector('#tourLayer > *'), null, { timeout: over }); };
    const readAll = () => page.evaluate(() => __H.stop());
    return { context, page, drop, press, done, readAll, close: () => context.close() };
  }
  const at = (r, name) => r.marks.filter(m => m[0] === name).map(m => m[1]);
  const startsOf = (r, metal) => r.starts.filter(s => s[1] === metal).map(s => s[0]);
  const poolIdsOf = (page, rid) => page.evaluate(rid => B.orders.rows.filter(x => String(x.order.receiptId) === rid).flatMap(x => x.poolIds || []), rid);
  const placed = (page, metal, n, over = 90000) => page.waitForFunction(([m, n]) => { const p = CN.S.sheets[m].pages.at(-1); return p.placements.length >= n && ['complete', 'partial', 'finishing'].includes(p.status); }, [metal, n], { timeout: over });
  const rebuilt = (r, m) => r.cards.filter(c => c[1] === m);
  // a card that changed before `t` (the sheet's card as the person reads it), and a picture drawn with a piece
  const cardChanged = (r, t) => ['gold', 'silver'].flatMap(m => rebuilt(r, m).filter(c => c[0] < t && c[2] !== r.base[m]).map(c => [c[0], m]));
  const banner = r => r.line.map(l => [l[0], l[4]]);

  try {
    // ── 1 · one piece, Auto: nothing of its nest until the landing beat, then the nest and the piece placed ──
    if (on(1)) {
      const s = await scene(['4175423829']); const rid = '4175423829';
      await s.drop(rid, ['gold']);
      await s.page.evaluate(() => { window.__said = []; new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && /tourCap/.test(n.className)) window.__said.push(n.textContent); }).observe(document.body, { childList: true, subtree: true }); });
      const t0 = await s.press();
      await s.done(t0, 40000);
      const r = await s.readAll(), ids = await poolIdsOf(s.page, rid), land = at(r, 'land'), L = land[0];
      console.log(`  1 · tour ${JSON.stringify(r.marks)}; nest starts ${JSON.stringify(r.starts)}; run ${JSON.stringify(r.line.map(l => [l[0], l[1] ? 'HELD' : '-', l[2] + '/' + l[3], l[4], l[5]]))}`);
      const calls = srv.st.calls.filter(c => c.t >= t0), rel = c => c.t - t0;
      check(land.length === 1 && ids.length === 1, `the tour lands its one piece (land marks ${JSON.stringify(land)}; pieces ${ids.length})`);
      const pooled = calls.find(c => c.op === 'poolPut' && (c.body.pools || []).some(p => ids.includes(p.poolId)));
      check(pooled && rel(pooled) < L, `the pool record is written at once, before the landing (poolPut at ${pooled && Math.round(rel(pooled))} ms, landing at ${L} ms)`);
      check(!r.starts.some(x => x[0] < L), `no nest starts before the landing beat (starts ${JSON.stringify(r.starts.map(x => x.slice(0, 3)))}, landing ${L})`);
      const h0 = r.line.find(l => l[1]);
      check(h0 && h0[0] < 400 && !r.line.some(l => l[0] > h0[0] && l[0] < L && !l[1]), `held from the press (${h0 && h0[0]} ms) until the landing`);
      check(!r.line.some(l => l[0] < L && (l[2] === 'nest' || /Nesting/.test(l[4]) || ['nesting', 'queued', 'finishing'].includes(l[5]))),
        `the run says nothing of nesting and no sheet is nesting, queued or finishing while it is held: ${JSON.stringify(r.line.filter(l => l[0] < L).map(l => [l[0], l[1], l[2], l[3], l[4], l[5]]))}`);
      check(!calls.some(c => c.op === 'putSheet' && rel(c) < L), `no sheet is written before the landing beat (${JSON.stringify(calls.filter(c => c.op === 'putSheet').map(c => Math.round(rel(c))))})`);
      check(!cardChanged(r, L).length, `the sheet's card does not change before the landing beat (${JSON.stringify(cardChanged(r, L))})`);
      const withPiece = r.paints.filter(p => p[0] < L && p[1] === 'gold' && p[2] > 0);
      check(!withPiece.length && r.paints.some(p => p[0] < L && p[1] === 'gold'), `every picture drawn of the sheet before the landing is drawn without the piece (${r.paints.filter(p => p[0] < L && p[1] === 'gold').length} drawn, ${withPiece.length} with it)`);
      const st = startsOf(r, 'gold')[0];
      check(st >= L && st - L < 400, `the nest starts at the landing beat (landing ${L} ms, start ${st} ms)`);
      const after = rebuilt(r, 'gold').filter(c => c[0] >= L);
      check(after.some(c => /Nesting|Queued|Ready/.test(c[2].split('|')[0])) || after.some(c => c[2] !== r.base.gold), `the card shows the piece as it lands (${after.length} changes after the landing; first at ${after[0] && after[0][0]} ms)`);
      check(r.line.some(l => l[0] >= L && (l[2] === 'nest' || /Nesting/.test(l[4]))), `the run's banner turns to Nesting once it has (${JSON.stringify(banner(r).filter(b => b[0] >= L).slice(0, 3))})`);
      check(await s.page.evaluate(() => !NestHold.active()), 'nothing is held once the tour is home');
      const said1 = await s.page.evaluate(() => window.__said.slice());
      check(said1.some(x => /^Placed on Sheet 1/.test(x) && /nesting now/.test(x)) && !said1.some(x => /press Nest/.test(x)), `Auto: it is said placed and nesting now, not queued for Nest (${JSON.stringify(said1.filter(x => /Sheet 1/.test(x)))})`);
      await placed(s.page, 'gold', 1);
      console.log(`     the piece was placed ${Math.round(Date.now() - t0 - L)} ms after the landing (the worker's own time on this machine)`);
      check(await s.page.evaluate(id => { const sh = Pool.sheetOf(id); return !!sh && sh.placements.length === 1; }, ids[0]), 'the piece is placed on its Gold sheet');
      await s.close();
    }

    // ── 2 · one order on two sheets: each nest at its own landing, in the animation's order ──
    if (on(2)) {
      const s = await scene(['4175423841']); const rid = '4175423841';
      await s.drop(rid, ['gold', 'silver']);
      const t0 = await s.press();
      await s.done(t0, 40000);
      const r = await s.readAll(), land = at(r, 'land'), [L1, L2] = land, g = startsOf(r, 'gold'), sv = startsOf(r, 'silver');
      console.log(`  2 · landings ${JSON.stringify(land)}; GF starts ${JSON.stringify(g)}, SS starts ${JSON.stringify(sv)}`);
      check(land.length === 2 && L2 > L1, `two landings, GF's then SS's (${JSON.stringify(land)})`);
      check(g.length >= 1 && g[0] >= L1 && g[0] < L2, `GF's nest starts at its landing, before SS's piece has landed (start ${g[0]}, landings ${L1}, ${L2})`);
      check(sv.length >= 1 && sv[0] >= L2 && sv[0] - L2 < 400, `SS's nest starts at SS's own landing (start ${sv[0]}, landing ${L2})`);
      check(!r.line.some(l => l[0] < L1 && (l[2] === 'nest' || /Nesting/.test(l[4]) || ['nesting', 'queued'].includes(l[5]) || ['nesting', 'queued'].includes(l[6]))), 'nothing nests before the first landing');
      check(!r.line.some(l => l[0] < L2 && ['nesting', 'queued', 'finishing'].includes(l[6])), `SS shows no nest until its landing (${JSON.stringify(r.line.filter(l => l[0] < L2).map(l => [l[0], l[5], l[6]]))})`);
      check(!cardChanged(r, L1).length && !rebuilt(r, 'silver').filter(c => c[0] < L2 && c[2] !== r.base.silver).length, `neither card changes before the landing of its own piece (GF before ${L1}, SS before ${L2})`);
      check(!srv.st.calls.some(c => c.op === 'putSheet' && c.t >= t0 && c.t - t0 < L1) && await s.page.evaluate(() => !NestHold.active()), 'no sheet write before the first landing; nothing is held once the tour is home');
      await s.close();
    }

    // ── 3 · a click, and 4 · Esc, while the piece is still flying: the nest starts at once ──
    for (const [n, how] of [[3, 'click'], [4, 'Esc']]) {
      if (!on(n)) continue;
      const rid = n === 3 ? '4175423851' : '4175423852', s = await scene([rid]);
      await s.drop(rid, ['gold']);
      const t0 = await s.press();
      await s.page.waitForFunction(() => CN.S.mode === 'nest' && NestFocus.isOpen(), null, { timeout: 15000, polling: 'raf' });
      await s.page.waitForTimeout(600);
      const before = await s.page.evaluate(() => ({ starts: __H.starts.length, held: NestHold.active(), marks: __H.marks.map(m => m[0]) }));
      const tk = await s.page.evaluate(() => Math.round(performance.now() - __H.t0));
      if (how === 'click') await s.page.mouse.click(720, 480); else await s.page.keyboard.press('Escape');
      await s.page.waitForFunction(() => __H.starts.length > 0, null, { timeout: 4000, polling: 'raf' }).catch(() => {});
      const r0 = await s.page.evaluate(() => ({ starts: __H.starts.slice(), held: NestHold.active(), t: Math.round(performance.now() - __H.t0) }));
      await s.done(t0, 15000);
      const r = await s.readAll();
      console.log(`  ${n} · ${how} at ${tk} ms; before it ${JSON.stringify(before)}; nest starts ${JSON.stringify(r0.starts)}`);
      check(before.starts === 0 && before.held && !before.marks.includes('land'), `${how}: while it flies the nest is held (no start, held, not landed yet)`);
      check(r0.starts.length >= 1 && r0.starts[0][0] - tk < 400 && !r0.held, `${how}: the nest starts at once (${r0.starts[0] && r0.starts[0][0] - tk} ms after it), nothing is held`);
      check(startsOf(r, 'gold').length >= 1 && await s.page.evaluate(() => !NestHold.active()), `${how}: home, and nothing is left held`);
      await s.close();
    }

    // ── 5 · the tour off (no SendTour: tourOk() false), 6 · reduced motion: nothing is ever held ──
    for (const [n, what, opts] of [[5, 'tourOk() false', {}], [6, 'reduced motion', { reducedMotion: true }]]) {
      if (!on(n)) continue;
      const rid = n === 5 ? '4175423861' : '4175423862', s = await scene([rid], opts);
      await s.drop(rid, ['gold']);
      if (n === 5) await s.page.evaluate(() => { window.__ST = window.SendTour; window.SendTour = undefined; });
      await s.page.evaluate(() => { window.__held = false; const f = () => { if (NestHold.active()) window.__held = true; requestAnimationFrame(f); }; f(); });
      const t0 = await s.press();
      await s.page.waitForFunction(() => __H.starts.length > 0, null, { timeout: 12000, polling: 'raf' }).catch(() => {});
      const r0 = await s.page.evaluate(() => ({ starts: __H.starts.slice(), held: window.__held, mode: CN.S.mode }));
      console.log(`  ${n} · ${what}: nest starts ${JSON.stringify(r0.starts)}, ever held ${r0.held}`);
      check(r0.starts.length >= 1 && r0.starts[0][0] < 2500 && !r0.held, `${what}: the nest starts at once (${r0.starts[0] && r0.starts[0][0]} ms after the press) and nothing was ever held`);
      if (n === 5) await s.page.evaluate(() => { window.SendTour = window.__ST; });
      await s.close();
    }

    // ── 7 · a tour that never ends: the safety timer lets the nest go a few seconds past the tour's own length ──
    if (on(7)) {
      const rid = '4175423871', s = await scene([rid]);
      await s.drop(rid, ['gold']);
      await s.page.evaluate(() => { window.SendTour.play = () => new Promise(() => {}); window.SendTour.limit = () => 600; });
      const t0 = await s.press();
      await s.page.waitForFunction(() => __H.starts.length > 0, null, { timeout: 12000, polling: 'raf' }).catch(() => {});
      const r0 = await s.page.evaluate(() => ({ starts: __H.starts.slice(), held: NestHold.active() }));
      console.log(`  7 · a tour that never ends (its length 0.6 s): nest starts ${JSON.stringify(r0.starts)}`);
      check(r0.starts.length >= 1 && r0.starts[0][0] >= 3000 && r0.starts[0][0] < 6500 && !r0.held, `the safety timer lets the nest go 3 s past the tour's length (${r0.starts[0] && r0.starts[0][0]} ms after the press)`);
      await s.close();
    }

    // ── 8 · Manual: nothing asked the sheet to nest, so it is drawn again at the landing and waits for Nest, as before ──
    if (on(8)) {
      const rid = '4175423881', s = await scene([rid], { run: 'manual' });
      await s.drop(rid, ['gold']);
      const said = await s.page.evaluate(() => { window.__said = []; new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && /tourCap/.test(n.className)) window.__said.push(n.textContent); }).observe(document.body, { childList: true, subtree: true }); return 1; });
      const t0 = await s.press();
      await s.done(t0, 40000);
      const r = await s.readAll(), L = at(r, 'land')[0], said8 = await s.page.evaluate(() => window.__said.slice());
      console.log(`  8 · landing ${L}; nest starts ${JSON.stringify(r.starts)}; said ${JSON.stringify(said8)}`);
      check(!r.starts.length && !r.line.some(l => ['nesting', 'queued'].includes(l[5])), 'Manual: no nest starts, before the landing or after it; the piece waits in the card');
      check(said8.some(x => /^Queued on Sheet 1/.test(x) && /press Nest/.test(x)), `Manual: it is said queued, "press Nest" (${JSON.stringify(said8.filter(x => /Sheet 1/.test(x)))})`);
      const card = rebuilt(r, 'gold').filter(c => c[0] >= L);
      if (process.env.DEBUG) console.log('     card after landing', JSON.stringify(card), 'base', JSON.stringify(r.base.gold));
      check(card.length >= 1 && card.at(-1)[2].split('|')[2] === '1' && card.at(-1)[2].split('|')[5] === 'false' && await s.page.evaluate(() => !NestHold.active() && CN.S.sheets.gold.pages[0].status === 'ready' && CN.S.sheets.gold.pages[0].charms.length === 1), `Manual: the card shows the queued piece from the landing on, and nothing is held (${card.length} card changes after it)`);
      check(!cardChanged(r, L).length, `Manual: the card does not change before the landing beat (${JSON.stringify(cardChanged(r, L))})`);
      await s.close();
    }
    check(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); await srv.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
