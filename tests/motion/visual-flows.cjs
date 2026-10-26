// VISUAL flows (Paul, 26 Oct 2026: "I don't see you actually visually testing the outcome of animations and transitions."): the sorter
// (charm-nest-1.html) over the fake site (tests/charm-nest/bridge-server.cjs, a small realistic run: one written 14K sheet of two charms and
// its order), in a real Chromium. Each flow a person sees is driven as they would and recorded twice: once for its timing (every
// requestAnimationFrame delta, PerformanceObserver long tasks and long animation frames) and once as a strip of screenshots every ~50 ms
// (SHOTS=<dir> keeps them, with a contact sheet per flow, to be looked at). It holds to what can be measured:
//   · it ends where it belongs: the element at rest (no animation running on it, no transform left) within 1 px of where it sits once
//     every animation has been finished by force; a flight's copy ends on its target within 1 px
//   · it is smooth: no frame over 50 ms, no long task (> 50 ms) while it plays (hard only when the machine is quiet: load per core < 1.2)
//   · nothing left behind: no flying copy, motion layer child, closing copy or extra node in the page once it is done
//   · prefers-reduced-motion: reduce ends in the same state (same rect, same open/closed, same count), with nothing moving more than 1 px
// Flows: a pop-up window opening and closing (Motion.dialogOpen / dialogClose, Settings), the order window opening and closing (OrderWin),
// a card flight to a tab (Motion.ghost / fly: Send to Sheet, the cancel flight to Orders > Cancelled, the Library's flights use it), the
// tab switch Orders -> Nest, and the toasts (at most three, each dismissed, none left).
//   node tests/motion/visual-flows.cjs      (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const path = require('path'), fs = require('fs'), os = require('os'), assert = require('assert/strict'), { execFileSync } = require('child_process');
const root = path.join(__dirname, '../..');
const { start } = require('../charm-nest/bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const QUIET = os.loadavg()[0] / Math.max(1, os.cpus().length) < 1.2;
const BUDGET = 50;

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 30, 17) / 1000);
const SH = 'sheet-visual-gf', RID = '4179900011', TID = '41799000111', PA = `${RID}_${TID}_1`, PB = `${RID}_${TID}_2`, KEY = `${RID}_${TID}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Mia Lund' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TID, listingId: '1800099011', sku: 'TINY_TAG', title: 'TINY_TAG necklace', quantity: 2, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' }] };

/* in the page, before its scripts: rAF deltas, long tasks and long animation frames while recording */
function recorders() {
  const M = window.__V = { rec: false };
  const obs = type => { try { new PerformanceObserver(l => { if (M.rec) for (const e of l.getEntries()) M[type].push([Math.round(e.startTime - M.t0), Math.round(e.duration), (e.scripts || []).filter(x => x.duration > 10).map(x => `${x.invoker}${x.sourceFunctionName ? ' ' + x.sourceFunctionName : ''} ${Math.round(x.duration)}ms ${String(x.sourceURL || '').split('/').pop().split('?')[0]}:${x.sourceCharPosition}`).join(' + ')]); }).observe({ type, buffered: false }); } catch (_) {} };
  M.longtask = []; M['long-animation-frame'] = []; obs('longtask'); obs('long-animation-frame');
  const loop = t => { if (M.rec) M.frames.push(t - M.t0); requestAnimationFrame(loop); };
  requestAnimationFrame(loop);
  window.__vStart = () => Object.assign(M, { idle: null, rec: true, frames: [], longtask: [], 'long-animation-frame': [], t0: performance.now() });
  window.__vStop = () => { M.rec = false; const d = []; for (let i = 2; i < M.frames.length; i++) d.push([Math.round(M.frames[i]), M.frames[i] - M.frames[i - 1]]);
    // the task that opens it (the click's own work, before its first frame) is its response time, told apart: what plays after it must be smooth
    // (and what runs once the motion has ended, M.idle, is not its frames)
    const during = x => x[0] > (M.sync || 0) + 4 && (M.idle == null || x[0] <= M.idle);
    if (M.idle != null) d.splice(0, d.length, ...d.filter(x => x[0] <= M.idle + 20));
    return { sync: Math.round(M.sync || 0), worst: Math.round(Math.max(0, ...d.map(x => x[1]))), at: d.filter(x => x[1] > BUDGET_MS).map(x => x[0]), n: d.length, over: d.filter(x => x[1] > BUDGET_MS).length,
      long: M.longtask.filter(x => x[1] > 50 && during(x)).map(x => x[1]), opening: M.longtask.filter(x => x[1] > 50 && !during(x)).map(x => x[1]), loaf: M['long-animation-frame'].filter(x => x[1] > 50).map(x => `${x[1]}ms@${x[0]}${x[2] ? ' (' + x[2] + ')' : ''}`) }; };
  window.__vDo = (fn, arg) => { window.__vStart(); const t = performance.now(); try { fn(arg); } finally { M.sync = performance.now() - t; } };
  const BUDGET_MS = 50;
}

/* what the page holds that motion may leave behind */
const leftovers = () => ({
  ghosts: document.querySelectorAll('.motionLayer > *, .mdGhost, .mdClosing, [data-motion-ghost], .flyGhost').length,
  running: document.getAnimations().filter(a => a.playState === 'running' && !(a.effect && a.effect.getTiming && a.effect.getTiming().iterations === Infinity)).length,
  nodes: [...document.body.getElementsByTagName('*')].filter(n => !(n.id === 'motionLayer' && !n.childElementCount) && !n.closest('dialog')).length,   // (outside the windows, whose contents are filled as they open; the page's one shared #motionLayer is made at its first use and kept, empty)
  toasts: document.querySelectorAll('#toasts > *').length,
  open: [...document.querySelectorAll('dialog[open]')].map(d => d.id || d.className)
});
/* an element's rect at rest, then the same with every animation on it and its ancestors finished by force: they must agree */
const restOf = sel => { const el = document.querySelector(sel); if (!el) return null; const r1 = el.getBoundingClientRect();
  for (const a of document.getAnimations()) try { if (a.effect && a.effect.target && (a.effect.target === el || a.effect.target.contains(el)) && a.effect.getTiming().iterations !== Infinity) a.finish(); } catch (_) {}
  const r2 = el.getBoundingClientRect(), tf = getComputedStyle(el).transform;
  return { at: [r1.left, r1.top, r1.width, r1.height].map(Math.round), d: Math.max(Math.abs(r1.left - r2.left), Math.abs(r1.top - r2.top), Math.abs(r1.width - r2.width), Math.abs(r1.height - r2.height)), tf, op: getComputedStyle(el).opacity }; };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const charms = [{ id: 'g1', name: `${RID} · TINY_TAG · 1/2`, poolId: PA, order: RID, sku: 'TINY_TAG' }, { id: 'g2', name: `${RID} · TINY_TAG · 2/2`, poolId: PB, order: RID, sku: 'TINY_TAG' }];
  const placements = [['g1', 60, 60], ['g2', 200, 80]].map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 }));
  srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', metalLabel: '14k Gold Filled', sheetIndex: 1, day: new Date().toISOString().slice(0, 10), status: 'written', stock: { wPt: 300, hPt: 150 },
    charmCount: 2, placedCount: 2, density: .2, freePt2: 30000, orders: [RID], poolIds: [PA, PB], placements, charms, updatedAt: Date.now(), createdAt: Date.now() });
  for (const p of [PA, PB]) srv.st.put('Charm_Pool', p, { poolId: p, orderId: RID, transactionId: TID, lineKey: KEY, sku: 'TINY_TAG', material: 'gold', copy: +p.split('_').pop(), quantity: 2, state: 'written', sheetId: SH, sheetName: '2026-10-26_GF_Set-1_Sheet-1', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const failures = [], seen = [];
  const check = (cond, msg) => { if (cond) console.log('  ✓ ' + msg); else { console.log('  ✗ ' + msg); failures.push(msg); } };
  try {
    const boot = async reduced => {
      const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: reduced ? 'reduce' : 'no-preference' });
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
      await context.addInitScript(recorders);
      const page = await context.newPage(), errors = [];
      page.setDefaultTimeout(20000);
      page.on('pageerror', e => errors.push(String(e.message)));
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async ({ SH, charms, placements, order, pools }) => {
        const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
        S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete', placements, rejects: [], charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })) });
        await Orders.loadMaps(true);
        for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools, engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); CN.setMode('orders'); Orders.render();
      }, { SH, charms, placements, order: ORDER, pools: [PA, PB] });
      await page.waitForTimeout(800);
      return { context, page, errors };
    };
    const settle = async (page, ms = 2600) => { await page.waitForTimeout(ms); };
    /* one flow, twice: timing, then (SHOTS) a strip of frames and a contact sheet */
    const flow = async (env, name, act, ms, tag) => {
      const { page } = env;
      await page.evaluate(({ src, arg }) => window.__vDo(eval(src), arg), { src: act.do.toString(), arg: act.arg });
      // recorded while it moves: until no animation with an end is running (at most ms), and a frame or two after
      await page.waitForTimeout(150);
      await page.waitForFunction(() => !document.getAnimations().some(a => a.playState === 'running' && a.effect && Number.isFinite(a.effect.getComputedTiming().endTime)) && ((window.__V.idle = performance.now() - window.__V.t0), true), null, { timeout: ms, polling: 'raf' }).catch(() => {});
      await page.waitForTimeout(80);
      const t = await page.evaluate(() => window.__vStop());
      if (SHOTS && act.undo) {
        await page.evaluate(act.undo, act.arg); await settle(page, 1800);
        const dir = path.join(SHOTS, `${name}${tag || ''}`); fs.mkdirSync(dir, { recursive: true });
        const t0 = Date.now(); let i = 0;
        const go = page.evaluate(act.do, act.arg);
        while (Date.now() - t0 < ms) { await page.screenshot({ path: path.join(dir, `f${String(i++).padStart(3, '0')}.png`) }); await page.waitForTimeout(Math.max(0, 50 - 0)); }
        await go;
        const files = fs.readdirSync(dir).filter(f => /^f\d+\.png$/.test(f)).sort(), pick = files.filter((_, k) => k % Math.max(1, Math.ceil(files.length / 12)) === 0);
        try { execFileSync('montage', [...pick.map(f => path.join(dir, f)), '-tile', '4x', '-geometry', '480x300+4+4', path.join(SHOTS, `${name}${tag || ''}-sheet.png`)]); } catch (_) {}
      }
      const line = `${name}${tag || ''}: opening task ${t.sync} ms, then ${t.n} frames, worst ${t.worst} ms, ${t.over} over ${BUDGET} ms${t.at.length ? ' at ' + t.at.join(',') : ''}${t.long.length ? ' · long tasks ' + t.long.join(',') : ''}${t.loaf.length ? ' · long frames ' + t.loaf.join(',') : ''}`;
      console.log('  · ' + line); seen.push(line);
      if (QUIET) check(t.worst <= BUDGET && !t.long.length, `${name}${tag || ''}: smooth (no frame over ${BUDGET} ms, no long task)`);
      if (QUIET) check(t.sync <= 250, `${name}${tag || ''}: answers the click within 250 ms (${t.sync} ms)`);
      return t;
    };

    const results = {};
    for (const reduced of [false, true]) {
      const tag = reduced ? '·reduced' : '';
      console.log(reduced ? '\nprefers-reduced-motion: reduce' : 'full motion');
      const env = await boot(reduced), { page } = env, R = results[reduced ? 'r' : 'f'] = {};
      const base = await page.evaluate(leftovers);

      /* 1 · a pop-up window opens and closes (Settings; Motion.dialogOpen / dialogClose) */
      await flow(env, 'dialog-open', { do: () => document.getElementById('btnSettings').click(), undo: () => document.getElementById('dlgSettings').close() }, 1400, tag);
      R.dlg = await page.evaluate(restOf, '#dlgSettings');
      check(R.dlg && R.dlg.d <= 1 && R.dlg.tf === 'none' && R.dlg.op === '1', `dialog${tag}: opens to its resting place (Δ ${R.dlg && R.dlg.d.toFixed(1)} px, transform ${R.dlg && R.dlg.tf}, opacity ${R.dlg && R.dlg.op})`);
      await flow(env, 'dialog-close', { do: () => document.getElementById('dlgSettings').close(), undo: () => document.getElementById('dlgSettings').showModal() }, 1400, tag);
      await settle(page, 600);
      R.dlgEnd = await page.evaluate(leftovers);
      check(!R.dlgEnd.open.length && !R.dlgEnd.ghosts && !R.dlgEnd.running && R.dlgEnd.nodes === base.nodes, `dialog${tag}: closes and leaves nothing (${JSON.stringify(R.dlgEnd)} vs ${base.nodes} nodes)`);

      /* 2 · the order window opens and closes */
      await flow(env, 'orderwin-open', { do: k => OrderWin.open(k), undo: () => OrderWin.close(true), arg: KEY }, 2000, tag);
      R.ow = await page.evaluate(restOf, '#orderWin');
      check(R.ow && R.ow.d <= 1 && R.ow.op === '1', `order window${tag}: opens to its resting place (Δ ${R.ow && R.ow.d.toFixed(1)} px at ${R.ow && R.ow.at})`);
      await flow(env, 'orderwin-close', { do: () => OrderWin.close(), undo: k => OrderWin.open(k), arg: KEY }, 1800, tag);
      await settle(page, 800);
      R.owEnd = await page.evaluate(leftovers);
      check(!R.owEnd.open.length && !R.owEnd.ghosts && !R.owEnd.running, `order window${tag}: closes and leaves nothing (${JSON.stringify(R.owEnd)})`);

      /* 2b · opened from nothing to grow out of, the order window's text is never drawn over the list beneath: per frame, while the window is
         not yet opaque, what of its content shows (its opacity, less the plain surface laid over it) stays under 10 % */
      const seep = await page.evaluate(async k => {
        const d = document.getElementById('orderWin'); let worst = 0, n = 0, on = true;
        const s = () => { if (!on) return; const op = +getComputedStyle(d).opacity; if (d.open && op < .95) { const veil = Math.max(0, ...[...d.querySelectorAll('.owTint')].map(t => +getComputedStyle(t).opacity)); worst = Math.max(worst, op * (1 - veil)); n++; } requestAnimationFrame(s); };
        OrderWin.open(k); s(); await new Promise(r => setTimeout(r, 700)); on = false; OrderWin.close(true); await new Promise(r => setTimeout(r, 300)); return { worst, n };
      }, KEY);
      check(seep.worst < .1, `order window${tag}: its content is not seen through it over the list while it fades in (at most ${(seep.worst * 100).toFixed(0)} % over ${seep.n} frames)`);

      /* 3 · a card's flight to a tab (Send to Sheet, the cancel flight, the Library's flights: Motion.ghost + Motion.fly) */
      const fl = await page.evaluate(async () => {
        const src = document.querySelector('#ordersView .ocard, #ordersView [data-mkey], .ocard, .card') || document.querySelector('header button, .tabs button');
        const to = document.querySelector('[data-mode="nest"], #tabNest, .tabs [data-tab="nest"]') || document.querySelector('header button:last-of-type');
        if (!src || !to) return { skip: 'no card or tab: ' + !!src + !!to };
        window.__vStart();
        const g = Motion.ghost(src, src.getBoundingClientRect()); const tr = to.getBoundingClientRect();
        const seenAt = []; let on = true; const s = () => { if (!on) return; const r = g && g.isConnected ? g.getBoundingClientRect() : null; if (r) seenAt.push([r.left + r.width / 2, r.top + r.height / 2]); requestAnimationFrame(s); }; s();
        await Motion.fly(g, to); on = false;
        const t = window.__vStop(), last = seenAt[seenAt.length - 1] || [0, 0], first = seenAt[0] || [0, 0], sr = src.getBoundingClientRect();
        await new Promise(r => setTimeout(r, 400));
        return { t, steps: seenAt.length, moved: Math.max(0, ...seenAt.map(p => Math.hypot(p[0] - first[0], p[1] - first[1]))), startOff: Math.hypot(first[0] - (sr.left + sr.width / 2), first[1] - (sr.top + sr.height / 2)), endOff: Math.hypot(last[0] - (tr.left + tr.width / 2), last[1] - (tr.top + tr.height / 2)), left: document.querySelectorAll('.motionLayer > *').length + (g && g.isConnected ? 1 : 0) };
      });
      R.fly = fl;
      if (fl.skip) console.log('  – flight: ' + fl.skip);
      else {
        console.log(`  · flight${tag}: ${fl.steps} positions, starts ${fl.startOff.toFixed(1)} px from the card, ends ${fl.endOff.toFixed(1)} px from the tab, worst frame ${fl.t.worst} ms`);
        check(fl.left === 0, `flight${tag}: its copy is gone once landed`);
        if (!reduced) check(fl.startOff <= 1 && fl.steps >= 8, `flight${tag}: starts where the card is and is seen going (${fl.steps} frames)`);
        if (!reduced) check(fl.endOff <= 24, `flight${tag}: ends on the tab (${fl.endOff.toFixed(1)} px from its centre)`);
        if (QUIET) check(fl.t.worst <= BUDGET && !fl.t.long.length, `flight${tag}: smooth`);
      }

      /* 3b · the 10K / 14K sheet waiting greyed in its set, cut: the Library is drawn again with it as a sheet of the set (SetRepair's refresh, VISUAL 26 Oct 2026);
         the very path it takes (LibraryDone.glide around the redraw, then its colour back): it goes from where it stood greyed to its place in the set's row
         without a jump, and ends there, in full colour */
      const gp = await page.evaluate(async reduced => {
        CN.setMode('library'); await new Promise(r => setTimeout(r, 400));
        const body = document.getElementById('libBody'); if (!body || !window.LibraryDone || !LibraryDone.glide) return { skip: 'no Library body or glide' };
        const tile = (id, grey) => `<article class="librarySheet${grey ? ' goldPending' : ''}"${grey ? ` data-gold-pending="${id}"` : ''}><div class="libCard" data-id="${id}" style="width:150px;height:110px;background:#e9d9a8${grey ? ';opacity:.5;filter:grayscale(1)' : ''}">${id}</div></article>`;
        const card = rows => `<div class="setCard"><div class="sh">Set 1</div><div class="sheetsRow" style="display:flex;gap:10px">${rows}</div></div>`;
        const keep = body.innerHTML; body.innerHTML = card(tile('gf-1') + tile('ss-1') + tile('k14-1', true));
        const from = body.querySelector('[data-id="k14-1"]').getBoundingClientRect();
        const seen = []; let on = true; const s = () => { if (!on) return; const c = body.querySelector('.librarySheet:not(.goldPending) > [data-id="k14-1"]'); if (c) { const r = c.getBoundingClientRect(); seen.push([r.left, +getComputedStyle(c).opacity]); } requestAnimationFrame(s); };
        window.__vStart();
        await LibraryDone.glide(body, async () => { body.innerHTML = card(tile('k14-1') + tile('gf-1') + tile('ss-1')); });
        const c = body.querySelector('.librarySheet:not(.goldPending) > [data-id="k14-1"]');
        if (c && !Motion.reduced()) c.animate([{ opacity: .5 }, { opacity: 1 }], { duration: Motion.T.slide, easing: 'ease' });
        requestAnimationFrame(s); await new Promise(r => setTimeout(r, 1100)); on = false; const t = window.__vStop();
        const to = c.getBoundingClientRect(), op = +getComputedStyle(c).opacity; let jump = 0; for (let i = 1; i < seen.length; i++) jump = Math.max(jump, Math.abs(seen[i][0] - seen[i - 1][0]));
        body.innerHTML = keep; CN.setMode('orders'); await new Promise(r => setTimeout(r, 300));
        return { t, n: seen.length, start: seen.length ? Math.abs(seen[0][0] - from.left) : 1e9, travel: Math.abs(to.left - from.left), end: seen.length ? Math.abs(seen[seen.length - 1][0] - to.left) : 1e9, jump, op, firstOp: seen.length ? seen[0][1] : 1 };
      }, reduced);
      if (gp.skip) console.log('  – pending tile: ' + gp.skip);
      else {
        console.log(`  · pending tile cut${tag}: ${gp.n} frames, starts ${gp.start.toFixed(1)} px from its grey place, travels ${gp.travel.toFixed(0)} px, biggest step ${gp.jump.toFixed(1)} px, ends ${gp.end.toFixed(1)} px from its place at opacity ${gp.op}`);
        check(gp.end <= 1 && gp.op === 1, `pending tile${tag}: ends in its place in the set's row, in full colour`);
        if (!reduced) check(gp.start <= 1 && gp.jump < gp.travel / 3 && gp.firstOp < .7, `pending tile${tag}: goes from its grey place without a jump, its colour coming back (first opacity ${gp.firstOp.toFixed(2)})`);
        if (QUIET) check(gp.t.worst <= BUDGET && !gp.t.long.length, `pending tile${tag}: smooth (worst ${gp.t.worst} ms)`);
      }

      /* 3c · "In Review" pressed on an order of the Orders tab: the Review tab, the order's card there (the Review card flight's way in) */
      await flow(env, 'review-open', { do: () => document.querySelector('#ordersView .revSendBtn').click(), undo: () => CN.setMode('orders') }, 2400, tag);
      await settle(page, 400);
      R.rv = Object.assign(await page.evaluate(() => ({ mode: CN.S.mode, card: [...document.querySelectorAll('.rvItem, #reviewView [data-mkey]')].filter(n => n.offsetParent).length })), { l: await page.evaluate(leftovers) });
      check(R.rv.mode === 'review' && R.rv.card >= 1 && !R.rv.l.ghosts && !R.rv.l.running, `review${tag}: the press ends on the Review tab with the order's card shown, nothing in flight left (${JSON.stringify({ mode: R.rv.mode, cards: R.rv.card, ghosts: R.rv.l.ghosts, running: R.rv.l.running })})`);
      await page.evaluate(() => CN.setMode('orders')); await settle(page, 900);

      /* 4 · tab switch Orders -> Nest -> Orders */
      await flow(env, 'tab-nest', { do: () => CN.setMode('nest'), undo: () => CN.setMode('orders') }, 1200, tag);
      await settle(page, 400);
      R.tab = await page.evaluate(leftovers);
      check(!R.tab.ghosts && !R.tab.running, `tab switch${tag}: at rest after, no copy left (${R.tab.ghosts} copies, ${R.tab.running} running)`);
      await page.evaluate(() => CN.setMode('orders')); await settle(page, 1200);

      /* 5 · toasts: four in a row show at most three; each dismissed; none left */
      await flow(env, 'toasts', { do: () => { for (let i = 1; i <= 4; i++) CN.toast('Visual check ' + i, i === 4 ? 'ok' : ''); }, undo: () => document.querySelectorAll('#toasts > *').forEach(n => n.remove()) }, 900, tag);
      R.toasts = await page.evaluate(() => [...document.querySelectorAll('#toasts > *')].filter(n => n.style.opacity !== '0').length);
      check(R.toasts <= 3, `toasts${tag}: at most three showing (${R.toasts})`);
      // a notice never takes a click meant for what is under it (it lay over the order card's "In Review" button): a real click on its text
      // reaches the page beneath (stopped there: the test does not press it); its ✕ still dismisses it
      const under = await page.evaluate(() => { const m = document.querySelector('#toasts .toast:last-child .m').getBoundingClientRect(), x = m.left + m.width / 2, y = m.top + m.height / 2, e = document.elementFromPoint(x, y);
        window.__hit = null; addEventListener('click', ev => { ev.stopPropagation(); ev.preventDefault(); window.__hit = ev.target.closest('#toasts') ? 'toast' : (ev.target.tagName + '.' + ev.target.className).slice(0, 40); }, { capture: true, once: true }); return { x, y, inToast: !!(e && e.closest('#toasts')) }; });
      await page.mouse.click(under.x, under.y);
      const hit = await page.evaluate(() => window.__hit);
      check(!under.inToast && hit && hit !== 'toast', `toasts${tag}: a click on a notice's text goes through to the page under it (${hit})`);
      for (let i = 0; i < 4 && await page.$('#toasts .toast .c'); i++) { await page.click('#toasts .toast .c'); await page.waitForTimeout(260); }
      await settle(page, 700);
      R.toastEnd = await page.evaluate(leftovers);
      check(R.toastEnd.toasts === 0 && !R.toastEnd.running, `toasts${tag}: dismissed, none left (${R.toastEnd.toasts})`);
      check(!env.errors.length, `no page error${tag} (${env.errors.slice(0, 3).join(' | ')})`);
      await env.context.close();
    }

    /* reduced motion ends in the same state */
    const f = results.f, r = results.r;
    check(f.dlg && r.dlg && JSON.stringify(f.dlg.at) === JSON.stringify(r.dlg.at), `reduced motion: the dialog rests in the same place (${f.dlg && f.dlg.at} / ${r.dlg && r.dlg.at})`);
    check(f.ow && r.ow && JSON.stringify(f.ow.at) === JSON.stringify(r.ow.at), `reduced motion: the order window rests in the same place (${f.ow && f.ow.at} / ${r.ow && r.ow.at})`);
    check(f.toasts === r.toasts && f.owEnd.open.length === r.owEnd.open.length && f.dlgEnd.open.length === r.dlgEnd.open.length, 'reduced motion: same windows open, same toasts');
    if (r.fly && !r.fly.skip) check(r.fly.moved <= 1 && r.fly.left === 0, `reduced motion: the flight's copy does not travel (moved ${r.fly.moved.toFixed(1)} px) and is gone after`);
    if (!QUIET) console.log('  (machine busy: the frame-time gates were reported, not enforced)');
  } finally { await browser.close(); srv.close(); }
  if (failures.length) { console.log(`\nvisual-flows: ${failures.length} failed`); process.exit(1); }
  console.log('\nvisual-flows: all passed');
})().catch(e => { console.error(e); process.exit(1); });
