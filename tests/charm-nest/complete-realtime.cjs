// COMPLETE / QR PRINT IN REAL TIME (worker LB2, Paul 6 Oct 2026 23:58: "Once an order is Completed/QR Print then all effects must take place in real-time so the user
// is not confused while waiting for the updates to take hold"). The Library is LB1's; this measures and asserts every OTHER surface, and what the cloud is asked.
//
//   node tests/charm-nest/complete-realtime.cjs                  both presses, three cases, the table, the assertions (exit 1 when a surface is late or the cost grew)
//   --only complete|print        one press         --dump   print every reading      --latency 120   ms added to every call to a Netlify function
//   --reduced   reduced motion (the default keeps the app's real stamps and flights, which hold the page's own redraws)      --idle 30   seconds of the idle-cost count
//   --json <file>  write the table      PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules
//
// The shop is the LARGE one of Paul's screenshot (complete-realtime-shop.cjs): 359 orders, 176 Review decisions, 300 sheets in the cloud. The press is the real button
// (Complete Order, Print QR label) on the order window's piece row of an order that has a piece in Review (a rework) and a piece on a sheet (Paul's order 4171450075).
// Three cases, every one with its own pages (the fake backend is one cloud that every page only reads):
//   same tab        the page whose button is pressed: its order window, the Review list behind it, the top bar
//   other tab       more pages of the SAME browser context (own tab, same storage, same BroadcastChannel)
//   other computer  pages of ANOTHER browser context (own storage), which learn of it from the cloud alone
// Time zero is the press (the click) for Complete Order, the moment the order is written for a QR print (the label's print dialog is the person's time); every surface is probed in its own page every 50 ms for "shows the completed state" (PROBES). Alongside: when the write reached the
// cloud (the fake's own clock), what each poll of a page reads from the database (documents and bytes, counted over the fake's reads), and what each call costs.
// Offline only: the fake backend, no Etsy, no AI, no Netlify, no Firestore.
const fs = require('fs'), path = require('path');
const { AsyncLocalStorage } = require('async_hooks');
const root = path.join(__dirname, '../..');
process.env.CHARM_NEST_DELETE_CODE = 'rt-' + Math.random().toString(36).slice(2, 10);
const O = require('./placement-oracle.cjs'), SHOP = require('./complete-realtime-shop.cjs');
const argv = process.argv.slice(2), flag = n => argv.includes('--' + n), opt = (n, d) => { const i = argv.indexOf('--' + n); return i >= 0 ? argv[i + 1] : d; };
const { sleep } = O;
const LATENCY = +opt('latency', 120), ONLY = opt('only', process.env.RT_ONLY || ''), DUMP = flag('dump'), MOTION = !flag('reduced'), IDLE = +opt('idle', 24);
const SAME = 1000, FAR = 3000;   // Paul's targets: the same computer within 1 s, another computer within about 3 s
const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__printed = (window.parent.parent.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;
const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });

/* ───────────── what a surface says once the piece is completed ─────────────
   base: taken before the press (body of a function of q, txt, K, R)   done: true once the surface shows the completed state (body of a function of q, txt, K, R, b)
   K: the piece in Review (its line key)  R: its order number  b: what base returned */
const NUM = `const n = e => parseInt(txt(e), 10)`;
const PROBES = {
  'ow.row':      { label: 'Order window · the piece\'s row (Completed, seal)', base: `return txt(q('#owPcSum .owPcRow[data-piece="' + K + '"]'))`, done: `const r = q('#owPcSum .owPcRow[data-piece="' + K + '"]'); return !!r && /Completed/.test(txt(r)) && !!r.querySelector('.seal')` },
  'ow.rail':     { label: 'Order window · header rail', base: `return txt(q('#owRail .tlNowT'))`, done: `const t = txt(q('#owRail .tlNowT')); return !!t && t !== b && !/^(Finding|Reading|Loading)/i.test(t)` },
  'ow.header':   { label: 'Order window · header line', base: `return txt(q('#owNow'))`, done: `const t = txt(q('#owNow')); return !!t && t !== b` },
  'owSheet.row': { label: 'Order window · Sheet tab', base: `return txt(q('#owSheetPanel .owPieces'))`, done: `const t = txt(q('#owSheetPanel .owPieces')); return !!t && t !== b && /complet|done|hand/i.test(t)` },
  'rv.count':    { label: 'Review · Open / Completed counts', base: `return [...document.querySelectorAll('#reviewView .rvSeg [data-cseg] b')].map(e => parseInt(e.textContent, 10))`, done: `const c = [...document.querySelectorAll('#reviewView .rvSeg [data-cseg] b')].map(e => parseInt(e.textContent, 10)); return c[0] === b[0] - 1 && c[1] === b[1] + 1` },
  'rv.card':     { label: 'Review · the card leaves Open', base: `return !!q('#rvList .reviewListRow[data-rid="' + R + '"]')`, done: `return b === true && !q('#rvList .reviewListRow[data-rid="' + R + '"]')` },
  'list.pill':   { label: 'Orders list · the piece\'s row', base: `return txt(q('#ordItems [data-key="' + K + '"]'))`, done: `const r = q('#ordItems [data-key="' + K + '"]'); return !!r && txt(r) !== b && /complet|done|printed/i.test(txt(r))` },
  'se.now':      { label: 'Search · the order\'s card', base: `return txt(q('.cnsCard .cnsNow'))`, done: `const t = txt(q('.cnsCard .cnsNow')); return !!t && t !== b` },
  'badge.review': { label: 'Top bar · Review number', base: `return parseInt(txt(q('#tabReviewN')), 10)`, done: `return parseInt(txt(q('#tabReviewN')), 10) === b - 1` },
};
// the pages of a computer: where each stands (the tab it shows, what is open over it) and which surfaces it carries
const ROLES = {
  press:   { where: 'the page that pressed: the order window over the Review tab', mode: 'review', win: 'overview', probes: ['ow.row', 'ow.rail', 'ow.header', 'rv.count', 'rv.card', 'badge.review'] },
  review:  { where: 'the Review tab', mode: 'review', probes: ['rv.count', 'rv.card', 'badge.review'] },
  orders:  { where: 'the Orders tab, the order found', mode: 'orders', find: true, probes: ['list.pill', 'badge.review'] },
  owView:  { where: 'the Nest tab, the order window open', mode: 'nest', win: 'overview', probes: ['ow.row', 'ow.rail', 'ow.header', 'badge.review'] },
  owSheet: { where: 'the Nest tab, the order window on its Sheet tab', mode: 'nest', win: 'sheet', probes: ['owSheet.row', 'badge.review'] },
  engrave: { where: 'the Engrave tab, the search box open', mode: 'engrave', search: true, probes: ['se.now', 'badge.review'] },
  // (nothing open over the Engrave tab: the top bar's Review number alone; another computer reads nothing here, by design: a read every 2 s for a page that shows no list would be the cost of an hour of polling)
  bare:    { where: 'the Engrave tab, nothing open', mode: 'engrave', probes: ['badge.review'], notFollowed: true },
};

/* ───────────── the cost meter: documents and bytes the database is asked for, per call, attributed to the op that read them ───────────── */
function meter(srv) {
  const als = new AsyncLocalStorage(), tally = { ops: {}, total: { reads: 0, bytes: 0, writes: 0 }, log: [] };
  const size = d => { try { return JSON.stringify(d).length; } catch (_) { return 0; } };
  const note = (reads, bytes) => { const s = als.getStore(); if (!s) return; s.reads += reads; s.bytes += bytes; };
  const wrap = obj => new Proxy(obj, { get(t, k) {
    const v = t[k]; if (typeof v !== 'function') return v;
    if (k === 'get') return async (...a) => { const r = await v.apply(t, a); if (r && r.docs) { for (const d of r.docs) note(1, size(d.data())); } else if (r && r.exists !== undefined) note(1, r.exists ? size(r.data()) : 0); return r; };
    return (...a) => { const r = v.apply(t, a); return r && typeof r === 'object' && typeof r.then !== 'function' ? wrap(r) : r; };
  } });
  const db = srv.st.db, collection = db.collection, getAll = db.getAll, tx = db.runTransaction;
  db.collection = c => wrap(collection.call(db, c));
  if (getAll) db.getAll = async (...refs) => { const r = await getAll.apply(db, refs); for (const s of r) if (s && s.exists !== undefined) note(1, s.exists ? size(s.data()) : 0); return r; };
  const h = srv.st.handlers.charmNestLibrary, handler = h.handler;
  h.handler = (ev, ...rest) => {
    let op = ''; try { op = JSON.parse(ev.body || '{}').op || ''; } catch (_) {}
    const s = { op, reads: 0, bytes: 0, t0: Date.now() };
    return als.run(s, async () => { try { return await handler.call(h, ev, ...rest); } finally { const o = tally.ops[op] || (tally.ops[op] = { calls: 0, reads: 0, bytes: 0, ms: 0 }); o.calls++; o.reads += s.reads; o.bytes += s.bytes; o.ms += Date.now() - s.t0; tally.total.reads += s.reads; tally.total.bytes += s.bytes; tally.log.push({ at: s.t0, op, reads: s.reads, bytes: s.bytes, ms: Date.now() - s.t0 }); } });
  };
  tally.reset = () => { tally.ops = {}; tally.total = { reads: 0, bytes: 0 }; tally.log = []; };
  tally.restore = () => { h.handler = handler; db.collection = collection; };
  return tally;
}

/* ───────────── the pages ───────────── */
async function seedPage(page, shop) {
  await page.evaluate(async ({ orders, skus }) => {
    await Orders.loadMaps(true);
    for (const s of skus) B.master.entries.set(s, { sku: s, updatedAt: 1 });
    B.orders.rows = []; B.orders.byKey = new Map();
    const at = Date.now();
    for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: at, spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
    Orders.interpretAll(); Review.syncOrderItems(); Orders.render(); CN.setMode('nest'); LiveStrip.now();
  }, { orders: shop.orders, skus: SHOP.SKUS });
}
/** put a page where its role says, over the subject order */
async function stage(page, role, subj) {
  const R = ROLES[role];
  await page.evaluate(async () => { try { OrderWin.isOpen() && OrderWin.close(); OrderSearch.isOpen && OrderSearch.isOpen() && OrderSearch.close(); await Seal.whenIdle(); } catch (_) {} });
  await page.evaluate(m => { CN.setMode(m); }, R.mode);
  if (R.mode === 'review') await page.evaluate(() => { const v = Review.view(); v.cseg = 'open'; v.limit = v.keep = 400; Review.render(); });
  if (R.find) await page.evaluate(rid => { const v = Orders.view(); v.q = rid; Orders.render(); }, subj.rid);
  if (R.win) {
    await page.evaluate(k => OrderWin.open(k), subj.chainKey);
    await page.waitForSelector('#owPcSum .owPcRow', { timeout: 30000 });
    if (R.win === 'sheet') { await page.evaluate(() => OrderWin.setView('sheet')); await page.waitForSelector('#owSheetPanel', { timeout: 15000 }); }
  }
  if (R.search) { await page.evaluate(rid => OrderSearch.open(rid), subj.rid); await page.waitForSelector('.cnsCard', { timeout: 15000 }); }
}
const ARM = ({ defs, K, R }) => {
  const q = s => document.querySelector(s), txt = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '';
  const P = window.__P = { hits: {}, base: {}, last: {}, K, R };
  const fns = defs.map(d => ({ id: d.id, base: new Function('q', 'txt', 'K', 'R', d.base), done: new Function('q', 'txt', 'K', 'R', 'b', d.done) }));
  for (const f of fns) { try { P.base[f.id] = f.base(q, txt, K, R); } catch (e) { P.base[f.id] = null; } }
  P.again = () => Object.fromEntries(fns.map(f => { try { return [f.id, f.base(q, txt, K, R)]; } catch (e) { return [f.id, String(e)]; } }));
  clearInterval(window.__Pi);
  window.__Pi = setInterval(() => { for (const f of fns) { if (P.hits[f.id]) continue; try { if (f.done(q, txt, K, R, P.base[f.id])) P.hits[f.id] = Date.now(); } catch (_) {} } }, 50);
  return P.base;
};
const READ = () => ({ hits: window.__P.hits, base: window.__P.base, printed: window.__printed || 0, now: Date.now(), again: window.__P.again(), heard: (window.ReviewLive && ReviewLive.state && ReviewLive.state().heard) || 0 });

/** One run: a fresh backend and the pages of ONE role in the three cases (the press page and a tab of its computer, and a page of another computer), one press.
 *  Few pages at a time on purpose: a dozen pages on a machine with four cores measure the machine. */
async function run(browser, action, role, activityWait) {
  const srv = await O.backend(), shop = SHOP.buildShop(), subj = shop.subjects[action];
  const calls = [], activity = [], writeAt = [];
  { const push = srv.st.calls.push.bind(srv.st.calls); srv.st.calls.push = c => { c.at = Date.now(); calls.push(c); return push(c); }; }
  for (const [c, id, d] of SHOP.cloudDocs(shop)) srv.raw.set(c + '/' + id, d);
  { const set = srv.st.docs.set; srv.st.docs.set = (k, v) => { if (k === `Charm_Custom_Orders/${subj.chainKey}`) writeAt.push(Date.now()); return set.call(srv.st.docs, k, v); }; }
  const M = meter(srv), errors = [], ctxs = [];
  try {
    const open = async (cn, ctx, rl) => {
      const p = await O.openPage(browser, srv, { owner: false, name: `${cn}.${rl}`, ctx, motion: MOTION, employee: cn === 'A' ? 'Paul' : 'Maria', latency: LATENCY });
      if (!ctx) {
        ctx = p.ctx; ctxs.push(ctx);
        await ctx.route(/pdfmake@[^/]+\/build\/pdfmake/, r => r.fulfill(js(PDFMAKE))); await ctx.route(/pdfmake@[^/]+\/build\/vfs_fonts/, r => r.fulfill(js('')));
        // the stations' door: Paul is the Admin (the sorter's own station, no role question); the efficiency feed's posts are answered and written down with the time they arrive
        await ctx.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async r => {
          let j = null; try { j = JSON.parse(r.request().postData() || 'null'); } catch (_) {}
          const ok = body => r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
          if (r.request().method() === 'POST' && j && typeof j.stationAdmin === 'string') return ok({ ok: true, admin: /^paul$/i.test(j.stationAdmin) });
          if (r.request().method() === 'POST' && j && (Array.isArray(j.activity) || j.session || j.live)) { activity.push({ at: Date.now(), computer: cn, activity: j.activity || null }); return ok({ success: true, written: 1 }); }
          return r.fallback();
        });
      }
      p.page.on('pageerror', e => errors.push(`${cn}.${rl}: ${e.message}`));
      await seedPage(p.page, shop);
      return p;
    };
    const press = await open('A', null, 'press');
    // Paul is signed in at the sorter (the name bar, the Admin door answers yes): what he does is recorded for the Employee efficiency board, as in the shop
    await press.page.evaluate(() => CNEmployee.edit({})); await press.page.waitForSelector('.cnNameBar[data-kind="edit"] input', { timeout: 20000 });
    await press.page.fill('.cnNameBar input', 'Paul'); await press.page.press('.cnNameBar input', 'Enter');
    await press.page.waitForFunction(() => window.CNRole && CNRole.state() === 'admin' && StationActivity.who(), null, { timeout: 30000 });
    const targets = [{ id: 'A.press', page: press.page, role: 'press', case: 'same tab' }];
    if (role !== 'press') {
      const tab = await open('A', press.ctx, role), far = await open('B', null, role);
      targets.push({ id: `A.${role}`, page: tab.page, role, case: 'other tab' }, { id: `B.${role}`, page: far.page, role, case: 'other computer' });
    }
    for (const t of targets) await stage(t.page, t.role, subj);
    await sleep(3500);   // (every page has its first reads behind it)
    for (const t of targets) t.base = await t.page.evaluate(ARM, { defs: ROLES[t.role].probes.map(pr => Object.assign({ id: pr }, PROBES[pr])), K: subj.chainKey, R: subj.rid });
    M.reset(); const calls0 = calls.length, load = require('os').loadavg()[0];
    const tp = await press.page.evaluate(({ k, sel }) => { const row = document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"]`), b = row && row.querySelector(sel); if (!b) return 0; const t = Date.now(); b.click(); return t; }, { k: subj.chainKey, sel: action === 'complete' ? '[data-cu-complete]' : '[data-cu-print]' });
    if (!tp) throw new Error(`the ${action} button is not on the piece's row`);
    const deadline = Date.now() + 12000; let all = {};
    for (;;) {
      await sleep(250); all = {}; let missing = 0;
      for (const t of targets) { const r = await t.page.evaluate(READ); all[t.id] = r; for (const pr of ROLES[t.role].probes) if (!r.hits[pr]) missing++; }
      if (!missing || Date.now() > deadline) break;
    }
    const out = [];
    // time zero: Complete Order is the press; a QR print is the moment its label has printed and the order is written (the print dialog's own time is the person's: the harness's stub closes it at once, a real one does not)
    const t0 = action === 'print' && writeAt[0] ? writeAt[0] : tp;
    for (const t of targets) for (const pr of ROLES[t.role].probes) {
      const hit = all[t.id].hits[pr];
      out.push({ action, role: t.role, probe: pr, case: t.case, ms: hit ? hit - t0 : null, fromPress: hit ? hit - tp : null, was: all[t.id].base[pr], now: hit ? null : all[t.id].again[pr] });
      if (DUMP) console.log(`     ${action} ${t.id} ${pr}: ${hit ? ((hit - t0) / 1000).toFixed(2) + ' s' : 'NEVER'}${hit ? '' : '   now ' + JSON.stringify(all[t.id].again[pr]).slice(-300)}`);
    }
    const my = calls.slice(calls0), cost = JSON.parse(JSON.stringify(M.ops));
    const writer = { cloudWrite: writeAt[0] ? writeAt[0] - tp : null, timeline: (c => c ? c.at - tp : null)(my.find(c => c.op === 'timelineAdd' && (c.body.events || []).some(e => String(e.orderId) === subj.rid))) };
    // the efficiency feed sends in batches (every 10 s): wait for it, on the first run of a press
    for (const t1 = Date.now(); Date.now() - t1 < activityWait && !activity.some(x => x.activity && x.at >= tp);) await sleep(250);
    const act = activity.filter(x => x.activity && x.at >= tp); writer.activity = act.length ? act[0].at - tp : null; writer.kinds = act.flatMap(x => x.activity.map(a => `${a.action}@${x.at - tp}ms`));
    // a quiet stretch: what these pages ask of the database while nothing happens
    await sleep(2500); M.reset(); const t1 = Date.now(); await sleep(IDLE * 1000);
    const idle = { seconds: (Date.now() - t1) / 1000, pages: targets.length, ops: JSON.parse(JSON.stringify(M.ops)) };
    const etsy = srv.st.calls.filter(c => ['listOpenOrders', 'etsyOrderProxy', 'etsyImages', 'refreshEtsyToken', 'etsySandbox'].includes(c.name)).length;
    const tabT = targets.find(t => t.case === 'other tab');
    return { out, writer, cost, idle, load, errors, etsy, paid: srv.paid, heard: tabT ? all[tabT.id].heard : null };
  } finally { M.restore(); for (const c of ctxs) await c.close().catch(() => {}); srv.close(); }
}

async function main() {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: not run'); return; }
  const exe = process.env.CHROMIUM || (fs.existsSync('/opt/pw-browsers/chromium-1194/chrome-linux/chrome') ? '/opt/pw-browsers/chromium-1194/chrome-linux/chrome' : undefined);
  const browser = await chromium.launch(Object.assign({ args: ['--no-sandbox'] }, exe ? { executablePath: exe } : {}));
  const t00 = Date.now(), log = (...a) => console.log(((Date.now() - t00) / 1000).toFixed(1).padStart(6) + 's', ...a);
  const runs = [];
  try {
    const roles = (opt('roles', process.env.RT_ROLES || '') || Object.keys(ROLES).filter(r => r !== 'press').join(',')).split(',');
    for (const action of ['complete', 'print']) {
      if (ONLY && ONLY !== action) continue;
      for (const role of roles) {
        const r = await run(browser, action, role, runs.some(x => x.action === action) ? 0 : 13000); r.action = action; r.role = role; runs.push(r);
        log(`${action} · ${role}: ${r.out.map(x => `${x.probe}/${x.case.split(' ')[1] || x.case}=${x.ms == null ? '>12' : (x.ms / 1000).toFixed(1)}`).join(' ')}   (load ${r.load.toFixed(1)})`);
      }
    }
    report(runs, SHOP.buildShop());
    if (opt('json', '')) fs.writeFileSync(opt('json', ''), JSON.stringify(runs.map(r => ({ action: r.action, role: r.role, out: r.out.map(({ was, now, ...x }) => x), writer: r.writer, idle: r.idle, cost: r.cost, load: r.load, heard: r.heard })), null, 1));
  } finally { await browser.close(); }
}

function report(runs, shop) {
  const all = runs.flatMap(r => r.out), fmt = v => v == null ? '>12' : (v / 1000).toFixed(1), key = v => v == null ? 1e9 : v, med = a => a.slice().sort((x, y) => key(x) - key(y))[Math.floor(a.length / 2)];
  console.log(`\n══ Complete / QR Print: seconds from the press to each surface showing it ══   (large shop: ${shop.counts.orders} orders, ${shop.counts.review} Review, ${shop.counts.sheets} sheets; ${LATENCY} ms per call; ${MOTION ? 'real motion' : 'reduced motion'}; machine load at the presses ${runs.map(r => r.load.toFixed(0)).join('/')})`);
  let bad = 0;
  const cell = (rows, lim, by) => { if (!rows.length) return '    -'; const v = rows.map(x => x.ms), m = med(v), ok = m != null && m <= lim, w = Math.max(...v.map(key)); if (!ok && !by) bad++; return fmt(m) + (v.length > 1 ? ` (${fmt(w >= 1e9 ? null : w)})` : '') + (by ? ' ·' : ok ? ' ' : '*'); };
  console.log('  surface (where it is looked at)'.padEnd(78) + 'press'.padEnd(10) + 'seconds'.padStart(12));
  // the page that pressed (same tab): the middle of the runs
  for (const action of ['complete', 'print']) {
    if (ONLY && ONLY !== action) continue;
    for (const pr of ROLES.press.probes) { const rows = all.filter(x => x.role === 'press' && x.probe === pr && x.action === action); if (rows.length) console.log(('  same tab · ' + PROBES[pr].label).padEnd(78) + action.padEnd(10) + cell(rows, SAME).padStart(12)); }
  }
  // the other tab of the computer and the other computer: per role
  for (const action of ['complete', 'print']) {
    if (ONLY && ONLY !== action) continue;
    for (const role of Object.keys(ROLES).filter(r => r !== 'press')) for (const pr of ROLES[role].probes) {
      const t = all.filter(x => x.role === role && x.probe === pr && x.action === action && x.case === 'other tab'), f = all.filter(x => x.role === role && x.probe === pr && x.action === action && x.case === 'other computer');
      if (!t.length && !f.length) continue;
      console.log(('  ' + PROBES[pr].label + ' — on ' + ROLES[role].where).padEnd(78) + action.padEnd(10) + ('tab ' + cell(t, SAME)).padStart(12) + ('  computer ' + cell(f, FAR, ROLES[role].notFollowed)).padStart(18));
    }
  }
  console.log(`  (the middle number of the runs, the slowest in brackets; * = slower than the target: ${SAME / 1000} s on the same computer, ${FAR / 1000} s on another; · = not read there by design: no page on that tab asks the cloud for changes, which would cost a read every 2 s)`);
  for (const r of runs.filter(r => r.writer.activity != null || r.writer.cloudWrite != null).slice(0, 4)) console.log(`  writer (${r.action}, ${r.role}): cloud write ${r.writer.cloudWrite} ms after the press · order timeline write ${r.writer.timeline} ms · efficiency activity ${r.writer.activity == null ? 'not seen' : r.writer.activity + ' ms'} ${r.writer.kinds.join(' ')}`);
  console.log('\n══ what the database is asked ══');
  const tot = {}, idleTot = {};
  for (const r of runs) for (const [op, o] of Object.entries(r.idle.ops)) { const t = idleTot[op] || (idleTot[op] = { calls: 0, reads: 0, bytes: 0 }); t.calls += o.calls; t.reads += o.reads; t.bytes += o.bytes; }
  const secs = runs.reduce((s, r) => s + r.idle.seconds * r.idle.pages, 0), pageHours = secs / 3600;
  const reads = Object.values(idleTot).reduce((s, o) => s + o.reads, 0), bytes = Object.values(idleTot).reduce((s, o) => s + o.bytes, 0);
  console.log(`  idle, ${runs.length} runs of ${runs[0].idle.pages} pages (${secs.toFixed(0)} page-seconds): ${Math.round(reads / pageHours)} documents and ${(bytes / pageHours / 1048576).toFixed(2)} MB per page-hour`);
  for (const [op, o] of Object.entries(idleTot).sort((a, b) => b[1].bytes - a[1].bytes)) console.log(`    ${(op || '?').padEnd(16)} ${(o.calls / pageHours).toFixed(0).padStart(5)} calls/page-hour · ${(o.reads / o.calls).toFixed(1).padStart(5)} docs and ${(o.bytes / o.calls / 1024).toFixed(1).padStart(6)} KB per call · ${Math.round(o.reads / pageHours).toString().padStart(6)} docs and ${(o.bytes / pageHours / 1048576).toFixed(2).padStart(6)} MB per page-hour`);
  for (const r of runs) for (const [op, o] of Object.entries(r.cost)) { const t = tot[op] || (tot[op] = { calls: 0, reads: 0, bytes: 0 }); t.calls += o.calls; t.reads += o.reads; t.bytes += o.bytes; }
  console.log('  around the presses (all runs): ' + Object.entries(tot).sort((a, b) => b[1].bytes - a[1].bytes).slice(0, 6).map(([op, o]) => `${op || '?'} x${o.calls}: ${o.reads} docs, ${(o.bytes / 1024).toFixed(0)} KB`).join(' · '));
  const told = runs.filter(r => r.heard != null), silent = told.filter(r => !(r.heard >= 1));
  console.log(`\n  other tabs of the pressing computer told of the press by the BroadcastChannel (no read): ${told.length - silent.length} of ${told.length}${silent.length ? '  — not told: ' + silent.map(r => r.action + '/' + r.role).join(', ') : ''}`);
  if (silent.length) bad += silent.length;
  const etsy = runs.reduce((n, r) => n + r.etsy, 0), paid = runs.reduce((n, r) => n + r.paid, 0), errors = runs.flatMap(r => r.errors);
  console.log(`\n  guards: ${etsy} Etsy calls · ${paid} paid calls · ${errors.length} page errors`); for (const e of errors.slice(0, 5)) console.log('      ' + e);
  if (bad || etsy || paid) { console.log(`\n${bad} cell(s) slower than the target`); process.exitCode = 1; } else console.log('\nevery surface follows within the targets');
}
if (require.main === module) main().catch(e => { console.error(e); process.exit(2); });
module.exports = { PROBES, ROLES, main };
