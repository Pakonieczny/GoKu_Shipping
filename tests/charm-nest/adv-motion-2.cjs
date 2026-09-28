// Motion leftovers (Paul, 28 Sep: animations "were not exactly visible and they were jerky"), measured in a real
// Chromium, unthrottled, with requestAnimationFrame deltas, long tasks and every element.animate() call recorded:
// 1 · Nest tab, a file removed (the real code: a real .ai dropped, one charm sent to Gold, then the file's ×): its row
//     folds away while the cards it touched wait to redraw until the fold is done (they redrew under it, and their
//     pictures stalled it with long tasks up to ~94 ms); the cards are redrawn no later than just after the fold.
// 2 · The Custom designs window (the real code, on the fake site): a design removed folds its row and the "Same metal
//     for all" bar folds; a design added opens its room and the bar opens. Only transform, opacity and clip-path are
//     animated (their height was, a layout per frame); what is under them is seen gliding, and lands where it belongs.
// 3 · The seal's wooden stamp (Seal.stampOn): no filter animated per frame (its drop-shadow was); its shadow is seen.
// With BEFORE=<a checkout of the code before> the same scenes run first on that code, in this same run, for a
// before/after table. The timing gates are hard only when the machine is quiet (load per core < 1.2).
//   node tests/charm-nest/adv-motion-2.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { build } = require('./fixture.cjs');
const LAYOUT = /^(top|left|right|bottom|width|height|maxHeight|minHeight|maxWidth|minWidth|margin.*|padding.*|border.*Width|inset|flex.*|gap)$/;
const PAINT = /^(boxShadow|filter|backdropFilter|background.*|color|borderColor|outline.*)$/;
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.ai': 'application/pdf' };
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const ORDERS = [{ receiptId: '4175423829', orderNumber: '4175423829', createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 3829' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41754238291', listingId: '1800038291', sku: 'CUSTOM-N-001-665441', title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] }];

// recorders, in the page before its scripts: frames, long tasks, animate() calls, and named rects sampled per frame
function recorders() {
  const M = window.__M = { rec: false };
  const A = Element.prototype.animate;
  Element.prototype.animate = function (k, o) {
    if (M.rec) {
      const props = new Set(); for (const f of Array.isArray(k) ? k : k ? [k] : []) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p);
      const cls = typeof this.className === 'string' ? this.className : (this.getAttribute && this.getAttribute('class')) || this.tagName;
      M.anims.push({ cls: String(cls).slice(0, 30), props: [...props], dlg: !!(this.closest && this.closest('#cuDlg')), t: performance.now() - M.t0 });
    }
    return A.call(this, k, o);
  };
  try { new PerformanceObserver(l => { if (M.rec) for (const e of l.getEntries()) M.long.push([Math.round(e.startTime - M.t0), Math.round(e.duration)]); }).observe({ entryTypes: ['longtask'] }); } catch (_) {}
  const loop = t => { if (M.rec) { M.frames.push(t - M.t0); for (const [name, fn] of M.watch) { let v = null; try { v = fn(); } catch (_) {} (M.seen[name] = M.seen[name] || []).push([Math.round(t - M.t0), v]); } } requestAnimationFrame(loop); };
  requestAnimationFrame(loop);
  window.__start = () => Object.assign(M, { rec: true, frames: [], long: [], anims: [], watch: [], seen: {}, marks: [], t0: performance.now() });
  window.__mark = name => { if (M.rec) M.marks.push([name, Math.round(performance.now() - M.t0)]); };
  window.__stop = () => { M.rec = false; const d = []; for (let i = 2; i < M.frames.length; i++) d.push([Math.round(M.frames[i]), M.frames[i] - M.frames[i - 1]]); return { d, long: M.long, anims: M.anims, seen: M.seen, marks: M.marks }; };
}
const judge = (r, from = 0, to = 1e9) => {
  const d = r.d.filter(([t]) => t >= from && t <= to);
  return { frames: d.length, worst: Math.round(Math.max(0, ...d.map(x => x[1]))), over34: d.filter(x => x[1] > 34).length, long: r.long.filter(([s, ms]) => ms > 50 && s + ms > from && s < to).map(x => x[1]) };
};
const propsOf = (r, pick = () => true) => { const a = r.anims.filter(pick); return { layout: [...new Set(a.flatMap(x => x.props.filter(p => LAYOUT.test(p)).map(p => x.cls.split(' ')[0] + ':' + p)))], paint: [...new Set(a.flatMap(x => x.props.filter(p => PAINT.test(p)).map(p => x.cls.split(' ')[0] + ':' + p)))], all: [...new Set(a.flatMap(x => x.props))] }; };
/** positions a watched rect took (distinct, rounded): a glide shows several between its start and its end */
const glide = s => { const v = (s || []).map(x => x[1]).filter(x => x != null); const u = [...new Set(v.map(Math.round))]; let jump = 0; for (let i = 1; i < v.length; i++) jump = Math.max(jump, Math.abs(v[i] - v[i - 1])); return { from: Math.round(v[0]), to: Math.round(v[v.length - 1]), steps: u.length, jump: Math.round(jump) }; };

async function scenes(root, label, browser) {
  const out = {};
  /* ── 1 · Nest tab: a file removed ── */
  {
    const server = http.createServer((req, res) => {
      const u = decodeURIComponent(req.url.split('?')[0]);
      if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"none"}'); }
      const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
      if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
      res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp' });
      fs.createReadStream(f).pipe(res);
    }).listen(0);
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-motion2-')), fixture = path.join(tmp, 'TEST-charms.ai');
    await build(fixture, 8);
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    try {
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
      await context.addInitScript(recorders);
      const page = await context.newPage(), errors = []; page.on('pageerror', e => errors.push(String(e)));
      await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
      await page.waitForFunction(() => window.CN && window.CN.S && window.Motion && window.Motion.reconcile);
      await page.evaluate(() => { CN.S.settings.naming = 'off'; CN.S.settings.review = 'off'; });
      await page.setInputFiles('#fileInput', fixture);
      await page.waitForFunction(() => CN.S.sources.length === 1 && ['ready', 'error'].includes(CN.S.sources[0].state) && CN.S.unassigned.length >= 6, null, { timeout: 60000 });
      await page.evaluate(() => CN.setMode('nest'));
      await page.waitForFunction(() => document.querySelector('#unList .unRow') && document.querySelector('#unList .unRow').getBoundingClientRect().height > 0, null, { timeout: 10000 });
      await page.waitForTimeout(800);
      // one charm to Gold (its card now holds a charm of the file), as a person does before thinking better of the file;
      // not nested yet (a nest running draws its sheet live, and a charm placed keeps its file)
      await page.evaluate(() => { CN.S.settings.autoNest = 'off'; document.querySelector('#unList .unRow button[data-m="gold"]').click(); });
      await page.waitForTimeout(2600);
      // the card redraws (renderCard, one held back for later is not one) and its picture (drawPreviewNow), timed from
      // the click; the offline cloud's retry (a test server without functions) is stopped, so it cannot redraw them
      await page.evaluate(() => { window.cloudProbe = async () => {}; for (const f of ['renderCard', 'drawPreviewNow']) { const o = window[f]; window[f] = function () { if (!window.cardHold) __mark(f); return o.apply(this, arguments); }; } });
      const r = await page.evaluate(async () => {
        __start(); const q = () => [...document.querySelectorAll('[data-r="queue"] img[data-cid]')].filter(i => i.style.visibility !== 'hidden').length;
        __M.watch.push(['queue', q], ['ghosts', () => document.querySelectorAll('#motionLayer .mGhost').length]);
        document.querySelector('#srcList .srcRow .srcX').click(); __mark('clicked');
        await new Promise(res => setTimeout(res, 1800)); return __stop();
      });
      const fold = await page.evaluate(() => Motion.T.fade);
      const after = await page.evaluate(() => ({ sources: CN.S.sources.length, unassigned: CN.S.unassigned.length, queue: document.querySelectorAll('.sheetCard[data-m="gold"] [data-r="queue"] img[data-cid]').length, gold: allSheets().filter(s => s.metal === 'gold').reduce((n, s) => n + s.charms.length, 0) }));
      const card = r.marks.filter(m => m[0] === 'renderCard').map(m => m[1]), pic = r.marks.filter(m => m[0] === 'drawPreviewNow').map(m => m[1]);
      out.remove = { label: 'Nest · file removed', fold, during: judge(r, 0, fold + 20), whole: judge(r), card, pic, after, errors, queue: r.seen.queue, ghosts: r.seen.ghosts };
    } finally { await context.close(); server.close(); }
  }
  /* ── 2 · the Custom designs window, and 3 · the seal's stamp ── */
  {
    const { start } = require(path.join(root, 'tests/charm-nest/bridge-server.cjs'));
    const srv = await start({ receipts: [] });
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    try {
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
      await context.addInitScript(recorders);
      const page = await context.newPage(), errors = []; page.setDefaultTimeout(20000);
      page.on('pageerror', e => errors.push(String(e)));
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async orders => {
        await Orders.loadMaps(true);
        for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      }, ORDERS);
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = '#rvList .reviewListRow[data-rid="4175423829"]';
      await page.waitForSelector(card);
      const drop = (sel, files) => page.evaluate(({ sel, files }) => { const dt = new DataTransfer(); for (const f of files) dt.items.add(new File([f.text], f.name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel, files });
      await drop(card, [{ name: 'heart-a.dxf', text: DXF(18) }, { name: 'heart-b.dxf', text: DXF(14) }]);
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 2 && !document.querySelector('#cuDlg .cuAll').hidden, null, { timeout: 30000 });
      await page.waitForTimeout(1500);
      const foot = () => { const f = document.querySelector('#cuDlg .dlgFoot'); return f ? f.getBoundingClientRect().top : null; };
      // a design removed: its row folds away, the bar folds, the rows and the foot rise
      const rm = await page.evaluate(async src => {
        __start(); const foot = (0, eval)(src), rows = () => document.querySelectorAll('#cuDlg .cuFile').length;
        const gone = document.querySelector('#cuDlg .cuFile'), keep = document.querySelectorAll('#cuDlg .cuFile')[1];
        __M.watch.push(['foot', foot], ['keep', () => keep.getBoundingClientRect().top], ['goneOpacity', () => gone.isConnected ? +getComputedStyle(gone).opacity : null], ['rows', rows], ['dlgBottom', () => document.querySelector('#cuDlg').getBoundingClientRect().bottom]);
        gone.querySelector('[data-rm]').click();
        await new Promise(res => setTimeout(res, 1100)); const r = __stop();
        r.end = { rows: rows(), bar: document.querySelector('#cuDlg .cuAll').hidden, foot: foot(), keepTop: keep.getBoundingClientRect().top, keepTransform: getComputedStyle(keep).transform, anims: document.getAnimations().filter(a => a.effect && a.effect.target && a.effect.target.closest && a.effect.target.closest('#cuDlg') && a.playState !== 'finished').length };
        return r;
      }, String(foot));
      await page.waitForTimeout(400);
      // a design added: its row opens its room, the bar opens, the foot glides down
      await page.evaluate(src => { __start(); const foot = (0, eval)(src); __M.watch.push(['foot', foot]); window.__addFoot = foot; }, String(foot));
      await drop('#cuDlg', [{ name: 'heart-c.dxf', text: DXF(16) }]);
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg .cuFile .cuThumb img').length === 2 && !document.querySelector('#cuDlg .cuAll').hidden, null, { timeout: 30000 });
      await page.waitForTimeout(1300);
      const ad = await page.evaluate(() => { const r = __stop(); r.end = { foot: __addFoot(), bar: !document.querySelector('#cuDlg .cuAll').hidden }; return r; });
      const inDlg = a => a.dlg;
      out.cuRemove = { label: 'Custom window · design removed', props: propsOf(rm, inDlg), whole: judge(rm), foot: glide(rm.seen.foot), keep: glide(rm.seen.keep), gone: (rm.seen.goneOpacity || []).map(x => x[1]).filter(x => x != null), end: rm.end, dlgBottom: glide(rm.seen.dlgBottom) };
      out.cuAdd = { label: 'Custom window · design added', props: propsOf(ad, inDlg), whole: judge(ad), foot: glide(ad.seen.foot), end: ad.end };
      // 3 · the seal's stamp pressed on a button
      const st = await page.evaluate(async () => {
        const host = document.createElement('div'); host.style.cssText = 'position:fixed;left:420px;top:260px;width:520px;height:240px;background:#fff;border:1px solid #ddd;border-radius:12px;z-index:70';
        host.innerHTML = '<button class="btn gold sm" style="position:absolute;left:180px;top:100px">Print QR label</button>'; document.body.appendChild(host);
        __start(); __M.watch.push(['tool', () => { const t = host.querySelector('.sealTool'); if (!t) return null; const r = t.getBoundingClientRect(); return r.left + r.top + r.width; }], ['shade', () => { const s = host.querySelectorAll('.sealShade'); return s.length ? [...s].map(x => +getComputedStyle(x).opacity).join('/') : null; }]);
        await Seal.stampOn(host, { btn: host.querySelector('button'), stamp: { how: 'print', at: Date.now(), by: 'Test Operator', n: 1 } });
        await new Promise(res => setTimeout(res, 200)); const r = __stop(); r.left = host.querySelectorAll('.sealTool, .sealShade').length; r.seal = !!host.querySelector('.seal'); host.remove(); return r;
      });
      const shades = (st.seen.shade || []).map(x => x[1]).filter(Boolean);
      out.seal = { label: 'Seal · the stamp comes down and lifts', props: propsOf(st), whole: judge(st), tool: glide(st.seen.tool), shades: [...new Set(shades)].length, left: st.left, seal: st.seal };
      out.errors = errors;
    } finally { await context.close(); await srv.close(); }
  }
  return out;
}

(async () => {
  const load = os.loadavg()[0] / os.cpus().length, quiet = load < 1.2;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], check = (ok, what, hard = true) => { if (!ok && hard) fails.push(what); console.log((ok ? '  ok   ' : hard ? '  FAIL ' : '  warn ') + what); };
  let before = null, now;
  try {
    if (process.env.BEFORE) before = await scenes(path.resolve(process.env.BEFORE), 'before', browser);
    now = await scenes(here, 'after', browser);
  } finally { await browser.close(); }
  const row = (x, k) => x ? `worst ${String(x[k].worst).padStart(3)} ms, >34 ms ${x[k].over34}/${x[k].frames}, long ${JSON.stringify(x[k].long)}` : '';
  console.log(`\n  load per core ${load.toFixed(2)} (${quiet ? 'quiet: timing gates are hard' : 'busy: timing gates only warn'})`);
  console.log('\n  before / after' + (before ? '' : ' (no BEFORE given: after only)'));
  const rmB = before && before.remove, rmA = now.remove;
  for (const [name, b, a] of [['Nest · file removed, during the fold', rmB, rmA, 'during'], ['Nest · file removed, 1.8 s', rmB, rmA]].map(x => x)) {
    const k = /fold/.test(name) ? 'during' : 'whole';
    if (b) console.log(`   ${name.padEnd(42)} before: ${row(b, k)}\n   ${''.padEnd(42)} after:  ${row(a, k)}`); else console.log(`   ${name.padEnd(42)} after:  ${row(a, k)}`);
  }
  for (const [x, y] of [[rmB, 'before'], [rmA, 'after']]) if (x) console.log(`   Nest · card redraws (ms from the click) ${y}: renderCard ${JSON.stringify(x.card)}, picture ${JSON.stringify(x.pic.map(Math.round))}`);
  for (const k of ['cuRemove', 'cuAdd', 'seal']) {
    for (const [x, y] of [[before && before[k], 'before'], [now[k], 'after']]) if (x) console.log(`   ${x.label.padEnd(42)} ${y.padEnd(6)} ${row(x, 'whole')}; layout ${JSON.stringify(x.props.layout)}, paint ${JSON.stringify(x.props.paint)}; props ${x.props.all.join(',')}`);
  }
  console.log('');
  const r = now.remove, fold = r.fold;
  check(r.after.sources === 0 && r.after.unassigned === 0 && r.after.gold === 0 && r.after.queue === 0, `Nest: the file, its charms and the Gold charm are gone, and the Gold card shows it (${JSON.stringify(r.after)})`);
  check(!r.card.some(t => t < fold) && !r.pic.some(t => t < fold), `Nest: no card or picture redrawn while the row folds (${fold} ms): renderCard ${JSON.stringify(r.card)}, picture ${JSON.stringify(r.pic.map(Math.round))}`);
  check(r.card.length > 0 && Math.max(...r.card) <= fold + 250, `Nest: the cards are redrawn just after the fold, not later (last at ${Math.max(...r.card)} ms)`);
  const q = (r.queue || []).map(x => x[1]), gh = (r.ghosts || []).map(x => x[1]);
  check(q.length && q[0] === 0 && Math.max(...gh) > 0, `Nest: the card's thumbnails of the file are seen only as the copies that fade (real ones shown ${q[0]}, copies ${Math.max(...gh)})`);
  // (STRICT=1 on a quiet machine: not one frame over 34 ms; otherwise a frame lost to the shared machine is tolerated,
  // at most one in ten, as adv-tab-motion.cjs does)
  check(!r.during.long.length && (process.env.STRICT ? !r.during.over34 : r.during.over34 <= r.during.frames / 10), `Nest: the fold is smooth (no long task over 50 ms while it runs, frames over 34 ms ${process.env.STRICT ? 'none' : 'at most 1 in 10'}): ${row(r, 'during')}`, quiet);
  for (const k of ['cuRemove', 'cuAdd']) { const x = now[k]; check(!x.props.layout.length, `${x.label}: no layout property animated (${x.props.layout.join(' ') || 'none'})`); check(!x.props.paint.length, `${x.label}: no filter or shadow animated (${x.props.paint.join(' ') || 'none'})`); check(!x.whole.long.length && x.whole.over34 <= x.whole.frames / 10, `${x.label}: smooth: ${row(x, 'whole')}`, quiet); }
  const cr = now.cuRemove;
  check(cr.gone.some(o => o > .05 && o < .95), `Custom window: the removed row is seen fading (opacity ${cr.gone.slice(0, 12).map(o => o.toFixed(2)).join(',')}…)`);
  check(cr.foot.steps >= 6 && cr.foot.to < cr.foot.from && cr.keep.steps >= 6, `Custom window: the row under it and the foot glide up (foot ${JSON.stringify(cr.foot)}, row ${JSON.stringify(cr.keep)})`);
  check(cr.end.rows === 1 && cr.end.bar === true && cr.end.keepTransform === 'none' && Math.abs(cr.end.foot - cr.foot.to) < 1, `Custom window: at the end the row is gone, the bar hidden, nothing left lifted, the foot where it belongs (${JSON.stringify(cr.end)})`);
  const ca = now.cuAdd;
  check(ca.foot.steps >= 6 && ca.foot.to > ca.foot.from && ca.end.bar, `Custom window: a design added: the foot glides down to its new place, the bar is back (${JSON.stringify(ca.foot)})`);
  const s = now.seal;
  check(!s.props.paint.length && !s.props.layout.length, `Seal: the stamp animates no filter or layout property (${s.props.paint.concat(s.props.layout).join(' ') || 'none'})`);
  check(s.tool.steps >= 10 && s.shades >= 6 && s.left === 0 && s.seal, `Seal: the stamp is seen moving (${s.tool.steps} positions), its shadow changing (${s.shades} states), all gone after, the seal left`);
  check(!s.whole.long.length && s.whole.over34 <= s.whole.frames / 10, `Seal: smooth: ${row(s, 'whole')}`, quiet);
  check(!now.errors.length && !r.errors.length, 'no page errors: ' + now.errors.concat(r.errors).join(' | '));
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
