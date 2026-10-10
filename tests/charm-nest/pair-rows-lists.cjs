// ROWLISTS (Paul, 10 Oct 2026): "All earring sets (Stud and Huggie Hoop) should be listed as one order with 2 individual thumbnails or the 1 thumbnail we
// currently have with both left and right charm vectors displayed side by side." Every list that shows an order line shows an earring PAIR line (matching or
// mismatched) as ONE row whose picture is the Left and the Right side by side (the Right turned over), by the ONE component charm-nest-pair-thumb.js; singles,
// necklaces, discs and plain quantity-N lines stay exactly as they were. Counts say orders and pieces truthfully. Presentation only: no record is written.
//   node tests/charm-nest/pair-rows-lists.cjs       (the browser part needs PW_DIR=<playwright node_modules> and CHROMIUM=<chrome>; skipped without)
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const Pair = require('../../charm-nest-pair.js');
const PT = require('../../charm-nest-pair-thumb.js');
require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const read = f => fs.readFileSync(path.join(root, f), 'utf8');

// ── fixtures: a one-body earring (an L-shaped boot, so a turned copy is visible), a hoop-welded one, a mismatched pair, three bodies ──
const rect = (x0, y0, x1, y1, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, paintOp: 'S', layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] }, extra || {});
const eng = (x0, y0, x1, y1, rgb) => Object.assign(rect(x0, y0, x1, y1), { layer: 'ENGRAVE', strokeRGB: rgb || [1, 0, 0] });
const boot = x0 => { const o = rect(x0, 0, x0 + 24, 26, { subpaths: [[['m', [x0, 0]], ['l', [x0 + 24, 0]], ['l', [x0 + 24, 10]], ['l', [x0 + 8, 10]], ['l', [x0 + 8, 26]], ['l', [x0, 26]], ['h']]] }); return { o, members: [o, eng(x0 + 2, 2, x0 + 6, 20, [0, 0, 1])] }; };
const bootCharm = (name, extra) => { const b = boot(0); return Object.assign({ id: 'c-' + name, name, sku: name, outline: b.o, members: b.members, bbox: [0, 0, 24, 26], strokePt: .25, centerPt: [12, 13] }, extra || {}); };
const flag = x0 => { const o = rect(x0, 0, x0 + 14, 30, { subpaths: [[['m', [x0, 0]], ['l', [x0 + 14, 0]], ['l', [x0 + 14, 30]], ['l', [x0 + 6, 30]], ['l', [x0 + 6, 12]], ['l', [x0, 12]], ['h']]] }); return { o, members: [o, eng(x0 + 8, 2, x0 + 12, 24, [0, 0, 1])] }; };
const flagCharm = (name, extra) => { const b = flag(0); return Object.assign({ id: 'c-' + name, name, sku: name, outline: b.o, members: b.members, bbox: [0, 0, 14, 30], strokePt: .25, centerPt: [7, 15] }, extra || {}); };
const mitten = (x0, rgb, h) => { const o = rect(x0, 0, x0 + 16, 26, { subpaths: [[['m', [x0 + 4, 0]], ['l', [x0 + 16, 0]], ['l', [x0 + 16, 26]], ['l', [x0 + 4, 26]], ['l', [x0 + 4, 14]], ['l', [x0, 12]], ['l', [x0, 8]], ['l', [x0 + 4, 6]], ['h']]] }); return { o, members: [o, eng(x0 + 7, 3, x0 + 14, 3 + h, rgb)] }; };
const mA = mitten(0, [1, 0, 0], 5), mB = mitten(22, [0, 0, 1], 9);
const mittens = () => ({ id: 'm', name: 'MISMATCHED_7134', outline: mB.o, members: [mB.o, ...mB.members.slice(1), mA.o, ...mA.members.slice(1)], bbox: [0, 0, 38, 26], strokePt: .25, centerPt: [19, 13] });
const twinCharm = () => { const a = boot(0), b = boot(40); return { id: 't', name: 'TWIN', outline: a.o, members: [...a.members, ...b.members], bbox: [0, 0, 64, 26], strokePt: .25, centerPt: [32, 13] }; };
const threeCharm = () => { const a = boot(0), b = rect(30, 0, 50, 26), c = rect(60, 0, 84, 26); return { id: 'th', name: 'THREE', outline: a.o, members: [...a.members, b, eng(32, 2, 48, 6), c, eng(62, 2, 82, 12)], bbox: [0, 0, 84, 26], strokePt: .25, centerPt: [42, 13] }; };

const recorder = () => { const log = []; const ctx = new Proxy({}, { get: (t, k) => k === 'canvas' ? ctx._cv : (k in t ? t[k] : (...a) => { log.push([k, a]); }), set: (t, k, v) => { t[k] = v; log.push(['set:' + k, v]); return true; } }); return { log, make: (w, h) => { const cv = { width: w, height: h, getContext: () => ctx }; ctx._cv = cv; return cv; } }; };
let drew = []; const fakeP = { drawCharm: (ctx, c, tx, k) => drew.push({ c, k, p: tx(0, 0), q: tx(24, 26) }) };
const turns = log => log.filter(l => l[0] === 'scale' && l[1][0] === -1 && l[1][1] === 1).length;
const chipsOf = log => log.filter(l => l[0] === 'fillText').map(l => l[1][0]);

/* ── 1 · the component draws a MATCHING pair as Left and Right side by side ── */
PT.use(Pair);
{
  const mp = PT.matchPlan(bootCharm('PR_BOOT'));
  ok(mp && mp.bodies.length === 2 && mp.bodies[0].side === 'L' && mp.bodies[1].side === 'R' && mp.bodies[0].label === 'Left' && mp.bodies[1].label === 'Right', 'a one-body charm has a two-ear plan, Left then Right');
  ok(mp.bodies[0].bbox[0] === 0 && mp.bodies[1].bbox[0] === mp.dx && mp.dx > 24 && mp.bodies[1].bbox[2] - mp.bodies[1].bbox[0] === 24, 'the Right ear sits to the right of the Left, the same size, with a gap');
  ok(mp.bodies.map(b => b.mirror).join() === 'false,true', 'facing unknown: the Left as drawn, the Right turned over (the rule CharmNestPair.piecesFor follows)');
  ok(PT.matchPlan(bootCharm('PR_BOOT'), { facing: 'R' }).bodies.map(b => b.mirror).join() === 'true,false', 'a person said the drawing faces right: the Left is the turned one');
  ok(PT.matchPlan(bootCharm('PR_BOOT'), { facing: 'L' }).bodies.map(b => b.mirror).join() === 'false,true', 'facing left: the Right is turned');
  ok(PT.matchPlan(bootCharm('PR_BOOT'), { facing: 'X' }).bodies.map(b => b.mirror).join() === 'false,true', 'an old facing X says nothing any more: the Left as drawn and the Right turned over (Paul, 10 Oct 2026)');
  ok(PT.matchPlan(bootCharm('PR_BOOT'), { sku: 'LETTER_A' }).bodies.map(b => b.mirror).join() === 'false,true', 'a design named a letter is an earring like any other: the Right is turned over');
  ok(PT.matchPlan(null) === null && PT.matchPlan({}) === null && PT.matchPlan({ bbox: [0, 0, 5, 5] }) === null && PT.matchPlan(threeCharm()) === null && PT.matchPlan(mittens()) === null, 'nothing to draw, three bodies and a mismatched design have no matching-pair plan');
  const tw = PT.matchPlan(twinCharm());
  ok(tw && tw.bodies.length === 2 && tw.one.bbox[2] - tw.one.bbox[0] === 24, 'two identical bodies drawn in one design: the first is the earring, drawn twice');
}
{
  let r = recorder(); drew = [];
  const cv = PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3 * 72 / 25.4, bg: '#fff', pair: true, makeCanvas: r.make });
  ok(cv && cv.width <= 220 && cv.height <= 220 && cv.width > cv.height * .8, `the picture stays inside the size asked for (${cv && cv.width} x ${cv && cv.height})`);
  ok(drew.length === 2 && drew[0].k === drew[1].k, 'the earring is drawn twice at one scale');
  ok(drew[1].p[0] > drew[0].q[0], 'the second copy starts to the right of where the first ends');
  ok(turns(r.log) === 1, 'exactly one copy is turned over (the Right)');
  ok(chipsOf(r.log).join() === 'Left,Right', 'a Left and a Right chip: ' + chipsOf(r.log));
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3, bg: '#fff', pair: true, facing: 'X', makeCanvas: r.make });
  ok(drew.length === 2 && turns(r.log) === 1, 'an old facing X is drawn twice and the Right is turned, like every pair');
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 90, padPt: 3, bg: '#fff', pair: true, makeCanvas: r.make });
  ok(chipsOf(r.log).join() === 'L,R', 'a small picture says L and R');
  r = recorder(); drew = [];
  ok(PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3, bg: '#fff', makeCanvas: r.make }) === null, 'without the option a plain design is as it was: null, the maker draws it once');
  ok(PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3, bg: '#fff', pair: false, makeCanvas: r.make }) === null, 'pair: false is the same');
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3, bg: '#fff', pair: true, side: 'R', mirror: true, makeCanvas: r.make });
  ok(drew.length === 1 && turns(r.log) === 1, 'one ear asked for by side stays ONE ear (the piece pictures are untouched)');
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, mittens(), { size: 220, padPt: 3, bg: '#fff', pair: true, makeCanvas: r.make });
  const r2 = recorder(); const d1 = drew.length; drew = [];
  PT.canvasFor(fakeP, mittens(), { size: 220, padPt: 3, bg: '#fff', makeCanvas: r2.make });
  ok(d1 === drew.length && chipsOf(r.log).join() === chipsOf(r2.log).join() && r.log.length === r2.log.length, 'a mismatched design is the same picture with or without the option: its own plan runs');
  r = recorder(); drew = [];
  ok(PT.canvasFor(fakeP, threeCharm(), { size: 220, padPt: 3, bg: '#fff', pair: true, makeCanvas: r.make }) === null, 'a design of three bodies is not a pair: the maker draws it as it always did');
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, twinCharm(), { size: 220, padPt: 3, bg: '#fff', pair: true, makeCanvas: r.make });
  ok(drew.length === 2 && drew.every(d => d.c.bbox[2] - d.c.bbox[0] === 24), 'a twin design: the first body drawn twice, never the four bodies');
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3, bg: '#fff', pair: true, highlight: 'L', makeCanvas: r.make });
  ok(r.log.some(l => l[0] === 'fillRect' && l[1][1] === 0 && l[1][0] > 60), 'highlight Left washes the Right ear out, as in a mismatched pair');
  ok(PT.canvasFor(fakeP, bootCharm('PR_BOOT'), { size: 220, padPt: 3, bg: '#fff', pair: true }) === null && PT.canvasFor(null, bootCharm('PR_BOOT'), { size: 220, pair: true, makeCanvas: r.make }) === null, 'no canvas maker or no drawer: null, never a throw');
}

/* ── 1b · a line of TWO separate designs: the Left's design and the Right's side by side ── */
{
  const tp = PT.twoPlan(bootCharm('A'), flagCharm('B'), { turnRight: true });
  ok(tp && tp.bodies.length === 2 && tp.bodies[0].side === 'L' && tp.bodies[1].side === 'R', 'two one-body designs have a Left and a Right plan');
  ok(tp.bodies[0].bbox[2] - tp.bodies[0].bbox[0] === 24 && tp.bodies[1].bbox[2] - tp.bodies[1].bbox[0] === 14, 'each keeps its own width (the Left 24, the Right 14: two different designs)');
  ok(tp.bodies[1].bbox[0] > tp.bodies[0].bbox[2], 'the Right sits to the right of the Left, with a gap');
  ok(tp.bodies.map(b => b.mirror).join() === 'false,true' && PT.twoPlan(bootCharm('A'), flagCharm('B'), {}).bodies.map(b => b.mirror).join() === 'false,false', 'the Right is turned only when the caller says its own drawing needs it');
  ok(tp.frame[3] === 30 && tp.bodies[0].bbox[1] === 2 && tp.bodies[1].bbox[1] === 0, 'the shorter design is centred on the taller (frame 30 high, the Left 26 starts at 2)');
  ok(PT.twoPlan(bootCharm('A'), threeCharm(), {}) === null && PT.twoPlan(bootCharm('A'), mittens(), {}) === null && PT.twoPlan(bootCharm('A'), null, {}) === null && PT.twoPlan(null, flagCharm('B'), {}) === null, 'a design of several bodies, or none, cannot be one ear of two designs');
  let r = recorder(); drew = [];
  const cv = PT.canvasFor(fakeP, bootCharm('A'), { size: 220, padPt: 3, bg: '#fff', pair: true, other: flagCharm('B'), turnRight: true, makeCanvas: r.make });
  ok(cv && cv.width <= 220 && cv.height <= 220, 'the picture stays inside the size asked for');
  ok(drew.length === 2 && drew[0].c.name === 'A' && drew[1].c.name === 'B' && drew[0].k === drew[1].k, 'the Left design and the Right design, each drawn once at one scale: ' + drew.map(d => d.c.name));
  ok(drew[1].p[0] > drew[0].q[0], 'the Right design starts to the right of where the Left ends');
  ok(turns(r.log) === 1 && chipsOf(r.log).join() === 'Left,Right', 'the Right is turned over and a Left and a Right chip are drawn: ' + turns(r.log) + ' ' + chipsOf(r.log));
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, bootCharm('A'), { size: 220, padPt: 3, bg: '#fff', pair: true, other: flagCharm('B'), makeCanvas: r.make });
  ok(drew.length === 2 && turns(r.log) === 0, 'a Right design that already faces right is not turned');
  r = recorder(); drew = [];
  ok(PT.canvasFor(fakeP, bootCharm('A'), { size: 220, padPt: 3, bg: '#fff', pair: true, other: threeCharm(), makeCanvas: r.make }) === null && drew.length === 0, 'a Right design that cannot be an ear: the maker draws the Left as it always did');
  r = recorder(); drew = [];
  PT.canvasFor(fakeP, bootCharm('A'), { size: 220, padPt: 3, bg: '#fff', pair: true, other: flagCharm('B'), side: 'R', mirror: true, makeCanvas: r.make });
  ok(drew.length === 1 && drew[0].c.name === 'A', 'one ear asked for by side is still that one piece, never the pair');
}

/* ── 2 · the wiring, read from the sources ── */
{
  const bg = read('charm-nest-background.js'), wk = read('charm-nest-compute-worker.js'), br = read('charm-nest-bridge.js'), html = read('charm-nest-1.html');
  ok(/opts\.pair===true/.test(bg) && /pair:opts\.pair===true\?true:undefined/.test(bg) && /facing:opts\.pair===true\?opts\.facing:undefined/.test(bg), 'the page proxy hands pair, facing and sku to the worker');
  ok(/pair:a\.opts&&a\.opts\.pair,facing:a\.opts&&a\.opts\.facing,sku:a\.opts&&a\.opts\.sku/.test(wk), 'the worker hands them to the component');
  ok(/pair: opts && opts\.pair, facing: opts && opts\.facing, sku: opts && opts\.sku/.test(br), 'renderFront (no worker) hands them to the component');
  ok(br.includes('const earPair = row =>') && br.includes('${earPair(row) ? " · Left + Right" : ""}') && !br.includes('" · Left ear"'), 'the Vector design caption says Left + Right for every earring pair line, a line of two separate designs too');
  ok(/other:opts\.pair===true&&opts\.other\?charm\(opts\.other\):undefined,turnRight:/.test(bg) && /other:a\.opts&&a\.opts\.other,turnRight:a\.opts&&a\.opts\.turnRight/.test(wk) && /other: opts && opts\.other, turnRight: opts && opts\.turnRight/.test(br), 'the second design (and which way the Right is turned) goes through the page proxy, the worker and the no-worker fallback');
  ok(/const both=!\(opts && \(opts\.side==='L' \|\| opts\.side==='R' \|\| opts\.highlight \|\| opts\.piece===true\)\) && matchPair\(row\)/.test(br) && /const matchPair = row => .*!pairRow\(row\).*earPairRow\(row\)/.test(br), 'the line picture is the pair only when no single ear is asked for');
  ok(/!opts\?\.highlight && !opts\?\.side && !opts\?\.piece\?\(matchPair\(row\)\?'\|pair':twoOf\(row\)\?'\|two:'/.test(br), 'a pair line (one design, or two) is its own picture key');
  ok(html.includes('.comparePair figure:first-child figcaption{white-space:normal'), 'the longer caption wraps under its picture instead of running into the Etsy listing caption');
  ok(/"charm-nest-pair-thumb\.js"/.test(read('scripts/build-public.cjs')), 'the public build ships the component (it already did)');
}

/* ── 2b · the Efficiency person page's order tiles: a pair order is ONE row with a Left tile and a Right tile ── */
{
  const vm = require('vm'), win = { document: { getElementById: () => null, addEventListener() {}, createElement: () => ({}) }, console }; win.window = win; vm.createContext(win);
  vm.runInContext(read('charm-nest-efficiency-orders.js'), win);
  const E = win.EfficiencyOrders, o = JSON.parse(JSON.stringify(E.normOrder({ rid: '4175272689', piecesCount: 2, pieces: [{ id: 'a_1', label: 'NEW SUGAR SKULL · Left', sku: 'NEW SUGAR SKULL', side: 'L', thumbUrl: '' }, { id: 'a_2', label: 'NEW SUGAR SKULL · Right', sku: 'NEW SUGAR SKULL', side: 'R', thumbUrl: '' }, { id: 'b_1', label: 'Heart', sku: 'HEART', side: 'X' }] })));
  ok(o.pieces[0].side === 'L' && o.pieces[1].side === 'R' && o.pieces[2].side === '', 'the order list keeps a piece\'s side (Left or Right) and nothing else: ' + o.pieces.map(p => p.side || '-').join());
  const eo = read('charm-nest-efficiency-orders.js');
  ok(eo.includes('data-n="${p && p.side ? p.side : i + 1}"') && eo.includes('data-side="${p.side}"'), 'a Left or Right tile wears L or R where it wore the piece number, and carries its side to its picture');
  ok(eo.includes('vecThumb(t.dataset.sku, t.dataset.side)') && eo.includes('vecThumb(i.dataset.sku, i.dataset.side)'), 'the vector fallback of a tile and of the hover card asks for that ear');
}

{
  const es = read('charm-nest-efficiency-stations.js'), pd = read('charm-nest-piece-dots.js');
  ok(es.includes('vectorOf(ctx.rid, ctx.pool, ctx.side)') && es.includes('side: p.both ? "" : p.side') && es.includes('PM.earMirror(row, "R")'), 'the stations board: a Right piece tile that falls back to its vector draws that ear turned over, a Left, a mismatched pair\'s one tile and any other piece as before');
  ok(/vectorThumb\(row, opts\) : LM\.vectorThumb\(row\)/.test(pd), 'the piece dots still ask for one ear or the piece as drawn, never the pair (unchanged file)');
}

/* ── 3 · the real app: the Orders tab, the Review card and the order window, in headless Chromium over the fake site ── */
(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log(`pair-rows-lists: ${n} checks passed (no playwright-core: the browser checks were not run)`); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log(`pair-rows-lists: ${n} checks passed (no chrome: the browser checks were not run)`); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
  const line = (tid, sku, title, q, vars) => ({ transactionId: tid, listingId: '19100' + tid.slice(-5), sku, title, quantity: q, expectedShipDate: SHIP, variations: (vars || [['Metal', 'Gold Filled']]).map(([name, value]) => ({ name, value })) });
  const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const I = '4300000009', A = '4300000001', B2 = '4300000002', C = '4300000003', D = '4300000004', E = '4300000005', F = '4300000006', G = '4300000007', H = '4300000008';
  const orders = [
    order(A, 'Stud Pair', [line('9001', 'PR_BOOT', 'Boot stud earrings', 1)]),
    order(B2, 'Two Pairs', [line('9002', 'PR_BOOT', 'Boot stud earrings (2 pairs)', 2)]),
    order(C, 'Mismatched', [line('9003', 'MISMATCHED_7134', 'Mismatched mittens earrings', 1)]),
    order(D, 'Huggie Pair', [line('9004', 'PR_HUGGIE', 'Huggie Hoop Earrings with a boot charm', 1, [['METAL CHOICE', 'Silver'], ['HOOP SIZE', '8.5mm']])]),
    order(E, 'Plain Charm', [line('9005', 'PR_CHARM', 'Boot charm necklace', 1, [['Metal', 'Gold Filled']])]),
    order(F, 'Plain Four', [line('9006', 'PR_CHARM', 'Boot charm necklace', 4, [['Metal', 'Gold Filled']])]),
    order(G, 'Single Ear', [line('9007', 'PR_BOOT', 'Custom Single Replacement Boot Earring Left Ear', 1)]),
    order(H, 'Discs', [line('9008', 'PR_CHARM', 'Initial Disc Necklace with Satellite Chain', 1, [['Number of Discs / Metal', '3 discs - gold']])]),
    order(I, 'Two Designs', [line('9009', 'Huggie Hoops-Tennis Ball/Racket3', 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Sports Jewelry', 1, [['METAL CHOICE', 'Silver'], ['HOOP SIZE', '8.5mm']])])
  ];
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [];
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 2200 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(40000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderPieces && window.CharmNestPair && window.PieceMedia && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    const fixtures = { PR_BOOT: bootCharm('PR_BOOT'), PR_HUGGIE: bootCharm('PR_HUGGIE'), PR_CHARM: bootCharm('PR_CHARM'), MISMATCHED_7134: mittens(), 'TENNIS BALL (HUGGIE)': bootCharm('TENNIS BALL (HUGGIE)'), 'TENNIS RACKET (HUGGIE)': flagCharm('TENNIS RACKET (HUGGIE)') };
    await page.evaluate(async ({ orders, fixtures }) => {
      for (const sku of Object.keys(fixtures)) {
        const aiPath = 'charmnest/test/' + sku + '.ai', e = { sku, aiPath, charmHash: 'h-' + sku, indexedAt: 1, upAngle: 90, engravable: true, sizes: null };
        if (sku === 'MISMATCHED_7134') e.pair = { v: 1, bodies: 2, mismatched: true };
        B.master.entries.set(sku, e);
        B.pool.sources.set(aiPath, { id: 'src-' + sku, charms: [fixtures[sku]], usedAt: Date.now() });   // (the design read already: the list draws it from here, as it does for a design in use)
      }
      await Orders.loadMaps(true);
      for (const order of orders) order.lines.forEach(line => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders, fixtures });
    await page.evaluate(() => {
      window.__ink = async src => {   // the picture's ink clusters above the chip band, the chips, and whether the right half is the exact mirror of the left
        const im = new Image(); im.src = src; await im.decode(); const cv = document.createElement('canvas'); cv.width = im.width; cv.height = im.height; const c = cv.getContext('2d'); c.drawImage(im, 0, 0); const d = c.getImageData(0, 0, im.width, im.height).data;
        const ink = (x, y) => { const i = (y * im.width + x) * 4; return !(d[i] > 200 && d[i + 1] > 200 && d[i + 2] > 200); }, dark = (x, y) => { const i = (y * im.width + x) * 4; return d[i] < 70 && d[i + 1] < 70 && d[i + 2] < 70; };
        const band = Math.round(im.height * .16), H = im.height - band;
        const cl = (y0, y1, f) => { const cols = []; for (let x = 0; x < im.width; x++) { let h = false; for (let y = y0; y < y1 && !h; y++) h = f(x, y); cols.push(h); } const out = []; let s = -1; for (let x = 0; x <= im.width; x++) { if (cols[x] && s < 0) s = x; if (!cols[x] && s >= 0) { out.push([s, x - 1]); s = -1; } } return out; };
        const bodies = cl(0, H, ink), chips = cl(H, im.height, dark);
        let same = null, shifted = null;
        if (bodies.length === 2) { const [a, b] = bodies, w = Math.min(a[1] - a[0], b[1] - b[0]); let eq = 0, tot = 0, eq2 = 0, tot2 = 0; for (let y = 0; y < H; y++) for (let k = 0; k <= w; k++) { const p = ink(a[0] + k, y), q = ink(b[1] - k, y), q2 = ink(b[0] + k, y); if (p || q) { tot++; if (p === q) eq++; } if (p || q2) { tot2++; if (p === q2) eq2++; } } same = tot ? eq / tot : 0; shifted = tot2 ? eq2 / tot2 : 0; }
        return { w: im.width, h: im.height, bodies, chips, mirrorMatch: same, copyMatch: shifted };
      };
    });
    const rowsOf = async rid => page.evaluate(async rid => {
      const nodes = [...document.querySelectorAll('#ordItems > [data-rid]')].filter(x => x.dataset.rid === rid);
      return Promise.all(nodes.map(async nd => { const v = nd.querySelector('[data-vector]'), im = v && v.querySelector('img'); return { caption: (nd.querySelector('.comparePair figure:first-child figcaption') || {}).textContent, facts: ((nd.querySelector('.rowFacts') || {}).textContent || '').replace(/\s+/g, ' '), pic: im && im.src ? await window.__ink(im.src) : null, src: im ? im.src : null }; }));
    }, rid);
    await page.waitForFunction(() => [...document.querySelectorAll('#ordItems [data-vector]')].every(h => h.getAttribute('aria-busy') === 'false' && h.querySelector('img')), null, { timeout: 60000 }).catch(async e => { console.log(JSON.stringify(await page.evaluate(() => [...document.querySelectorAll('#ordItems > [data-rid]')].map(n => [n.dataset.rid, (n.querySelector('[data-vector]') || {}).textContent, (n.querySelector('[data-vector]') || {}).innerHTML.slice(0, 80), (B.orders.byKey.get(n.dataset.key) || {}).spec && B.orders.byKey.get(n.dataset.key).spec.designSku]))), 'orders'); throw e; });
    const got = {}; for (const rid of [A, B2, C, D, E, F, G, H]) got[rid] = await rowsOf(rid);
    for (const rid of Object.keys(got)) ok(got[rid].length === 1, `order ${rid}: ONE row in the Orders list`);
    // an earring pair: both vectors, the Right the exact mirror of the Left, a Left and a Right chip, "Left + Right" said twice
    for (const [rid, what] of [[A, 'stud pair'], [D, 'huggie hoop pair'], [B2, 'two pairs']]) {
      const r = got[rid][0];
      ok(/Left \+ Right/.test(r.caption), `${what}: the caption says Left + Right (${r.caption})`);
      ok(r.pic.bodies.length === 2, `${what}: the picture shows two bodies side by side ${JSON.stringify(r.pic.bodies)}`);
      const wd = b => b[1] - b[0]; ok(Math.abs(wd(r.pic.bodies[0]) - wd(r.pic.bodies[1])) <= 2, `${what}: both ears the same size`);
      ok(r.pic.mirrorMatch > .9 && r.pic.mirrorMatch > r.pic.copyMatch + .1, `${what}: the Right is the mirror of the Left, not a copy of it (mirror ${r.pic.mirrorMatch.toFixed(3)}, plain copy ${r.pic.copyMatch.toFixed(3)})`);
      ok(r.pic.chips.length === 2, `${what}: a Left and a Right chip under them ${JSON.stringify(r.pic.chips)}`);
    }
    ok(/Qty 1 · Left \+ Right/.test(got[A][0].facts) && /Qty 1 · Left \+ Right/.test(got[D][0].facts), 'a pair says Qty 1 · Left + Right');
    ok(/Qty 2 · 2 pairs, Left \+ Right each/.test(got[B2][0].facts), 'two pairs say so: ' + got[B2][0].facts);
    // a mismatched pair: its own two bodies, different sizes, as before
    { const r = got[C][0]; ok(/Left \+ Right/.test(r.caption) && r.pic.bodies.length === 2 && r.pic.chips.length === 2 && /Qty 1 · Left \+ Right/.test(r.facts), 'a mismatched pair: its own two bodies, caption Left + Right, two chips ' + JSON.stringify(r.pic.bodies)); }
    // singles, discs, a single named ear, a plain quantity-4 line: one body, no chips, no pair words, the very picture as before
    const plainPic = await page.evaluate(async () => Pool.masterPreview(Master.entryFor('PR_CHARM'), null, true));
    const plainBoot = await page.evaluate(async () => Pool.masterPreview(Master.entryFor('PR_BOOT'), null, true));
    for (const [rid, what, ref] of [[E, 'a plain charm', plainPic], [F, 'a plain quantity-4 line', plainPic], [H, 'a 3-disc necklace', plainPic], [G, 'a single earring that names its ear', plainBoot]]) {
      const r = got[rid][0];
      ok(!/Left \+ Right/.test(r.caption + r.facts), `${what}: no pair words (${r.caption} | ${r.facts})`);
      ok(r.pic.bodies.length === 1 && r.pic.chips.length === 0, `${what}: ONE body and no chips ${JSON.stringify(r.pic.bodies)}`);
      ok(r.src === ref || r.src.replace(/^blob:.*/, '') === '' || r.pic.w === (await page.evaluate(async s => { const im = new Image(); im.src = s; await im.decode(); return im.width; }, ref)), `${what}: the picture is the plain one`);
    }
    ok(/Qty 4/.test(got[F][0].facts) && !/pairs/.test(got[F][0].facts), 'a plain quantity-4 line says Qty 4 as before');
    // the cache keys: the pair line is its own picture, never one body as drawn nor one ear
    const keys = await page.evaluate(({ A, E }) => { const a = B.orders.rows.find(r => r.order.receiptId === A), e = B.orders.rows.find(r => r.order.receiptId === E); return { plain: PieceMedia.vectorKey(e), pair: PieceMedia.vectorKey(a, { pair: true }), piece: PieceMedia.vectorKey(a), left: PieceMedia.vectorKey(a, { side: 'L' }), right: PieceMedia.vectorKey(a, { side: 'R', mirror: true }) }; }, { A, E });
    ok(!/\|pair/.test(keys.piece), 'vectorKey of a piece (the hover cards, the station tiles, the piece dots) is what it always was: ' + keys.piece);
    ok(/\|pair$/.test(keys.pair) && !/\|pair/.test(keys.plain) && !/\|pair/.test(keys.left) && new Set([keys.pair, keys.left, keys.right]).size === 3, 'vectorKey: the pair line, the Left ear and the Right ear are three pictures ' + JSON.stringify(keys));
    // one ear asked for by side is still ONE ear (the piece dots and the order window's piece rows)
    const ear = await page.evaluate(async A => { const a = B.orders.rows.find(r => r.order.receiptId === A); const pairSrc = await PieceMedia.vectorThumb(a, { pair: true }), pieceSrc = await PieceMedia.vectorThumb(a), left = await PieceMedia.vectorThumb(a, { side: 'L', mirror: false, highlight: 'L', px: 120 }), right = await PieceMedia.vectorThumb(a, { side: 'R', mirror: true, highlight: 'R', px: 120 }); return { pair: await window.__ink(pairSrc), piece: await window.__ink(pieceSrc), left: await window.__ink(left), right: await window.__ink(right) }; }, A);
    ok(ear.piece.bodies.length === 1 && ear.piece.chips.length === 0, 'a hover card or a station tile that asks for a piece of the line (no ear named) is ONE body as drawn, never the pair: ' + JSON.stringify(ear.piece.bodies));
    ok(ear.pair.bodies.length === 2 && ear.left.bodies.length === 1 && ear.right.bodies.length === 1, 'the line is the pair; an ear asked for by side is one body: ' + JSON.stringify([ear.pair.bodies.length, ear.left.bodies.length, ear.right.bodies.length]));
    // the order window's header: the same picture, from the same component
    await page.evaluate(A => { OrderWin.open(B.orders.rows.find(r => r.order.receiptId === A).key, { view: 'info' }); }, A);
    await page.waitForFunction(() => { const v = document.getElementById('owVector'); return v && v.querySelector('img') && v.getAttribute('aria-busy') !== 'true'; }, null, { timeout: 30000 });
    const head = await page.evaluate(async () => { const im = document.querySelector('#owVector img'); return { pic: await window.__ink(im.src), pieces: [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => r.dataset.side || '') }; });
    ok(head.pic.bodies.length === 2 && head.pic.chips.length === 2, 'the order window header shows the pair side by side ' + JSON.stringify(head.pic.bodies));
    ok(head.pieces.filter(Boolean).join() === 'L,R', 'its pieces list still has the Left and the Right piece rows: ' + head.pieces);
    await page.evaluate(() => OrderWin.close()); await page.waitForTimeout(300);
    // counts that say orders and pieces truthfully (the Review card of several lines says "N pieces · M orders"): a pair is 1 order and 2 pieces; every other line counts as the one it always did
    const counts = await page.evaluate(({ A, B2, C, E, F, G, H }) => {
      const R = rid => B.orders.rows.find(r => r.order.receiptId === rid);
      const pill = (...ids) => { const rows = ids.map(R), n = Review.card({ kind: 'unmatchedSku', key: 'ord:rl-' + ids.join('-'), row: rows[0], rows, problem: {}, why: 'test' }); return ((n.querySelector('.pill') || {}).textContent || ''); };
      return { pairPlain: pill(A, E), twoPairsPlain: pill(B2, E), misPlain: pill(C, E), plainPlain4: pill(E, F), singlePlain: pill(G, E), discsPlain: pill(H, E), mixed: pill(A, B2, E, F), pairs: pill(A, B2) };
    }, { A, B2, C, E, F, G, H });
    ok(/3 pieces · 2 orders/.test(counts.pairPlain), 'a pair + a plain line: 3 pieces, 2 orders: ' + counts.pairPlain);
    ok(/5 pieces · 2 orders/.test(counts.twoPairsPlain), 'two pairs + a plain line: 5 pieces, 2 orders: ' + counts.twoPairsPlain);
    ok(/3 pieces · 2 orders/.test(counts.misPlain), 'a mismatched pair + a plain line: 3 pieces, 2 orders: ' + counts.misPlain);
    ok(/2 pieces · 2 orders/.test(counts.plainPlain4) && /2 pieces · 2 orders/.test(counts.singlePlain), 'a plain line, a quantity-4 line and a single named ear count as the one line they always did: ' + [counts.plainPlain4, counts.singlePlain].join(' | '));
    // (DISCREAD 3: a counted line, "3 discs", is its 3 pieces wherever the app says how many, the count the pipeline cuts: it used to count as one line)
    ok(/4 pieces · 2 orders/.test(counts.discsPlain), 'a 3-disc necklace + a plain line: 3 + 1 pieces, 2 orders: ' + counts.discsPlain);
    ok(/8 pieces · 4 orders/.test(counts.mixed), 'a mixed group adds them up: ' + counts.mixed);
    ok(/6 pieces · 2 orders/.test(counts.pairs), 'two pair orders are 2 orders and 6 pieces, never "2 pieces": ' + counts.pairs);
    // a line of TWO separate designs (order 4171010675's shape: Left Tennis Ball, Right Tennis Racket): ONE row, the two designs side by side, each its own size, a Left and a Right chip
    const two = await page.evaluate(async I => {
      const r = B.orders.rows.find(x => x.order.receiptId === I);
      await new Promise(res => setTimeout(res, 50));
      const members = r.spec && r.spec.pair && (r.spec.pair.members || []).map(m => m.side + ':' + m.sku);
      const nodes = [...document.querySelectorAll('#ordItems > [data-rid]')].filter(x => x.dataset.rid === I);
      const nd = nodes[0], v = nd && nd.querySelector('[data-vector]');
      for (let i = 0; i < 300 && !(v && v.querySelector('img') && v.getAttribute('aria-busy') === 'false'); i++) await new Promise(res => setTimeout(res, 100));
      const im = v && v.querySelector('img');
      return { rows: nodes.length, members, caption: nd && (nd.querySelector('.comparePair figure:first-child figcaption') || {}).textContent, facts: nd && ((nd.querySelector('.rowFacts') || {}).textContent || '').replace(/\s+/g, ' '), pic: im && await window.__ink(im.src), key: PieceMedia.vectorKey(r, { pair: true }), pieceKey: PieceMedia.vectorKey(r) };
    }, I);
    ok(two.rows === 1, 'two designs: ONE row in the Orders list');
    ok(JSON.stringify(two.members) === JSON.stringify(['L:TENNIS BALL (HUGGIE)', 'R:TENNIS RACKET (HUGGIE)']), 'the intake reads the Left and the Right design: ' + JSON.stringify(two.members));
    ok(/Left \+ Right/.test(two.caption || '') && /Qty 1 · Left \+ Right/.test(two.facts || ''), 'two designs: caption and fact say Left + Right (not "Left ear"): ' + two.caption + ' | ' + two.facts);
    ok(two.pic && two.pic.bodies.length === 2 && two.pic.chips.length === 2, 'two designs: BOTH designs are drawn, a Left and a Right chip ' + JSON.stringify(two.pic && two.pic.bodies));
    { const wd = b => b[1] - b[0], b = two.pic.bodies; ok(wd(b[0]) > wd(b[1]) * 1.4, `the Left design (24 wide) is wider than the Right (14 wide), at one scale: ${wd(b[0])} vs ${wd(b[1])}`); }
    ok(/\|two:TENNIS RACKET \(HUGGIE\)$/.test(two.key) && !/\|two/.test(two.pieceKey), 'the line picture is keyed by both designs, a piece-level key is not: ' + two.key + ' / ' + two.pieceKey);
    // the Efficiency tile's vector fallback: the Right piece's picture is the Left turned over (the same rule), the Left and a plain tile are as drawn
    const tile = await page.evaluate(async () => {
      const L = await EfficiencyOrders.vecThumb('PR_BOOT', 'L'), R = await EfficiencyOrders.vecThumb('PR_BOOT', 'R'), P0 = await EfficiencyOrders.vecThumb('PR_BOOT'), mir = PieceMedia.earMirror({ spec: { designSku: 'PR_BOOT' }, line: { sku: 'PR_BOOT' } }, 'R'), none = PieceMedia.earMirror({ spec: { designSku: 'NO SUCH' }, line: { sku: 'NO SUCH' } }, 'R');
      const px = async src => { const im = new Image(); im.src = src; await im.decode(); const c = document.createElement('canvas'); c.width = im.width; c.height = im.height; const g = c.getContext('2d'); g.drawImage(im, 0, 0); return { w: im.width, h: im.height, d: g.getImageData(0, 0, im.width, im.height).data }; };
      const a = await px(L), b = await px(R), p = await px(P0), ink = (o, x, y) => { const i = (y * o.w + x) * 4; return !(o.d[i] > 200 && o.d[i + 1] > 200 && o.d[i + 2] > 200); };
      let flip = 0, copy = 0, tot = 0; for (let y = 0; y < a.h; y++) for (let x = 0; x < a.w; x++) { const l = ink(a, x, y), r = ink(b, x, y), m = ink(a, a.w - 1 - x, y); if (l || r) { tot++; if (r === m) flip++; if (r === l) copy++; } }
      let same = a.w === p.w && a.h === p.h; if (same) for (let i = 0; i < a.d.length; i += 4) if (Math.abs(a.d[i] - p.d[i]) > 2) { same = false; break; }
      return { mir, none, flip: flip / tot, copy: copy / tot, plainIsLeft: same, hasR: !!R };
    });
    ok(tile.mir === true && tile.none === false, 'earMirror: a design known here turns its Right ear, one that is not known does not');
    ok(tile.hasR && tile.flip > .9 && tile.flip > tile.copy + .1, `the Right tile's vector is the mirror of the Left tile's (mirror ${tile.flip.toFixed(3)}, plain copy ${tile.copy.toFixed(3)})`);
    ok(tile.plainIsLeft, 'the Left tile, and a tile with no side, are the design as drawn, exactly as before');
    ok(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`pair-rows-lists: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
