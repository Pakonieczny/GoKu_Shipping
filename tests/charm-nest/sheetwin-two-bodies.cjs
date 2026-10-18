// SHEETWIN TWO BODIES (Paul, 18 Oct 2026, order 4193072538, a GF sheet): "These 2 charms (Moon + Star) are not being correctly identified as belonging to the same order."
// The design TINY MOON & STARS 2 draws two bodies, a crescent moon and a star, and its line is cut as ONE piece holding both (a two-body design on a line that is not an earring pair, an old
// glued pair, or a pair whose bodies share a group of the master: Paul's rulings of 10 and 11 Oct keep such a piece whole). The sheet window rang only the piece's first body (its outline) and
// said "This order · 1 piece · this charm", so the star beside the ringed moon looked like no part of the order. This pins what the window says and draws of such a piece:
//   pure: the sheet window's own helpers (ringParts, charmsIn, copyText) read as source and run in node's vm over the real CharmNestPair;
//   browser (headless Chromium over the fake site, bridge-server.cjs: nothing live): the panel's heading and words, the plate's tip, and the gold line round the star as well as the moon.
//   node tests/charm-nest/sheetwin-two-bodies.cjs [--pure]       (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
let n = 0; const t = (name, fn) => { try { fn(); n++; console.log('ok   ' + name); } catch (e) { console.error('FAIL ' + name + '\n' + (e.stack || e)); process.exitCode = 1; } };

// ── the design: a crescent moon 13.8 mm tall and a 7 mm star beside it, two closed cut paths under one label (TINY MOON & STARS 2) ──
const MM = 72 / 25.4;
const cutPath = pts => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return { kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], layer: 'CUT', lwPt: 0.28, paintOp: 'S', subpaths: [[['m', pts[0]]].concat(pts.slice(1).map(p => ['l', p]))], bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }; };
function crescent(cx, cy, r) {
  const pts = [];
  for (let i = 0; i <= 40; i++) { const a = Math.PI / 2 + (i / 40) * Math.PI; pts.push([cx + r * Math.cos(a), cy + r * Math.sin(a)]); }
  for (let i = 40; i >= 0; i--) { const a = Math.PI / 2 + (i / 40) * Math.PI; pts.push([cx + 0.45 * r * Math.cos(a), cy + r * Math.sin(a)]); }
  return cutPath(pts);
}
function star(cx, cy, R) { const pts = []; for (let i = 0; i < 10; i++) { const a = Math.PI / 2 + i * Math.PI / 5, r = i % 2 ? R * 0.42 : R; pts.push([cx + r * Math.cos(a), cy + r * Math.sin(a)]); } pts.push(pts[0]); return cutPath(pts); }
function moonAndStar() {
  const moon = crescent(7 * MM, 6.9 * MM, 6.9 * MM), st = star(15.1 * MM, 6.9 * MM, 3.5 * MM);
  const bbox = [Math.min(moon.bbox[0], st.bbox[0]), Math.min(moon.bbox[1], st.bbox[1]), Math.max(moon.bbox[2], st.bbox[2]), Math.max(moon.bbox[3], st.bbox[3])];
  return { sku: 'TINY MOON & STARS 2', name: 'TINY MOON & STARS 2', outline: moon, members: [moon, st], bbox, widthPt: bbox[2] - bbox[0], heightPt: bbox[3] - bbox[1], centerPt: [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2], strokePt: 0.28 };
}
function onlyMoon() { const c = moonAndStar(); c.members = [c.outline]; c.bbox = c.outline.bbox.slice(); c.widthPt = c.bbox[2] - c.bbox[0]; c.heightPt = c.bbox[3] - c.bbox[1]; c.centerPt = [(c.bbox[0] + c.bbox[2]) / 2, (c.bbox[1] + c.bbox[3]) / 2]; return c; }

// ── the sheet window's own helpers, as the file has them ──
const src = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8');
const lineOf = start => { const i = src.indexOf(start); assert.ok(i > 0, 'found: ' + start); return src.slice(i, src.indexOf('\n', i)); };
function helpers(o) {
  const ctx = Object.assign({ window: { CharmNestPair: Pair }, cutLinesOf: c => c.members.filter(m => m !== c.outline && m.kind === 'path' && m.closed), tryDo: f => { try { return f ? f() : null; } catch (_) { return null; } },
    sideOfPiece: x => x.side || null, discOfPiece: () => null, sideWord: s => (s === 'L' ? 'Left' : s === 'R' ? 'Right' : '') }, o || {});
  vm.runInNewContext(['  const ringParts = ', '  const charmsIn = ', '  const charmsWord = ', '  const copyText = '].map(lineOf).join('\n') + '\n;this.H = { ringParts, charmsIn, charmsWord, copyText };', ctx);
  return ctx.H;
}

t('the design reads as two different bodies (a mismatched design), the way the pair model reads every design', () => {
  const d = Pair.describe(moonAndStar()); assert.equal(d.count, 2); assert.equal(d.mismatched, true);
});
t('charmsIn: a piece holding both bodies is 2 charms; a piece of one body, a piece with no drawing and a page without the pair model are 1', () => {
  const H = helpers();
  assert.equal(H.charmsIn({ c: moonAndStar() }), 2);
  assert.equal(H.charmsIn({ c: onlyMoon() }), 1);
  assert.equal(H.charmsIn({ c: null }), 1); assert.equal(H.charmsIn(null), 1);
  assert.equal(helpers({ window: {} }).charmsIn({ c: moonAndStar() }), 1, 'without CharmNestPair nothing is said');
});
t('ringParts: the line round a piece goes round the star as well as the moon; a charm of one body is ringed along its outline exactly as before', () => {
  const H = helpers(), c = moonAndStar(), parts = H.ringParts(c);
  assert.equal(parts.length, 2); assert.equal(parts[0], c.outline.subpaths[0]); assert.equal(parts[1], c.members[1].subpaths[0], 'the star is a part of the ring');
  const one = onlyMoon(); assert.deepEqual(H.ringParts(one), one.outline.subpaths);
  assert.deepEqual(helpers({ cutLinesOf: undefined }).ringParts(c), c.outline.subpaths, 'a page without cutLinesOf rings the outline, as before');
});
t('copyText: the panel says "2 charms, one piece" before the size; an ear, a copy and a single charm read as before', () => {
  const H = helpers();
  assert.equal(H.copyText({ c: moonAndStar(), qty: 1, copy: 1 }), '2 charms, one piece · ');
  assert.equal(H.copyText({ c: moonAndStar(), qty: 2, copy: 1 }), 'Copy 1 of 2 · 2 charms, one piece · ');
  assert.equal(H.copyText({ c: onlyMoon(), qty: 1, copy: 1 }), '');
  assert.equal(H.copyText({ c: onlyMoon(), qty: 3, copy: 2 }), 'Copy 2 of 3 · ');
  assert.equal(H.copyText({ c: onlyMoon(), side: 'L', qty: 1, copy: 1 }), 'Left piece · ');
});
console.log(`\n${n} checks (pure)`);

/* ═══════════════════════════ the sheet window, in a browser ═══════════════════════════ */
if (process.argv.includes('--pure') || process.exitCode) return;
(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const { start } = require('./bridge-server.cjs');
  const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool';
  const E = '4193072538', F = '4300000201', TXE = 5000000071, TXF = 5000000072;
  const pE = `${E}_${TXE}_1`, pF = `${F}_${TXF}_1`, charm = moonAndStar();
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  // GF Sheet 1: the moon-and-star piece of order E (one pool piece, no ear: the line is not an earring pair) and a plain piece of order F
  st.put(SHEETS, 'gold-ms-1', { id: 'gold-ms-1', metal: 'gold', charms: [{ id: 'm0', poolId: pE, order: E, sku: 'TINY MOON & STARS 2', name: `${E} · TINY MOON & STARS 2` }, { id: 'm1', poolId: pF, order: F, sku: 'TEST-F', name: `${F} · TEST-F` }],
    placements: [{ id: 'm0', cxPt: 70, cyPt: 50, angle: 0, wPt: charm.widthPt, hPt: charm.heightPt }, { id: 'm1', cxPt: 170, cyPt: 50, angle: 0, wPt: 20, hPt: 20 }], poolIds: [pE, pF], orders: [E, F], sheetIndex: 1,
    fileBase: 'GF_2026-10-18_Set-1_Sheet-1', runId: 'run-ms-1', placedCount: 2, charmCount: 2 });
  for (const [pid, rid, tx, sku] of [[pE, E, TXE, 'TINY MOON & STARS 2'], [pF, F, TXF, 'TEST-F']]) st.put(POOL, pid, { poolId: pid, orderId: rid, sheetId: 'gold-ms-1', state: 'placed', material: 'gold', lineKey: `${rid}_${tx}`, sku, copy: 1, quantity: 1 });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.OrderPieces && window.CharmNestPair, null, { timeout: 60000 });
    await page.evaluate(([e, f, txe, txf]) => {   // the lines the order rows carry (the pull): order E is ONE line of quantity 1 that makes one piece (a necklace of the two-body design)
      const row = (rid, tx, sku, title) => ({ key: `${rid}_${tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer' }, lines: [], messages: [] },
        line: { transactionId: String(tx), listingId: '', sku, title, quantity: 1, variations: [], personalization: [] }, spec: { designSku: sku, quantity: 1, material: 'gold', form: 'necklace', problems: [] },
        problems: [], state: 'written', reason: null, poolIds: [`${rid}_${tx}_1`], engrave: null, material: 'gold', arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(e, txe, 'TINY MOON & STARS 2', 'Tiny Moon & Stars Necklace'), row(f, txf, 'TEST-F', 'Test F')]; window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
    }, [E, F, TXE, TXF]);
    await page.evaluate(id => SheetWin.open('gold-ms-1', { select: id }), pF);
    await until(() => page.evaluate(() => SheetWin._W.pieces.length === 2 && SheetWin._W.geom && document.querySelectorAll('.swTrail li').length > 0), 25000, 'the sheet and the F piece');
    // the moon-and-star design, as Pool.masterCharm gives the page (the fake site holds no master file)
    await page.evaluate(([pid, c]) => { const x = SheetWin._W.pieces.find(y => y.poolId === pid); x.c = c; x._halo = null; }, [pE, charm]);
    await page.waitForTimeout(400);
    // point at the moon-and-star piece and press it: the tip, then the panel
    const at = await page.evaluate(pid => { const W = SheetWin._W, x = W.pieces.find(y => y.poolId === pid), r = W.el.fx.getBoundingClientRect(), s = r.width / W.el.fx.width; return { x: r.left + (W.R + x.p.cxPt * W.k - 0.25 * x.c.widthPt * W.k) * s, y: r.top + (W.R + x.p.cyPt * W.k) * s }; }, pE);
    await page.mouse.move(at.x, at.y); await page.waitForTimeout(250);
    const tip = await page.evaluate(() => SheetWin._W.el.tip.innerText.replace(/\s+/g, ' ').trim());
    assert.match(tip, /4193072538 TINY MOON & STARS 2 · 2 charms, one piece/, 'the tip says the piece is two charms: ' + tip);
    await page.mouse.click(at.x, at.y);
    await until(() => page.evaluate(pid => SheetWin._W.sel && SheetWin._W.sel.poolId === pid && /TINY MOON/.test((document.querySelector('.swPiece .facts b') || {}).textContent || ''), pE), 10000, 'the moon-and-star piece selected');
    await until(() => page.evaluate(() => [...document.querySelectorAll('.swTrail li')].some(li => /TINY MOON/.test(li.innerText))), 10000, 'its order list');
    const v = await page.evaluate(() => ({ head: (document.querySelector('[data-r2=trailHead]') || {}).textContent || '', facts: (document.querySelector('[data-r2=mm]') || {}).textContent || '',
      rows: [...document.querySelectorAll('.swTrail li')].map(li => li.innerText.replace(/\s+/g, ' ').trim()) }));
    assert.equal(v.head, 'This order · 1 piece · 2 charms', 'the heading counts the charms of the one piece: ' + v.head);
    assert.match(v.facts, /^2 charms, one piece · 18\.3 × 13\.8 mm$/, 'the piece pane says it is two charms: ' + v.facts);
    assert.equal(v.rows.length, 1, 'one piece in the order: ' + JSON.stringify(v.rows));
    assert.match(v.rows[0], /TINY MOON & STARS 2 ?2 charms, one piece ?these charms$/, 'its row says so: ' + v.rows[0]);
    console.log('ok   panel: "This order · 1 piece · 2 charms", "2 charms, one piece · 18.3 × 13.8 mm", its row "2 charms, one piece · these charms"; the tip says it too');
    // the gold line round the piece in hand goes round the STAR as well as the moon (it went round the moon, the outline, only)
    await page.waitForTimeout(1200);
    const ring = await page.evaluate(([pid]) => {
      const W = SheetWin._W, x = W.pieces.find(y => y.poolId === pid), c = x.c, k = W.k, cv = W.el.fx, g = cv.getContext('2d');
      const X = px => W.R + x.p.cxPt * k + (px - c.centerPt[0]) * k, Y = py => W.R + x.p.cyPt * k + (c.centerPt[1] - py) * k;
      // a band just beyond the right-hand edge of a body, as high as the body: the line lies 2.6 css px out from the cut edge
      const band = b => { const x0 = Math.ceil(X(b[2]) + 0.6 * W.dpr), x1 = Math.floor(X(b[2]) + 6 * W.dpr), y0 = Math.floor(Y(b[3])), y1 = Math.ceil(Y(b[1])); const d = g.getImageData(x0, y0, x1 - x0, y1 - y0).data; let gold = 0;
        for (let i = 0; i < d.length; i += 4) if (d[i + 3] > 90 && d[i] > 140 && d[i] - d[i + 2] > 50) gold++; return gold; };
      const moonTip = [c.outline.bbox[0], c.outline.bbox[1], c.outline.bbox[0] + 0.01, c.outline.bbox[3]];   // (the moon's left edge, read mirrored: the band left of it)
      const left = b => { const x1 = Math.floor(X(b[0]) - 0.6 * W.dpr), x0 = Math.ceil(X(b[0]) - 6 * W.dpr), y0 = Math.floor(Y(b[3])), y1 = Math.ceil(Y(b[1])); const d = g.getImageData(x0, y0, x1 - x0, y1 - y0).data; let gold = 0;
        for (let i = 0; i < d.length; i += 4) if (d[i + 3] > 90 && d[i] > 140 && d[i] - d[i + 2] > 50) gold++; return gold; };
      return { star: band(c.members[1].bbox), moon: left(moonTip) };
    }, [pE]);
    assert.ok(ring.moon > 0, 'the moon is ringed (control): ' + JSON.stringify(ring));
    assert.ok(ring.star > 0, 'the star is ringed too, as part of the same piece of the order: ' + JSON.stringify(ring));
    console.log('ok   plate: the gold line goes round the star as well as the moon of the one piece');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log('\nsheetwin-two-bodies: all passed');
  } catch (e) { console.error('errors:', errors); throw e; }
  finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
