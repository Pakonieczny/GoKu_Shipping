// RG Sheet 1 of RG Nov 22 Set-2 in the SHEET WINDOW (Paul, 22 Nov 2026), on the fake site (bridge-server.cjs: the real charmNestLibrary handler over an in-memory store) seeded from the live
// record, its physical sheet and its cuts (fixtures/rg-sheet1-window.json and rose-stock-history.json, read from the live cloud, read only). The master designs are stand-ins of each
// piece's size (the live files are not downloaded). Nothing live, no Etsy.
//   A  SRCLIST: every one of the 29 pieces is drawn from its design. 8 of them (6 designs) were drawn as plain boxes: the record's `sources` does not list their designs (the cloud put
//      them back, cmdRestoreNest), and the window looked for a piece's design only there; each piece's own entry names its file (aiPath / aiUrl)
//   B  RGWINDOW: the plate shows its physical sheet's history as the cloud's picture does: the metal cut 1 (RG Nov 18 Set-2 Sheet 1) took away in light grey (#f0eeeb) and cut 1's pieces in
//      grey (#d8d5d0) on the left, nothing grey where this sheet's own pieces stand. Before, the window never drew it: once its drawing came in over the picture the left was white.
//      B1 the sheet as it is now (its own cut 3 recorded, its stock revision 3, a viewer's view: no run, no live sheet on the page); B2 the sheet as it stood after the restore (its cut 2
//      set aside: no recorded cut, roseReopened, the stock its own at revision 2 at cut 1's outline; the history is cut 1 only, nothing grey for cut 2); B3 opened again: drawn at once,
//      its stock not read again; B4 a sheet of a stock never cut asks nothing; B5 on its Back the history is mirrored with the plate.
//   C  the order window's Sheet tab (drawOrder) and a card turned over (cardBack.sheet), which read the record as the window does, draw every piece from its design too.
//   PW_DIR=<playwright-core's node_modules> CHROMIUM=<chrome> NODE_PATH=<deps> node tests/charm-nest/sheetwin-rg-sheet1.cjs   (OUT=<dir> keeps the plates as PNG)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/rg-sheet1-window.json'), 'utf8'));
const HIST = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/rose-stock-history.json'), 'utf8'));
const S = FX.sheetId, STOCK = FX.stockId, SHEETS = 'Charm_Nest_Sheets', CUTS = `Charm_Nest_Rose_Stock/${STOCK}/cuts`;
const CUT1 = HIST.cuts.find(c => +c.revision === 1), CUT2 = HIST.cuts.find(c => +c.revision === 2);
const clone = o => JSON.parse(JSON.stringify(o));

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  // where cut 1's pieces lie (the grey) and where this sheet's own pieces stand, in points
  const plan1 = JSON.parse(CUT1.planJson), edgeAt = yPt => plan1.profile.values[Math.min(plan1.profile.values.length - 1, Math.floor(yPt / plan1.profile.step))];
  ok('0 · the fixture: 29 placed pieces, 17 designs listed; every one of this sheet\'s own pieces stands right of the outline cut 1 left', FX.sheet.placements.length === 29 && FX.sheet.sources.length === 17 && plan1.profile.axis === 'x'
    && FX.sheet.placements.every(p => +p.cxPt > edgeAt(+p.cyPt)), FX.sheet.placements.filter(p => !(+p.cxPt > edgeAt(+p.cyPt))).map(p => p.id));

  /* the master library: one stand-in design per SKU, of the piece's size, at the piece's own path */
  const bySku = new Map(); for (const c of FX.sheet.charms) { const k = String(c.sku).toUpperCase(), w = Math.max(+c.widthPt || 20, 8), h = Math.max(+c.heightPt || 20, 8), had = bySku.get(k); if (!had || had.w * had.h < w * h) bySku.set(k, { sku: c.sku, aiPath: c.aiPath, w, h }); }
  for (const [k, d] of bySku) {
    const bytes = await scene({ page: [d.w + 20, d.h + 20], bodies: [{ shape: 'blob', x: 10, y: 10, w: d.w, h: d.h }] });
    st.blobs.set(d.aiPath, { buf: Buffer.from(bytes), generation: 1, meta: { contentType: 'application/pdf', metadata: { firebaseStorageDownloadTokens: 't' } } });
    st.put('Charm_Master_Index', k, { sku: k, aiPath: d.aiPath, thumbPath: null, blocked: null, sizes: null, conflict: null, engravable: true, widthPt: d.w, heightPt: d.h, charmHash: 'stand-in-' + k.replace(/\W+/g, '').toLowerCase() });
  }
  const seedNow = () => {   // B1: the sheet and its physical sheet as the live cloud has them now
    st.put(SHEETS, S, clone(FX.sheet), false);
    st.put('Charm_Nest_Rose_Stock', STOCK, clone(FX.stock), false);
    for (const k of [...st.docs.keys()].filter(k => k.startsWith(CUTS + '/'))) st.docs.delete(k);
    st.put(CUTS, `${FX.ownCut.sheetId}-3`, clone(FX.ownCut), false); st.put(CUTS, `${CUT1.sheetId}-1`, Object.assign({ stockId: STOCK }, clone(CUT1)), false);
  };
  const seedAside = () => {   // B2: after the restore: its cut 2 set aside, the stock its own again at cut 1's outline (revision 2), no cut recorded
    const rec = clone(FX.sheet); Object.assign(rec, { roseCutAt: null, roseCutRevision: null, rosePlanHash: null, roseRevision: 2, roseReopened: { at: 1795409168979, by: 'Cloud', cut: true, lines: 0 } });
    st.put(SHEETS, S, rec, false);
    st.put('Charm_Nest_Rose_Stock', STOCK, Object.assign(clone(FX.stock), { revision: 2, owner: S, available: false, profileJson: JSON.stringify(plan1.profile), lastCutAt: 1795400303693 }), false);
    for (const k of [...st.docs.keys()].filter(k => k.startsWith(CUTS + '/'))) st.docs.delete(k);
    st.put(CUTS, `${S}-2`, Object.assign({ stockId: STOCK }, clone(CUT2), { setAsideAt: 1795409168979 }), false); st.put(CUTS, `${CUT1.sheetId}-1`, Object.assign({ stockId: STOCK }, clone(CUT1)), false);
  };
  seedNow();

  const exe = process.env.CHROMIUM || ['/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'].find(f => fs.existsSync(f));
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/^https:\/\/firebasestorage\.googleapis\.com\//.test(u)) { const m = /\/o\/(.+)$/.exec(new URL(u).pathname), b = m && st.blobs.get(decodeURIComponent(m[1])); return b ? r.fulfill({ status: 200, contentType: b.meta.contentType || 'application/pdf', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body: b.buf }) : r.fulfill({ status: 404, body: 'no blob' }); }
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => 'Tester'; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.setDefaultTimeout(30000);
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.RoseStock && window.Pool && window.Master, null, { timeout: 60000 });
    const roseGets = () => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'roseGet' && c.body.stockId === STOCK).length;
    /** The plate as drawn: its pieces, and how much of each zone of it is grey: cut 1's pieces (#d8d5d0, inside their own outlines), the metal cut 1 took away around them (#f0eeeb, left of
        cut 1's outline, outside its pieces) and the rest of the sheet, where this sheet's own pieces stand (neither grey). Zones in canvas pixels, from the plate's own scale. */
    const plate = () => page.evaluate(({ shapes, prof }) => {
      const W = SheetWin._W, cv = W.el.base, w = cv.width, h = cv.height, d = cv.getContext('2d').getImageData(0, 0, w, h).data, R = W.R, k = W.k, back = W.face === 'back', X = x => R + (back ? W.st.wPt - x : x) * k;   // (on its Back the plate is mirrored)
      const mk = document.createElement('canvas'); mk.width = w; mk.height = h; const m = mk.getContext('2d'); m.fillStyle = '#000';
      for (const sh of shapes) for (const pts of sh.paths) { m.beginPath(); pts.forEach(([x, y], i) => (i ? m.lineTo(X(x), R + y * k) : m.moveTo(X(x), R + y * k))); m.closePath(); m.fill(); }
      m.globalCompositeOperation = 'destination-out'; m.lineWidth = 3; m.strokeStyle = '#000';   // (their edges, stroked and anti-aliased, are no part of either zone)
      for (const sh of shapes) for (const pts of sh.paths) { m.beginPath(); pts.forEach(([x, y], i) => (i ? m.lineTo(X(x), R + y * k) : m.moveTo(X(x), R + y * k))); m.closePath(); m.stroke(); }
      const md = m.getImageData(0, 0, w, h).data, near = (i, c, t) => Math.abs(d[i] - c[0]) <= t && Math.abs(d[i + 1] - c[1]) <= t && Math.abs(d[i + 2] - c[2]) <= t;
      const z = { piece: [0, 0], away: [0, 0], rest: [0, 0] };
      for (let y = R + 4; y < h - 4; y += 2) for (let x = R + 4; x < w - 4; x += 2) {
        const i = (y * w + x) * 4, xPt = back ? W.st.wPt - (x - R) / k : (x - R) / k, yPt = (y - R) / k, edge = prof.values[Math.min(prof.values.length - 1, Math.floor(yPt / prof.step))];
        const grey = near(i, [216, 213, 208], 6), away = near(i, [240, 238, 235], 4);
        if (md[i + 3] > 200) { z.piece[0]++; if (grey) z.piece[1]++; }
        else if (md[i + 3] === 0 && xPt < edge - 3) { z.away[0]++; if (away) z.away[1]++; }
        else if (xPt > edge + 3) { z.rest[0]++; if (grey || away) z.rest[1]++; }
      }
      const f = a => +(a[1] / Math.max(1, a[0])).toFixed(3), pieces = W.pieces.map(x => ({ id: x.id, sku: x.sku, drawn: !!(x.c && x.c.outline) }));
      return { id: W.id, face: W.face, n: pieces.length, boxes: pieces.filter(x => !x.drawn), cut1Grey: f(z.piece), awayGrey: f(z.away), restGrey: f(z.rest), zones: z, png: cv.toDataURL('image/png') };
    }, { shapes: plan1.shapes, prof: plan1.profile });
    const keep = (name, p) => { if (process.env.OUT && p && p.png) { fs.mkdirSync(process.env.OUT, { recursive: true }); fs.writeFileSync(path.join(process.env.OUT, name + '.png'), Buffer.from(p.png.split(',')[1], 'base64')); } };
    const openIt = async () => {
      await page.evaluate(id => SheetWin.open(id), S);
      await page.waitForFunction(id => { const W = SheetWin._W; return SheetWin.isOpen() && W.rec && W.rec.id === id && W.geom; }, S, { timeout: 60000 });
    };
    const settled = () => until(async () => { const a = await plate(); await new Promise(r => setTimeout(r, 400)); const b = await plate(); return a.cut1Grey === b.cut1Grey && a.boxes.length === b.boxes.length && (b.cut1Grey > 0.5 || Date.now() - t0 > 8000) ? b : null; }, 20000);
    let t0 = Date.now();

    /* ═══ A and B1 · the sheet as it is now ═══ */
    await openIt(); t0 = Date.now();
    const p1 = await settled(); keep('rg-sheet1-now', p1);
    const lost = new Set(FX.sheet.charms.filter(c => !FX.sheet.sources.some(s => s.id === c.sourceId)).map(c => c.id));
    ok('A · the window draws the live record from itself (no run, no live sheet on this page): 29 pieces', !!p1 && p1.id === S && p1.n === 29 && await page.evaluate(() => !SheetWin._W.live), p1 && { id: p1.id, n: p1.n });
    ok('A · every piece is drawn from its design: none a plain box (8 were, of 6 designs the record\'s sources do not list)', !!p1 && p1.boxes.length === 0, p1 && p1.boxes.map(x => x.sku + (lost.has(x.id) ? ' (unlisted)' : '')));
    ok('A · the 8 pieces of unlisted designs are drawn from their own files', !!p1 && lost.size === 8 && [...lost].every(id => !p1.boxes.some(x => x.id === id)));
    ok('B1 · cut 1\'s pieces are grey (#d8d5d0) on the left of the plate, as in the cloud\'s picture', !!p1 && p1.cut1Grey > 0.8, p1 && { cut1Grey: p1.cut1Grey, zones: p1.zones });
    ok('B1 · the metal cut 1 took away is light grey (#f0eeeb) round them', !!p1 && p1.awayGrey > 0.6, p1 && { awayGrey: p1.awayGrey });
    ok('B1 · nothing grey on the rest of the sheet, where its own pieces stand (its own cut is never drawn grey on its own plate)', !!p1 && p1.restGrey < 0.01, p1 && { restGrey: p1.restGrey });
    ok('B1 · its physical sheet was read once (roseGet)', roseGets() === 1, roseGets());

    /* ═══ C · the window's siblings read the record the same way: the order window's Sheet tab (drawOrder) and a card turned over (cardBack.sheet) ═══ */
    const sib = await page.evaluate(async ({ id, rid }) => {
      const cv = document.createElement('canvas'); cv.style.cssText = 'position:fixed;left:0;top:0;width:600px;height:320px'; document.body.appendChild(cv);
      SheetWin.drawOrder(cv, id, rid, { isCurrent: () => true }).catch(() => {});
      const B = await new Promise(res => { let last = null; const t = setTimeout(() => res(last), 25000); SheetWin.cardBack.sheet(id, b => { last = b; if (b && b.pieces && b.pieces.every(x => x.c)) { clearTimeout(t); res(b); } }); });
      for (let i = 0; i < 100 && !(cv._order && cv._order.pieces.every(x => x.c)); i++) await new Promise(r => setTimeout(r, 200));
      const G = cv._order, out = { order: G ? G.pieces.filter(x => !x.c).map(x => x.sku) : null, orderN: G ? G.pieces.length : 0, back: B && B.pieces ? B.pieces.filter(x => !x.c).map(x => x.sku) : null, backN: B && B.pieces ? B.pieces.length : 0 };
      cv.remove(); return out;
    }, { id: S, rid: '4241214165' });
    ok('C · the order window\'s Sheet tab draws every one of the 29 pieces from its design (BUNNY5\'s order: an unlisted design)', sib.orderN === 29 && sib.order && sib.order.length === 0, sib);
    ok('C · a card turned over (its back) draws every one of the 29 pieces from its design', sib.backN === 29 && sib.back && sib.back.length === 0, sib);
    /* ═══ B3 · opened again: drawn at once from what was read, no second read ═══ */
    await page.keyboard.press('Escape'); await page.waitForFunction(() => !SheetWin.isOpen(), null, { timeout: 10000 }).catch(() => {});
    await openIt();
    const p3 = await plate();
    ok('B3 · opened again: the history is there as soon as the pieces are, its stock not read again', p3.cut1Grey > 0.8 && roseGets() === 1, { cut1Grey: p3.cut1Grey, roseGets: roseGets() });
    // B5 · turned to its Back (the window's Front / Back · engraving toggle): the history turns over with the plate, as the Nest card's does
    await page.evaluate(() => { const b = SheetWin._W.el.strip.querySelector('[data-r2=face]'); if (b) b.click(); });
    const p5 = await until(async () => { const q = await plate(); return q.face === 'back' && q.cut1Grey > 0.8 ? q : null; }, 15000) || await plate(); keep('rg-sheet1-back', p5);
    ok('B5 · on its Back the history is mirrored with the plate: cut 1 grey where it lies seen from behind, the rest not grey', p5.face === 'back' && p5.cut1Grey > 0.8 && p5.awayGrey > 0.6 && p5.restGrey < 0.03,   /* (the backs of the pieces are drawn in paler inks) */ { face: p5.face, cut1Grey: p5.cut1Grey, awayGrey: p5.awayGrey, restGrey: p5.restGrey });
    await page.keyboard.press('Escape'); await page.waitForFunction(() => !SheetWin.isOpen(), null, { timeout: 10000 }).catch(() => {});

    /* ═══ B2 · the sheet as it stood after the restore: its cut 2 set aside ═══ */
    seedAside();
    await page.evaluate(id => { try { CharmNestStore.drop('sheetFull', id); } catch (_) {} }, S);
    await openIt(); t0 = Date.now();
    const p2 = await settled(); keep('rg-sheet1-aside', p2);
    ok('B2 · cut set aside (no cut recorded, the stock its own at revision 2): every piece drawn, cut 1\'s pieces and the metal it took grey on the left', !!p2 && p2.boxes.length === 0 && p2.cut1Grey > 0.8 && p2.awayGrey > 0.6, p2 && { boxes: p2.boxes.length, cut1Grey: p2.cut1Grey, awayGrey: p2.awayGrey });
    ok('B2 · nothing grey for the cut it set aside (its own pieces stand there)', !!p2 && p2.restGrey < 0.01, p2 && { restGrey: p2.restGrey });
    ok('B2 · its changed state is read again (a second roseGet)', roseGets() === 2, roseGets());
    await page.keyboard.press('Escape'); await page.waitForFunction(() => !SheetWin.isOpen(), null, { timeout: 10000 }).catch(() => {});

    /* ═══ B4 · a sheet on a stock never cut: nothing to draw, nothing asked ═══ */
    { const rec = clone(FX.sheet); Object.assign(rec, { id: 'rose-fresh-1', roseStockId: 'rgs-fresh-1', roseCutAt: null, roseCutRevision: null, rosePlanHash: null, roseRevision: 0, roseReopened: null, setId: null, draft: true }); st.put(SHEETS, rec.id, rec, false); }
    const asked0 = st.calls.filter(c => c.op === 'roseGet').length;
    await page.evaluate(() => SheetWin.open('rose-fresh-1'));
    await page.waitForFunction(() => { const W = SheetWin._W; return SheetWin.isOpen() && W.rec && W.rec.id === 'rose-fresh-1' && W.geom; }, null, { timeout: 60000 });
    await new Promise(r => setTimeout(r, 1200));
    const p4 = await plate();
    ok('B4 · a sheet on a physical sheet never cut: nothing grey, its stock never asked for', p4.cut1Grey < 0.05 && p4.awayGrey < 0.05 && st.calls.filter(c => c.op === 'roseGet').length === asked0, { cut1Grey: p4.cut1Grey, awayGrey: p4.awayGrey });
    ok('no page error', !errors.length, errors);
  } catch (e) { ok('crashed: ' + (e && e.stack || e), false); }
  finally { await browser.close().catch(() => {}); srv.close(); }
  console.log(failed ? `FAIL sheetwin-rg-sheet1 (${failed})` : 'PASS sheetwin-rg-sheet1'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL sheetwin-rg-sheet1: ' + (e && e.stack || e)); process.exit(1); });
