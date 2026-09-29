// NestFocus, the Nest tab's side of the Send to Sheet tour (Paul, 29 Sep): on its own, with a stand-in sheet of ring
// charms on the Gold card (its second page, the first page shown) and the stage scrolled away from it. Checks that
// open() shows the page, glides the card into view, spotlights it (the others ease back and dim) under its caption, and
// holds the order's pieces back (a gold wash where each stands, the rest of the sheet as it was); that the spots are
// the pieces' own places on the drawn sheet (centre, size, turn); that land() pops each piece in with its ring, laid
// into the canvas already drawn (the sheet never painted again while it lands) and matching the finished picture; that
// the files' picture never holds anything back; that close() and the safety timeout always show everything and undim;
// and that waiting() says where a metal's waiting pieces stand. Screenshots to /mnt/project-files/plans/tour/b-*.png.
//   node tests/charm-nest/tour-b.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const SHOTS = '/mnt/project-files/plans/tour';
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
}).listen(0);

/** Gold: page 1 (a few charms, shown) and page 2 (28 ring charms, some turned, two to an order). */
async function setup(page) {
  await page.evaluate(async () => {
    const { S } = CN; const MM = 72 / 25.4;
    CharmNestPDF.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke();
      const [x2, y2] = tx(c.centerPt[0] + c.rMm * MM * .55, c.centerPt[1]); ctx.beginPath(); ctx.arc(x2, y2, Math.max(1.5, .9 * MM * k), 0, 7); ctx.fillStyle = '#1d4fb8'; ctx.fill(); };   // a blue dot off-centre shows the turn
    const realPath = CharmNestPDF.pathToCanvas;
    CharmNestPDF.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
    CharmNestPDF.cutLinesOf = () => [];
    const mk = (i, rid, rMm) => ({ id: 'g' + i, name: `${rid} · CHARM-${i}`, poolId: `${rid}_${900 + i}_1`, order: rid, sourceId: 's', centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
    const p1 = S.sheets.gold.pages[0]; p1.charms = []; p1.placements = [];
    for (let i = 0; i < 4; i++) { const c = mk(100 + i, '3811111100', 4.5); p1.charms.push(c); p1.placements.push({ id: c.id, cxPt: (12 + i * 14) * MM, cyPt: 12 * MM, angle: 0 }); }
    p1.status = 'complete';
    const p2 = CN.addPage('gold'); let i = 0;
    for (let row = 0; row < 4; row++) for (let col = 0; col < 8; col++) {
      const rMm = 4.2 + ((row * 3 + col * 5) % 4) * .45, x = 7 + col * 12 + (row % 2) * 5.5, y = 7 + row * 11.8;
      if (x + rMm > 99 || y + rMm > 49) continue;
      const c = mk(i, String(3812345600 + Math.floor(i / 2)), rMm); p2.charms.push(c);
      p2.placements.push({ id: c.id, cxPt: x * MM, cyPt: y * MM, angle: (i * 37) % 360, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
      i++;
    }
    p2.status = 'complete'; p2.sheetId = 'sheet-gold-2'; p2.seq = 1;
    CN.setMode('nest'); CN.showPage('gold', 0);
    await new Promise(r => setTimeout(r, 300));
    // the stage scrolled so the Gold card stands partly out of sight
    const stage = document.getElementById('stage'); stage.scrollTop = stage.scrollHeight;
    await new Promise(r => setTimeout(r, 200));
  });
}
/** Red (a charm's own line) pixels inside a piece's place on the card's canvas. */
const redIn = (page, charmId, from) => page.evaluate(([id, from]) => {
  const sh = CN.S.sheets.gold.pages.find(p => p.charms.some(c => c.id === id)), cv = from === 'full' ? window.__full : sh.el.querySelector('[data-r="canvas"]');
  const p = sh.placements.find(q => q.id === id), c = sh.charms.find(q => q.id === id), v = sh._view, rr = c.widthPt / 2 * v.k;
  const x0 = Math.round(v.R + p.cxPt * v.k - rr * 1.08), y0 = Math.round(v.R + p.cyPt * v.k - rr * 1.08), w = Math.round(rr * 2.16);
  const d = cv.getContext('2d').getImageData(x0, y0, w, w).data; let n = 0;
  for (let j = 0; j < d.length; j += 4) if (d[j] > 170 && d[j + 1] < 90 && d[j + 2] < 90) n++;
  return n;
}, [charmId, from || '']);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage({ viewport: { width: 1400, height: 760 } });
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.NestFocus && window.Gate);
    await setup(page);
    fs.mkdirSync(SHOTS, { recursive: true });
    const ids = ['3812345602_904_1', '3812345602_905_1', '3812345605_911_1'], charms = ['g4', 'g5', 'g11'];

    // the whole sheet drawn once as the files see it, and each piece's line there before the flight
    const before = await page.evaluate(() => { const st = document.getElementById('stage'), card = document.querySelector('.sheetCard[data-m="gold"]'); return { scroll: st.scrollTop, top: card.getBoundingClientRect().top, active: CN.S.sheets.gold.active }; });
    assert(before.active === 0 && before.scroll > 0, 'set up: page 1 shown, the stage scrolled: ' + JSON.stringify(before));

    /* 1 · open: the page, the glide, the spotlight, the caption, the pieces held back */
    await page.evaluate(ids => {
      window.__paints = 0; const real = window.paintPreview;
      window.paintPreview = function (cv, sh, clean, R) { if (cv && cv.dataset && cv.dataset.r === 'canvas') window.__paints++; return real.apply(this, arguments); };
      window.__opening = NestFocus.open('sheet-gold-2', { poolIds: ids, caption: 'GF · Sheet 2' }).then(f => { window.__f = f; return f.spots; });
    }, ids);
    await page.waitForTimeout(260);
    await page.screenshot({ path: `${SHOTS}/b-1-opening.png` });
    const spots = await page.evaluate(() => window.__opening);
    const o = await page.evaluate(() => {
      const sh = CN.S.sheets.gold.pages[1], card = document.querySelector('.sheetCard[data-m="gold"]'), cv = card.querySelector('[data-r="canvas"]'), r = cv.getBoundingClientRect(), v = sh._view, s = r.width / cv.width;
      const others = [...document.querySelectorAll('#sheets>.sheetCard')].filter(c => c !== card).map(c => ({ op: +getComputedStyle(c).opacity, tf: getComputedStyle(c).transform }));
      const cap = card.querySelector('.nfCap'), cr = card.getBoundingClientRect(), stage = document.getElementById('stage').getBoundingClientRect();
      const want = ['g4', 'g5', 'g11'].map(id => { const p = sh.placements.find(q => q.id === id), c = sh.charms.find(q => q.id === id); return { x: r.left + (v.R + p.cxPt * v.k) * s, y: r.top + (v.R + p.cyPt * v.k) * s, d: c.widthPt * v.k * s, rot: p.angle }; });
      return { active: CN.S.sheets.gold.active, focus: card.classList.contains('nfFocus'), others, cap: cap && cap.textContent, capOp: cap ? +getComputedStyle(cap).opacity : 0,
        cardTop: cr.top, capTop: cap ? cap.getBoundingClientRect().top : -1, stageTop: stage.top, stageBottom: stage.bottom, pvBottom: card.querySelector('.shPreviewWrap').getBoundingClientRect().bottom, want, hidden: sh._tourHide ? [...sh._tourHide] : [] };
    });
    assert(o.active === 1, 'the card shows the sheet the order went on');
    assert(o.focus && o.others.length && o.others.every(x => x.op < .45 && /matrix\(0\.96/.test(x.tf)), 'the sheet is spotlit, the other cards eased back and dimmed: ' + JSON.stringify(o.others));
    assert(o.cap === 'GF · Sheet 2' && o.capOp > .9, 'its caption shows: ' + o.cap + ' ' + o.capOp);
    assert(o.capTop >= o.stageTop && o.cardTop >= o.stageTop && o.pvBottom <= o.stageBottom, 'the card glided into view, its caption and picture whole: ' + JSON.stringify(o));
    assert.deepStrictEqual(o.hidden.sort(), charms.slice().sort(), 'the order\'s pieces are held back');
    assert(spots.length === 3 && spots.every(s => s.placed), 'a spot for each piece');
    spots.forEach((s, i) => { const w = o.want[i], cx = s.rect.left + s.rect.width / 2, cy = s.rect.top + s.rect.height / 2;
      assert(s.poolId === ids[i] && Math.abs(cx - w.x) < 1 && Math.abs(cy - w.y) < 1 && Math.abs(s.rect.width - w.d) < 1 && s.rot === w.rot && s.scale > 0, `spot ${i} is the piece's own place: ${JSON.stringify(s)} vs ${JSON.stringify(w)}`); });
    for (const id of charms) assert((await redIn(page, id)) === 0, `${id} is held back (only its wash shows)`);
    assert((await redIn(page, 'g6')) > 20, 'the rest of the sheet shows as it was');
    const files = await page.evaluate(() => { const sh = CN.S.sheets.gold.pages[1], cv = sh.el.querySelector('[data-r="canvas"]'), f = document.createElement('canvas'); f.width = cv.width; f.height = cv.height; paintPreview(f, sh, true, sh._view.R); window.__full = f; return 1; });
    assert((await redIn(page, 'g4', 'full')) > 20, 'the files\' picture never holds a piece back');
    await page.screenshot({ path: `${SHOTS}/b-2-spotlit.png` });

    /* 2 · land: the pop and the ring, laid into the canvas drawn (never painted again), as the finished picture */
    await page.evaluate(() => { window.__paints = 0; window.__gaps = []; let t = performance.now(); window.__run = true; const tick = n => { window.__gaps.push(n - t); t = n; if (window.__run) requestAnimationFrame(tick); }; requestAnimationFrame(tick); window.__landing = window.__f.land('3812345602_904_1'); });
    await page.waitForTimeout(200);
    const mid = await page.evaluate(() => { const r = document.querySelector('.sheetCard[data-m="gold"] .shPreviewWrap').getBoundingClientRect(); return { piece: document.querySelectorAll('#motionLayer .nfPiece').length, ring: document.querySelectorAll('#motionLayer .nfRing').length, clip: { x: r.left, y: r.top, width: r.width, height: r.height } }; });
    await page.screenshot({ path: `${SHOTS}/b-3-landing.png`, clip: mid.clip });   // mid-landing: the piece at its pop, its ring spreading
    assert(mid.piece === 1 && mid.ring === 1, 'mid-landing: the piece pops with its ring: ' + JSON.stringify(mid));
    await page.evaluate(() => window.__landing);
    assert((await redIn(page, 'g4')) > 20 && (await redIn(page, 'g5')) === 0, 'the landed piece is drawn in, the next still held back');
    await page.evaluate(() => Promise.all([window.__f.land('3812345602_905_1'), new Promise(r => setTimeout(r, 90)).then(() => window.__f.land('3812345605_911_1'))]));
    await page.waitForTimeout(900);
    const after = await page.evaluate(() => { window.__run = false; const paints = window.__paints, sh = CN.S.sheets.gold.pages[1], cv = sh.el.querySelector('[data-r="canvas"]'), f = document.createElement('canvas'); f.width = cv.width; f.height = cv.height; const view = sh._view; paintPreview(f, sh, false, view.R); sh._view = view;   // the card's own picture, drawn whole once after the landing
      const a = cv.getContext('2d').getImageData(0, 0, cv.width, cv.height).data, b = f.getContext('2d').getImageData(0, 0, cv.width, cv.height).data;
      let diff = 0; for (let j = 0; j < a.length; j += 4) if (Math.abs(a[j] - b[j]) + Math.abs(a[j + 1] - b[j + 1]) + Math.abs(a[j + 2] - b[j + 2]) > 24) diff++;
      const g = window.__gaps.slice(2).sort((x, y) => x - y);
      return { paints, diff, left: document.querySelectorAll('#motionLayer .nfPiece,#motionLayer .nfRing').length, hidden: sh._tourHide ? sh._tourHide.size : 0, p95: g[Math.floor(g.length * .95)] || 0, max: g.at(-1) || 0 };
    });
    for (const id of charms) assert((await redIn(page, id)) > 20, `${id} landed`);
    assert(after.paints === 0, 'the sheet was never painted again while its pieces landed: ' + after.paints);
    assert(after.diff < 40, 'the landed sheet matches the finished picture: ' + after.diff + ' pixels differ');
    assert(after.left === 0 && after.hidden === 0, 'nothing of the landing is left: ' + JSON.stringify(after));
    await page.screenshot({ path: `${SHOTS}/b-4-landed.png` });

    /* 3 · close: undimmed, the caption gone */
    await page.evaluate(() => window.__f.close());
    const c = await page.evaluate(() => ({ on: document.getElementById('sheets').className, cap: !!document.querySelector('.nfCap'), glow: !!document.querySelector('.nfGlow'), op: [...document.querySelectorAll('#sheets>.sheetCard')].map(x => +getComputedStyle(x).opacity), open: NestFocus.isOpen() }));
    assert(!/nf/.test(c.on) && !c.cap && !c.glow && c.op.every(x => x === 1) && !c.open, 'closed: undimmed, its caption gone: ' + JSON.stringify(c));

    /* 4 · close before any landing, and the safety timeout, show everything */
    await page.evaluate(ids => NestFocus.open('sheet-gold-2', { poolIds: ids }).then(f => f.close()), ids);
    for (const id of charms) assert((await redIn(page, id)) > 20, `close with nothing landed shows ${id}`);
    await page.evaluate(ids => NestFocus.open(null, { poolIds: ids, safeMs: 900 }).then(f => { window.__g = f; }), ids);
    assert((await redIn(page, 'g5')) === 0, 'found by its pieces alone (no sheet id), and held back');
    await page.waitForTimeout(1700);
    for (const id of charms) assert((await redIn(page, id)) > 20, `the safety timeout shows ${id}`);
    assert(!(await page.evaluate(() => NestFocus.isOpen() || /nf/.test(document.getElementById('sheets').className))), 'and undims');
    const none = await page.evaluate(() => NestFocus.open('no-such-sheet', { poolIds: ['nope'] }).then(f => ({ card: f.card, n: f.spots.length })));
    assert(none.card === null && none.n === 0, 'a sheet not found gives no spots, for the tour\'s own fallback');

    /* 5 · waiting: where Silver's waiting pieces stand */
    const w = await page.evaluate(async () => { const x = NestFocus.waiting('silver'); await x.done; const r = x.el.getBoundingClientRect(); return { rect: x.rect, now: { left: r.left, top: r.top }, h: innerHeight }; });
    assert(w.rect && Math.abs(w.rect.top - w.now.top) < 2 && w.rect.top > 0 && w.rect.top < w.h, 'waiting(): the place, where it stands once in view: ' + JSON.stringify(w));

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log(`NestFocus OK · open glides the card into view and spotlights it with its caption, the order's pieces held back as a gold wash; spots are the pieces' own places (centre, size, turn); land pops each in with its ring, laid into the drawn canvas (0 repaints, ${after.diff} px from the finished picture); close and the safety timeout show everything; waiting() places · frames while landing p95 ${after.p95.toFixed(1)} ms, max ${after.max.toFixed(1)} ms`);
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
