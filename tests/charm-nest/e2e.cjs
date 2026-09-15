// End-to-end: serve the repo root, open charm-nest-1.html in headless Chromium,
// drop a fixture sheet on the GF card, nest, verify, and check the written .ai.
//   node tests/charm-nest/e2e.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { build } = require('./fixture.cjs');
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.ai': 'application/pdf', '.pdf': 'application/pdf' };

(async () => {
  const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-'));
  const fixture = path.join(tmp, 'TEST-GF-sheet.ai');
  const fx = await build(fixture, +process.env.CN_COUNT || 18);
  const server = http.createServer((req, res) => {
    const u = decodeURIComponent(req.url.split('?')[0]);
    if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
    const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp' });
    fs.createReadStream(f).pipe(res);
  }).listen(0);
  const port = server.address().port;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' || m.type() === 'warning') errors.push(m.type() + ': ' + m.text()); });
  await page.goto(`http://127.0.0.1:${port}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S);
  // settings for a fast, deterministic run
  await page.evaluate(() => { CN.S.settings.budgetS = 60; CN.S.settings.clearancePt = -0.5; CN.S.settings.angleStep = 30; CN.S.settings.naming = 'off'; });
  if (process.env.CN_STOCK) { // e.g. "4.5x4" — a custom per-metal stock to force an overfilled sheet
    const [w, h] = process.env.CN_STOCK.split('x').map(Number);
    await page.evaluate(([w, h]) => { CN.S.settings.stock.gold = [w, h]; CN.S.stockPreset = 'custom'; document.querySelector('#stockSel').value = 'custom'; }, [w, h]);
  }
  // drop the fixture on the GF card via the hidden file input, then assign to gold
  await page.setInputFiles('#fileInput', fixture);
  await page.waitForFunction(() => CN.S.sources.length === 1 && ['ready', 'error'].includes(CN.S.sources[0].state), null, { timeout: 60000 });
  const src = await page.evaluate(() => ({ state: CN.S.sources[0].state, error: CN.S.sources[0].error, charms: CN.S.sources[0].charms.length, metal: CN.S.sources[0].metal, open: CN.S.sources[0].charms.filter(c => c.open).length, distinct: CN.S.sources[0].distinct }));
  console.log('source', src);
  assert.strictEqual(src.state, 'ready', src.error);
  assert.strictEqual(src.charms, fx.charms.length, 'charm count');
  assert.strictEqual(src.metal, 'gold', 'routed by file name');
  if (src.open) console.log('open charms', await page.evaluate(() => CN.S.sources[0].charms.filter(c => c.open).map(c => ({ i: c.index, w: +c.widthPt.toFixed(1), h: +c.heightPt.toFixed(1), members: c.members.length, area: +c.areaPt2.toFixed(0), sub: c.outline.subpaths.length }))));
  assert.strictEqual(src.open, 0, 'no open paths');
  const sat = await page.evaluate(() => CN.S.sheets.gold.sat);
  console.log('saturation before nest', { count: sat.count, needed: Math.round(sat.totalNeeded), usable: Math.round(sat.usable), nEst: sat.nEst, rho: sat.rho.rho, recommend: !!sat.recommend });
  await page.screenshot({ path: path.join(tmp, '1-queued.png') });
  // nest
  await page.click('.sheetCard[data-m=gold] [data-r=nest]');
  await page.waitForFunction(() => CN.S.sheets.gold.status === 'nesting' || ['complete', 'partial'].includes(CN.S.sheets.gold.status));
  let lastPlaced = -1, t0 = Date.now();
  while (Date.now() - t0 < 150000) {
    const st = await page.evaluate(() => ({ status: CN.S.sheets.gold.status, placed: CN.S.sheets.gold.placements.length, stage: CN.S.sheets.gold.stage, trials: CN.S.sheets.gold.trials }));
    if (st.placed !== lastPlaced) { lastPlaced = st.placed; console.log(' ', st.status, st.placed, 'placed ·', st.stage); }
    if (['complete', 'partial'].includes(st.status)) break;
    await page.waitForTimeout(500);
  }
  await page.screenshot({ path: path.join(tmp, '2-nested.png') });
  const result = await page.evaluate(() => { const sh = CN.S.sheets.gold; return { status: sh.status, placed: sh.placements.length, rejects: sh.rejects.length, density: sh.density, endedBy: sh.endedBy, trials: sh.trials, verification: sh.verification, sat: { fullPct: sh.sat.fullPct, free: Math.round(sh.sat.freeFact), nEst: sh.sat.nEst, pocket: sh.sat.pocket }, hasOutputs: !!sh.outputs, layers: sh.placements.map(p => p.layerName) }; });
  console.log('result', JSON.stringify(result, null, 1));
  // true overlap between un-eroded silhouettes (diagnostic for aggressive mode)
  const trueOv = await page.evaluate(() => new Promise(res => { const sh = CN.S.sheets.gold; const job = CN.buildJob(sh); job.clearancePt = 0; const w = new Worker('charm-nest-worker.js'); w.onmessage = e => { if (e.data.type === 'verified') { res(e.data.result); w.terminate(); } }; w.postMessage({ type: 'verify', jobId: 'x', job, placements: sh.placements, res: 6 }); }));
  if (result.verification.render && result.verification.render.overlapDetail) { console.log('render overlap detail', JSON.stringify(result.verification.render.overlapDetail)); const pl = await page.evaluate(() => CN.S.sheets.gold.placements.map(p => ({ n: p.layerName, a: p.angle, x: +p.cxPt.toFixed(1), y: +p.cyPt.toFixed(1), w: +p.wPt.toFixed(1), h: +p.hPt.toFixed(1) }))); console.log('placements', JSON.stringify(pl)); }
  console.log('true overlap (no tolerance):', trueOv.overlapPx, 'px ·', trueOv.overlappingPairs.length, 'pairs · min gap', trueOv.minGapPt, 'pt');
  assert(result.hasOutputs, 'outputs written');
  assert(result.verification && result.verification.geom.ok, 'geometry verification passed');
  assert(result.verification.render && result.verification.render.ok, 'render verification passed: ' + JSON.stringify(result.verification.render));
  if (process.env.CN_ALLOW_PARTIAL) console.log('partial allowed:', result.status, result.rejects, 'rejects');
  else { assert.strictEqual(result.rejects, 0, 'all placed'); assert.strictEqual(result.status, 'complete'); }
  // check the written .ai: a PDF with one OCG per charm + sheet, original path count preserved
  const ai = await page.evaluate(() => Array.from(CN.S.sheets.gold.outputs.ai));
  const aiBytes = Buffer.from(ai); fs.writeFileSync(path.join(tmp, 'out.ai'), aiBytes);
  const PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
  const doc = await PDFLib.PDFDocument.load(aiBytes);
  const ocp = doc.catalog.lookup(PDFLib.PDFName.of('OCProperties'));
  const ocgs = ocp.lookup(PDFLib.PDFName.of('OCGs')).asArray();
  console.log('output', aiBytes.length, 'bytes ·', ocgs.length, 'layers ·', doc.getPageCount(), 'page');
  assert.strictEqual(ocgs.length, result.placed + 1, 'one layer per charm + sheet');
  // report dialog opens and lists every charm
  await page.click('.sheetCard[data-m=gold] [data-r=report]');
  await page.waitForSelector('#dlgReport[open]');
  const tiles = await page.$$eval('#dlgReport .charmTile', els => els.length);
  assert.strictEqual(tiles, fx.charms.length, 'report lists every charm');
  await page.screenshot({ path: path.join(tmp, '3-report.png') });
  const bad = errors.filter(e => !/net::ERR|404|Failed to load resource|favicon|functions/.test(e));
  console.log('console issues', bad);
  assert.strictEqual(bad.length, 0, 'no page errors');
  console.log('artifacts in', tmp);
  await browser.close(); server.close();
  console.log('E2E OK');
})().catch(e => { console.error(e); process.exit(1); });
