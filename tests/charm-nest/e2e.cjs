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
  let fx;
  if (process.env.CN_FILE) { fs.copyFileSync(process.env.CN_FILE, fixture); fx = { charms: new Array(+process.env.CN_EXPECT || 21) }; }   // real artwork: expect CN_EXPECT charms
  else fx = await build(fixture, +process.env.CN_COUNT || 18);
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
  await page.goto(`http://127.0.0.1:${port}/charm-nest-1.html?budget=${process.env.CN_BUDGET || 60}`);
  await page.waitForFunction(() => window.CN && window.CN.S);
  // settings for a fast, deterministic run
  await page.evaluate(() => { CN.S.settings.budgetS = +(new URLSearchParams(location.search).get("budget")) || 60; CN.S.settings.clearancePt = -0.5; CN.S.settings.angleStep = 30; CN.S.settings.naming = 'off'; CN.S.settings.engine = 'solver'; });
  if (process.env.CN_ENGINE === 'ai') {
    // Stand-in planner: a row-packer that pretends to be Claude, so the AI loop's mechanics
    // (moves → exact placement / snap / failure feedback → rounds → outputs) run without the model.
    await page.evaluate(() => {
      CN.S.settings.engine = 'ai'; CN.S.settings.aiRounds = 6; CN.S.settings.snapIn = 0.3;
      CN.setAgent(async (mode, payload) => {
        if (mode !== 'place') return { skipped: 'stub' };
        const W = payload.wIn, H = payload.hIn; let x = 0.15, y = 0.15, rowH = 0; const moves = [];
        const rem = payload.remaining.slice().sort((a, b) => b.areaIn2 - a.areaIn2);
        for (const c of rem) {
          const w = c.wIn + 0.02, h = c.hIn + 0.02;
          if (x + w > W - 0.1) { x = 0.15; y += rowH + 0.02; rowH = 0; }
          if (y + h > H - 0.1) break;
          moves.push({ id: c.id, angle: payload.round % 2 ? 0 : 90, xIn: x + w / 2, yIn: y + h / 2, why: 'row packing, largest first' });
          x += w; rowH = Math.max(rowH, h);
        }
        // deliberately collide the first move with something already placed to exercise snap/failure feedback
        if (payload.placed.length && moves.length) { moves[0].xIn = payload.placed[0].xIn; moves[0].yIn = payload.placed[0].yIn; }
        return { moves, setAside: [], done: moves.length === 0 || payload.round >= 5, summary: `stub round ${payload.round}: ${moves.length} moves`, reasoning: 'stub planner' };
      });
    });
  }
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
  if (!process.env.CN_FILE) {
  // AI grouping review contract: a "fragment" verdict merges a charm into its parent and re-traces the silhouette
  const rev = await page.evaluate(async () => {
    const src = CN.S.sources[0]; const before = src.charms.length; const target = src.charms[0], frag = src.charms[1];
    const tArea = target.areaPt2, tMembers = target.members.length, fMembers = frag.members.length;
    const overview = CN.renderOverview(src);
    const r = await CN.applyGroupingReview(src, [{ index: frag.index, verdict: 'fragment', mergeInto: target.index, note: 'test' }, { index: src.charms[2].index, verdict: 'complete', mergeInto: null, note: '' }]);
    return { before, after: src.charms.length, merged: r.merged, members: target.members.length, expectMembers: tMembers + fMembers, areaGrew: target.areaPt2 > tArea, sheet: CN.S.sheets.gold.charms.length, overviewBytes: overview.length, hasBits: !!target.bits };
  });
  console.log('grouping review', rev);
  assert.strictEqual(rev.after, rev.before - 1, 'fragment removed'); assert.strictEqual(rev.members, rev.expectMembers, 'members merged'); assert(rev.areaGrew, 'silhouette re-traced'); assert.strictEqual(rev.sheet, rev.after, 'sheet queue updated'); assert(rev.overviewBytes > 20000, 'overview rendered');
  fx.charms.pop();   // one fewer charm from here on
  }
  if (process.env.CN_DUMP) { const dump = await page.evaluate(() => CN.S.sources[0].charms.map(c => ({ i: c.index, w: +c.widthPt.toFixed(1), h: +c.heightPt.toFixed(1), area: Math.round(c.areaPt2), m: c.members.length, open: !!c.open })).sort((a, b) => b.area - a.area)); fs.writeFileSync(process.env.CN_DUMP, JSON.stringify(dump, null, 1)); console.log('dumped', dump.length, 'charms to', process.env.CN_DUMP); await browser.close(); server.close(); return; }
  const sat = await page.evaluate(() => CN.S.sheets.gold.sat);
  console.log('saturation before nest', { count: sat.count, needed: Math.round(sat.totalNeeded), usable: Math.round(sat.usable), nEst: sat.nEst, rho: sat.rho.rho, recommend: !!sat.recommend });
  await page.screenshot({ path: path.join(tmp, '1-queued.png') });
  // nest
  await page.click('.sheetCard[data-m=gold] [data-r=nest]');
  await page.waitForFunction(() => CN.S.sheets.gold.status === 'nesting' || ['complete', 'partial'].includes(CN.S.sheets.gold.status));
  let lastPlaced = -1, t0 = Date.now();
  while (Date.now() - t0 < 700000) {
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
  if (process.env.CN_ENGINE === 'ai') { const ag = await page.evaluate(() => CN.S.sheets.gold.log.concat([]).length); console.log('ai engine:', result.endedBy, result.trials, 'round(s)', result.placed, 'placed'); assert(result.placed >= 10, 'AI loop placed pieces'); assert(['rounds', 'complete', 'claude-full'].includes(result.endedBy), 'ended by AI loop: ' + result.endedBy); void ag; }
  if (process.env.CN_ALLOW_PARTIAL) {
    console.log('partial allowed:', result.status, result.rejects, 'rejects');
    // overflow: what did not fit moved to sheet 2 of the same metal, which nests on its own and shows in a tab
    await page.waitForFunction(() => CN.S.sheets.gold.pages.length >= 2 && ['complete', 'partial'].includes(CN.S.sheets.gold.pages[1].status), null, { timeout: 600000 });
    const ov = await page.evaluate(() => { const p = CN.S.sheets.gold; const p2 = p.pages[1]; return { pages: p.pages.length, moved: p.movedOn && p.movedOn.n, rejects1: p.rejects.length, status1: p.status, charms2: p2.charms.length, placed2: p2.placements.length, status2: p2.status, tabs: document.querySelectorAll('.sheetCard[data-m=gold] .shTabs button').length, total: p.pages.reduce((n, x) => n + x.charms.length, 0) }; });
    console.log('overflow', JSON.stringify(ov));
    assert(ov.pages >= 2 && ov.moved > 0 && ov.rejects1 === 0 && ov.charms2 === ov.moved && ov.tabs === ov.pages && ov.total === fx.charms.length, 'extras moved to sheet 2: ' + JSON.stringify(ov));
    await page.evaluate(() => CN.showPage('gold', 1)); await page.screenshot({ path: path.join(tmp, '2b-sheet2.png') });
    const shown = await page.evaluate(() => CN.S.sheets.gold.pages[1].placements.length);
    assert(shown === ov.placed2 && shown > 0, 'sheet 2 tab shows its own placements');
    await page.evaluate(() => CN.showPage('gold', 0));
  }
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
  assert.strictEqual(tiles, process.env.CN_ALLOW_PARTIAL ? result.placed : fx.charms.length, 'report lists every charm (merged fragment gone)');
  await page.screenshot({ path: path.join(tmp, '3-report.png') });
  const bad = errors.filter(e => !/net::ERR|404|Failed to load resource|favicon|functions/.test(e));
  console.log('console issues', bad);
  assert.strictEqual(bad.length, 0, 'no page errors');
  // the finish must run exactly once, and no post-nest inspection may run
  const finishes = await page.evaluate(() => { const t = CN.AG.events.map(e => e.text || '').filter(Boolean); return { won: t.filter(x => /completed the sheet first|reached the ceiling/.test(x)).length, verified: t.filter(x => /^Verified twice/.test(x)).length, inspect: t.filter(x => /inspecting|Sent Claude the finished/.test(x)).length, ended: t.filter(x => /^Search ended/.test(x)).length }; });
  console.log('finish events', JSON.stringify(finishes));
  const sheetsDone = await page.evaluate(() => CN.S.sheets.gold.pages.filter(p => ['complete', 'partial'].includes(p.status)).length);
  assert(finishes.won <= sheetsDone && finishes.verified === sheetsDone && finishes.inspect === 0 && finishes.ended <= sheetsDone, 'single finish per sheet: ' + JSON.stringify(finishes) + ' for ' + sheetsDone + ' sheet(s)');
  console.log('artifacts in', tmp);
  await browser.close(); server.close();
  console.log('E2E OK');
})().catch(e => { console.error(e); process.exit(1); });
