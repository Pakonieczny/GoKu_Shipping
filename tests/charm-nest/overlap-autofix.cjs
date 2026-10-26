// OVERLAPFIX (Paul, 25 Oct 2026): "minor overlap between some charms on a sheet … cannot hold the sheet and the sheet must be auto fixed by the system … a tiny readjustment
// of the position should resolve the tiny overlap, but if that is not possible, then shrinking the charm by the smallest needed amount".
// The real page: a fixture sheet is nested, one charm is pushed into its nearest neighbour until their outlines overlap by about 1 pt, and the sheet is written as it stands
// (writeAsPlaced, the nest's own finish). The finish must clear the overlap by itself: verified, with the touched charm moved a point or two at most and shrunk only if it had to be.
//   CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/overlap-autofix.cjs
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const { chromium } = require(path.join(root, 'node_modules', 'playwright-core'));
const { build } = require('./fixture.cjs');
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.ai': 'application/pdf', '.pdf': 'application/pdf' };

(async () => {
  const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-ovf-'));
  const fixture = path.join(tmp, 'TEST-GF-sheet.ai');
  await build(fixture, 8);
  const server = http.createServer((req, res) => {
    const u = decodeURIComponent(req.url.split('?')[0]);
    if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
    const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp' });
    fs.createReadStream(f).pipe(res);
  }).listen(0);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
  const errors = []; page.on('pageerror', e => errors.push(e.message));
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html?budget=20`);
    await page.waitForFunction(() => window.CN && window.CN.S);
    await page.evaluate(() => { const s = CN.S.settings; s.budgetS = 20; s.clearancePt = -0.5; s.angleStep = 30; s.naming = 'off'; s.engine = 'solver'; s.autoNest = 'off'; s.stock.gold = [181 / 25.4, 153.2 / 25.4]; CN.S.stockPreset = 'custom'; document.querySelector('#stockSel').value = 'custom'; });
    await page.setInputFiles('#fileInput', fixture);
    await page.waitForFunction(() => CN.S.sources.length === 1 && CN.S.sources[0].state === 'ready', null, { timeout: 60000 });
    assert(await page.evaluate(() => typeof CharmNestOverlapFix === 'object' && typeof CharmNestOverlapFix.plan === 'function'), 'the overlap fixer is loaded on the page');
    if (await page.evaluate(() => CN.S.sheets.gold.status === 'ready')) await page.click('.sheetCard[data-m=gold] [data-r=nest]');
    // done: every turn of the batch placed, nothing waiting or saving, for 5 s
    for (let settled = 0, t0 = Date.now(); settled < 10; ) {
      assert(Date.now() - t0 < 900000, 'the nest finishes');
      const st = await page.evaluate(() => { const sh = CN.S.sheets.gold; return ['complete', 'partial'].includes(sh.status) && !(sh.feedWait || []).length && !(sh.persisted && !sh.persistedDone); });
      settled = st ? settled + 1 : 0; await page.waitForTimeout(500);
    }
    const before = await page.evaluate(() => ({ n: CN.S.sheets.gold.placements.length, ok: !!CN.S.sheets.gold.verification?.ok }));
    console.log('nested', before);
    assert(before.n >= 4 && before.ok, 'a verified sheet to start from');

    // push the charm nearest to another into it: step 0.25 pt along the line of centres until the geometry check flags them at the sheet's own clearance (past the allowed stroke share), then 1 pt more
    const pushed = await page.evaluate(async () => {
      const sh = CN.S.sheets.gold, P = sh.placements;
      let best = null; for (let i = 0; i < P.length; i++) for (let j = 0; j < P.length; j++) if (i !== j) { const d = Math.hypot(P[i].cxPt - P[j].cxPt, P[i].cyPt - P[j].cyPt); if (!best || d < best.d) best = { i, j, d }; }
      const A = P[best.i], B = P[best.j], ux = (A.cxPt - B.cxPt) / best.d, uy = (A.cyPt - B.cyPt) / best.d, start = { cxPt: B.cxPt, cyPt: B.cyPt };
      const touch = () => new Promise(res => { const job = CN.buildJob(sh); const w = new Worker('charm-nest-worker.js'); w.onmessage = e => { if (e.data.type === 'verified') { res(e.data.result.overlapPx > 0); w.terminate(); } }; w.postMessage({ type: 'verify', jobId: 'probe', job, placements: sh.placements, res: 6 }); });
      let steps = 0; while (!(await touch()) && steps < 400) { B.cxPt += ux * 0.25; B.cyPt += uy * 0.25; steps++; }
      B.cxPt += ux * 1; B.cyPt += uy * 1;
      return { a: A.id, b: B.id, steps, pushedPt: +Math.hypot(B.cxPt - start.cxPt, B.cyPt - start.cyPt).toFixed(2) };
    });
    console.log('pushed', pushed);
    assert(pushed.steps < 400, 'the two charms were brought into contact');

    const out = await page.evaluate(async () => {
      const sh = CN.S.sheets.gold; delete sh.overlapFix;
      await window.writeAsPlaced(sh);
      for (let k = 0; k < 600 && ['nesting', 'finishing', 'queued'].includes(sh.status); k++) await new Promise(r => setTimeout(r, 200));
      return { status: sh.status, ok: !!sh.verification?.ok, render: sh.verification?.render && { ok: sh.verification.render.ok, overlapPx: sh.verification.render.overlapPx }, fix: sh.overlapFix || null, log: sh.log.slice(-6) };
    });
    console.log('after the write', JSON.stringify(out, null, 1));
    assert(out.fix, 'the finish ran the overlap fix: ' + JSON.stringify(out.log));
    assert(out.ok && out.fix.ok, 'the sheet is verified after the fix, nothing holds it: ' + JSON.stringify(out.render));
    for (const c of out.fix.charms) {
      assert(c.movedPt <= 3.01, `${c.name} moved only a little (${c.movedPt} pt)`);
      assert(c.shrinkPct <= 6.01, `${c.name} shrunk only if needed, and only a little (${c.shrinkPct} %)`);
    }
    assert(out.fix.charms.length <= 2, 'only the charms in the overlap were touched');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('PASS: overlap-autofix: a ~1 pt overlap is cleared by the finish itself (smallest nudge, smallest shrink only if needed) and the sheet is verified');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
