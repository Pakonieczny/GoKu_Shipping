// SAVEDPRE (Paul, 18 Nov 2026: "Approvals not saved · 4 / 5" on every computer, the oldest 48 to 55 hours). An approval made BEFORE its piece was on a sheet (job.pre) is saved once the piece is on one. A piece on a sheet this
// page holds only as a SAVED card (a sheet of an earlier pass, rebuilt from the cloud: Pool.sheetOf knows only the live pages) counted as "not on a sheet", so its back file was neither written nor retried, for ever.
//   1  a pre job whose piece is on no sheet at all is still unplaced (nothing to save yet)
//   2  a pre job whose piece is on a live page is placed
//   3  a pre job whose piece is on a SAVED sheet (its pool row names the sheet, this page holds the sheet as a card) is placed: its back is written and retried
//   4  a pre job whose pool row was abandoned (taken off its sheet) is still unplaced
// Real charm-nest-1.html over the repo's fake site.   PW_DIR=... CHROMIUM=... node tests/charm-nest/saved-sheet-pre-approval.cjs
const path = require('path');
const { start } = require('./bridge-server.cjs');
(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(__dirname, '../../node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('saved-sheet-pre-approval: playwright-core not found, skipped'); process.exit(0); }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = []; const check = (ok, m) => { if (!ok) fails.push(m); console.log((ok ? '  ✓ ' : '  ✗ ') + m); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', JSON.stringify({ name: 'Ana_M' })); } catch (_) {} });
    const page = await context.newPage(); const errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Engrave && window.Pool && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const r = await page.evaluate(() => {
      const U = Engrave.unplacedPre, pid = '4290000001_42900000011_1', job = c => ({ pre: true, copies: c });
      const out = {};
      out.none = U(job([pid]));
      out.notPre = U({ pre: false, copies: [pid] });
      B.pool.rows.set(pid, { poolId: pid, sheetId: 'gold-saved-1', state: 'written' });
      out.rowButNoSheet = U(job([pid]));
      // the sheet held only as a saved card of this page
      const m = METALS.find(x => x.key === 'gold') || METALS[0], list = pagesOf(m.key);
      list.push({ sheetId: 'gold-saved-1', metal: m.key, charms: [], placements: [], recalled: { poolIds: [pid] }, status: 'saved', draft: false });
      out.savedSheet = U(job([pid]));
      B.pool.rows.set(pid, { poolId: pid, sheetId: 'gold-saved-1', state: 'abandoned' });
      out.abandoned = U(job([pid]));
      list.pop();
      return out;
    });
    console.log('   ', JSON.stringify(r));
    check(r.none === true && r.notPre === false, '1 a pre job whose piece is on no sheet is unplaced; a job that is not pre never is');
    check(r.rowButNoSheet === true, '1b a pool row that names a sheet this page does not hold at all: unplaced (nothing to write to)');
    check(r.savedSheet === false, '3 a pre job whose piece is on a SAVED sheet is placed: its back is written and retried');
    check(r.abandoned === true, '4 a piece taken off its sheet (abandoned row) is unplaced again');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 200));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('saved-sheet-pre-approval: ' + fails.length + ' failed'); process.exit(1); }
  console.log('saved-sheet-pre-approval: ok');
})().catch(e => { console.error(e); process.exit(1); });
