// SAVEDPRE (Paul, 18 Nov 2026: "Approvals not saved · 4 / 5" on every computer, the oldest 48 to 55 hours). An approval made BEFORE its piece was on a sheet (job.pre) is saved once the piece is on one. A piece on a sheet this
// page holds only as a SAVED card (a sheet of an earlier pass, rebuilt from the cloud: Pool.sheetOf knows only the live pages) counted as "not on a sheet", so its back file was neither written nor retried, for ever.
//   1  a pre job whose piece is on no sheet at all is still unplaced (nothing to save yet)
//   2  a pre job whose piece is on a live page is placed
//   3  a pre job whose piece is on a SAVED sheet (its pool row names the sheet, this page holds the sheet as a card) is placed: its back is written and retried
//   4  a pre job whose pool row was abandoned (taken off its sheet) is still unplaced
//   5  STALEBACK: a flag "its back waits to be saved" on a job that is no longer an approval in force (Review, Blocked, None) is cleared, and an approval in force keeps it; the drift count counts only approvals in force
//   6  an approval of a piece the cloud has since taken off its sheet (pool row abandoned) is cleared too
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
    // 5, 6 · stale "waits to be saved" flags
    srv.st.put('Charm_Pool', '4290000009_42900000091_1', { poolId: '4290000009_42900000091_1', orderId: '4290000009', state: 'abandoned', sheetId: null });
    const sb = await page.evaluate(async () => {
      const mk = async (rid, patch) => {
        const ORDER = { receiptId: rid, orderNumber: rid, createTs: 1, updateTs: 2, shipBy: 3, buyer: { name: 'N' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [{ transactionId: rid + '1', listingId: '4458930445', sku: 'MINI_0445', title: 'Mini', quantity: 1, expectedShipDate: 3, metalKey: 'gold', metalLabel: 'GF 14/20', variations: [], personalization: ['A'] }] };
        const key = rid + '_' + rid + '1', row = { key, order: ORDER, line: ORDER.lines[0], arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll(); row.spec.engraveCandidate = true;
        const j = Engrave.ensureJob(row); Object.assign(j, { copies: [key + '_1'], text: 'A', lines: ['A'], lineInput: ['A'], backPending: true }, patch); return j;
      };
      const stale = await mk('4290000007', { state: 'review', approvedAt: Date.now() - 1e8 });
      const live = await mk('4290000008', { state: 'approved', approvedAt: Date.now() - 1e8, fit: { size: 10, centre: [0, 0], angle: 0 }, view: {} });
      const gone = await mk('4290000009', { state: 'approved', approvedAt: Date.now() - 1e8, fit: { size: 10, centre: [0, 0], angle: 0 }, view: {} });
      const n = await Engrave.settleStuckBacks();
      return { n, stale: !!stale.backPending, live: !!live.backPending, gone: !!gone.backPending };
    });
    console.log('   settle:', JSON.stringify(sb));
    check(sb.stale === false, '5a a flag on a job that went back to Review is cleared (not an approval in force)');
    check(sb.live === true, '5b an approval in force keeps its flag: it still waits to be saved');
    check(sb.gone === false, '6 an approval of a piece the cloud has taken off its sheet (abandoned) is cleared');
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
