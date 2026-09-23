// Shared setup for the day simulation: the repo's own fake site (tests/charm-nest/bridge-server.cjs) on two origins,
// Chromium with the sorter and the framed Design Station, a master library indexed through the sorter.
const fs = require('fs'), path = require('path'), os = require('os');
const repo = path.join(__dirname, '../../..');
const pwDir = process.env.PW_DIR || path.join(repo, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require(path.join(repo, 'tests/charm-nest/bridge-server.cjs'));
const { buildMaster } = require(path.join(repo, 'tests/charm-nest/fixture-master.cjs'));

async function open(opts = {}) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-sim-'));
  const masters = [];
  for (const [i, m] of (opts.masters || [{ count: 4 }]).entries()) { const p = path.join(tmp, `BRITES-master-${i + 1}.ai`); masters.push({ path: p, fx: await buildMaster(p, Object.assign({ edge: false }, m)) }); }
  const srv = await start({ receipts: opts.receipts || [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  // the shop's clock: a set's date and number follow the browser's day, which ends at midnight in Toronto
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: process.env.SIM_TZ || 'America/Toronto' });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(repo, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ station, sorter, clockOffset }) => {
    if (clockOffset) { const realNow = Date.now.bind(Date); const off = () => (window.__simOffset ?? clockOffset); const RD = Date; Date.now = () => realNow() + off(); }
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200 * 24 * 30)); localStorage.setItem('employee_name', 'Tester'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Tester'); }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  }, { station: stationOrigin, sorter: sorterOrigin, clockOffset: opts.clockOffset || 0 });
  // a simulated clock shared by both pages (and, through simClock.shift, by the fake server's Date.now)
  if (opts.clock) await ctx.clock.install({ time: opts.clock });
  const page = await ctx.newPage();
  const errors = [], consoleLog = [];
  page.on('pageerror', e => errors.push('sorter pageerror: ' + e.message));
  ctx.on('response', r => { if (r.status() >= 400) errors.push('HTTP ' + r.status() + ' ' + r.url().slice(0, 400)); });
  page.on('console', m => { consoleLog.push(m.type() + ': ' + m.text().slice(0, 400)); if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 300)); if (process.env.CN_VERBOSE) console.log('  [page]', m.type(), m.text().slice(0, 200)); });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null);
  await page.evaluate(({ station, settings }) => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 12; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.runMode = 'manual'; s.pullMode = 'dueBy'; const d = new Date(Date.now() + 86400 * 1000 * 20); s.pullDueBy = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; Object.assign(s, settings || {}); CN.saveSettings && CN.saveSettings(); }, { station: stationOrigin, settings: opts.settings });
  const cloud = await page.evaluate(() => CN.S.cloud);
  if (!cloud.ok) throw new Error('sorter does not see the cloud: ' + JSON.stringify(cloud));
  // index the masters through the sorter
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForSelector('#mFile', { state: 'attached' });
  for (const m of masters) {
    await page.evaluate(() => B.master.jobs.clear());
    await page.setInputFiles('#mFile', m.path);
    await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 180000 });
    const mj = await page.evaluate(() => { const j = [...B.master.jobs.values()][0]; return { state: j.state, error: j.error, written: j.written }; });
    if (mj.state !== 'done') throw new Error('master indexing failed: ' + JSON.stringify(mj));
  }
  await page.evaluate(() => B.master.jobs.clear());
  await page.evaluate(() => CN.setMode('design'));
  await page.evaluate(() => DesignLink.ensure());
  const frame = () => page.frames().find(f => f.url().startsWith(stationOrigin));
  return { srv, st, page, ctx, browser, frame, errors, consoleLog, masters, sorterOrigin, stationOrigin, close: async () => { await browser.close(); srv.close(); } };
}
module.exports = { open, repo };
