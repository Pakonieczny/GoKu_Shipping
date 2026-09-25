// The sandbox's orders carry the real Etsy order numbers (Paul, 25 Sep 2026), so what the sorter keeps in the browser per
// order is kept per side: a Team draft typed in the sandbox never shows on the real order in production, and a sandbox
// message still in the outbox is sent to the sandbox's record only, never by a production tab to the real one.
//   node tests/charm-nest/sandbox-own-drafts.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
(async () => {
  const srv = await start({ receipts: [] });
  const { sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext();
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; }, { sorter: sorterOrigin });
  const page = await ctx.newPage(); const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  const boot = () => page.waitForFunction(() => window.CN && window.TeamMail && window.Sandbox, null, { timeout: 60000 });
  const mode = async on => { await page.evaluate(([on, station]) => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: on ? 'on' : 'off', sandboxStream: 'off', dsOrigin: station, pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s)); }, [on, stationOrigin]); await page.reload(); await boot(); };
  await page.goto(`${sorterOrigin}/charm-nest-1.html`); await boot();
  await mode(false);
  await page.evaluate(() => TeamMail.setDraft('4170000001', 'production words'));
  await mode(true);
  const sb = await page.evaluate(() => ({ sandbox: WORKSPACE_SANDBOX, draft: TeamMail.draftOf('4170000001') }));
  assert(sb.sandbox && sb.draft === '', 'the sandbox does not see the real order\'s Team draft: ' + JSON.stringify(sb));
  await page.evaluate(() => TeamMail.setDraft('4170000001', 'sandbox words'));
  // a sandbox message queued while the record cannot be reached stays the sandbox's: a production tab never sends it
  const posts = []; await page.route(/firebaseOrders/, r => { if (r.request().method() === 'POST') { posts.push(r.request().url()); return r.abort(); } return r.continue(); });
  await page.evaluate(() => TeamMail.queue('4170000001', 'sandbox message', 'Tester'));
  await page.waitForTimeout(800);
  assert(posts.length >= 1 && posts.every(u => /sandbox=1/.test(u)), 'the sandbox tab posts its message to the sandbox record only: ' + posts.join(' '));
  await page.unroute(/firebaseOrders/);
  const prodPosts = []; await page.route(/firebaseOrders/, r => { if (r.request().method() === 'POST') prodPosts.push(r.request().url()); return r.continue(); });
  await mode(false);
  await page.waitForTimeout(2500);   // the outbox flushes 1.5 s after load
  const pr = await page.evaluate(() => ({ sandbox: WORKSPACE_SANDBOX, draft: TeamMail.draftOf('4170000001'), pending: TeamMail.pending('4170000001').length, keys: Object.keys(localStorage).filter(k => /^cn\.(team|mail)\./.test(k)).sort() }));
  console.log(pr, 'production posts:', prodPosts.length);
  assert(!pr.sandbox && pr.draft === 'production words' && pr.pending === 0, 'production keeps its own draft and sees no sandbox message');
  assert.strictEqual(prodPosts.length, 0, 'production never sends the sandbox\'s queued message');
  assert(pr.keys.includes('cn.team.drafts') && pr.keys.includes('cn.team.drafts:sandbox') && pr.keys.includes('cn.team.outbox:sandbox'), 'each side has its own keys');
  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('sandbox own drafts OK: Team drafts and the outbox are kept per side, production never sends a sandbox message');
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
