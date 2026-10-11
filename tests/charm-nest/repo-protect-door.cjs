// Charm Repo protection, the page against the REAL password door (netlify/functions/_charmNestRepoAuth.js, REPOAUTH) on the in-memory Firestore:
// the page is served locked, the server is strict (st.repoStrict), the password is typed into the real dialog, and the real repoUnlock / masterPutIndex /
// masterPatch / masterDownload / masterRemoveFile answer. It proves the two halves agree on the wire (names, token kinds, the 401 repo-locked refusal).
// The only password is the stand-in operator's, made fresh in bridge-server.cjs (REPO_TEST); nothing real is read, written or printed.
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/repo-protect-door.cjs      (skipped without a browser)
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, Timestamp, REPO_TEST } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const WRONG = 'w' + crypto.randomBytes(9).toString('hex');
const hash = k => crypto.createHash('sha256').update('repo-door-' + k).digest('hex');

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-repodoor-'));
  const masterPath = path.join(tmp, 'DOOR-master.ai');
  const fx = await buildMaster(masterPath, { count: 3, edge: false });
  const srv = await start({ receipts: [], repoSeed: false });
  const { st, sorterOrigin } = srv; st.repoStrict = true;
  const FILES = [hash(1), hash(2)];
  FILES.forEach((h, i) => st.put('Charm_Master_Files', h, { masterHash: h, name: 'DOOR-' + i + '.ai', indexedAt: Timestamp.fromMillis(Date.UTC(2026, 9, 8, 12 + i)), charms: 1, labelled: 1, skus: ['DR-' + i], indexedBy: 'browser' }));
  ['DR-0', 'DR-1'].forEach((sku, i) => st.put('Charm_Master_Index', sku, { sku, masterHash: FILES[i], widthPt: 40, heightPt: 50, areaPt2: 1500, members: 3, holes: 1, upAngle: 0, aiPath: `charmnest/master/${sku}.ai`, aiUrl: srv.blobUrl(`charmnest/master/${sku}.ai`), thumbPath: `charmnest/master/${sku}.png`, indexedAt: Timestamp.fromMillis(Date.UTC(2026, 9, 8, 12)) }));
  st.blobs.set('charmnest/master/DR-0.ai', { buf: Buffer.from('%PDF-1.4 test'), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });

  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 200, contentType: 'application/octet-stream', body: 'x' }));
  const calls = [];   // what reached the real handlers: operation, whether a token went with it, the answer's status (never a password or a token)
  for (const fn of ['charmNestLibrary', 'charmNestOutput']) {
    const h = st.handlers[fn], real = h.handler;
    h.handler = async ev => { const out = await real(ev); let b = {}; try { b = JSON.parse(ev.body || '{}') || {}; } catch (_) { /* not JSON */ } calls.push({ fn, op: b.op, token: !!b.repoToken, action: b.action || null, viaSession: !!b.sessionToken, pw: !!b.password, status: out.statusCode || 200 }); return out; };
  }
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {}
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    window.__attempts = []; window.addEventListener('repo-auth-attempt', e => window.__attempts.push(e.detail));
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.Master && window.CharmNestRepoLock && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; });
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length >= 2, null, { timeout: 30000 });
  const typeAndGo = async pw => { const f = page.locator('dialog.repoDlg[open] #repoPw'); await f.fill(pw); await f.press('Enter'); };
  const protectedOps = ['masterPutIndex', 'masterPutFile', 'masterPatch', 'masterDownload', 'masterRemoveFile'];
  const asked = op => calls.filter(c => c.op === op);

  // 1 · the page reads and shows the library with no password; nothing was refused so far
  ok(calls.some(c => c.op === 'masterList' && c.status === 200) && !calls.some(c => c.status === 401), 'the library loads from the real handlers with no password and no refusal');
  ok(await page.evaluate(() => CN.S.cloud.ok === true), 'the sorter sees the (in-memory) cloud');

  // 2 · add a master file: a wrong password, then the right one; the real door lets the whole add through on ONE unlock
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=upload]');
  await typeAndGo(WRONG);
  await page.waitForFunction(() => /not right/i.test((document.querySelector('dialog.repoDlg .repoMsg') || {}).textContent || ''), null, { timeout: 15000 });
  ok(await page.evaluate(() => document.querySelector('#repoPw').value === '' && !!document.querySelector('dialog.repoDlg[open]')), 'a wrong password: the real door said wrong, the dialog stays, the field is clear');
  ok(!calls.some(c => protectedOps.includes(c.op) && c.status === 200), 'and nothing protected ran');
  await typeAndGo(REPO_TEST.password);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 15000 });
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()].pop(); return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  const job = await page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, error: j.error, written: j.written }; });
  ok(job.state === 'done' && job.written === fx.charms.length, 'the right password: the real door let the add through (' + job.written + ' SKUs written)');
  ok(asked('repoUnlock').length === 2 && asked('repoUnlock').every(c => c.action === 'upload' && c.status === 200), 'two unlock tries in all (one wrong, one right), both answered by the real door');
  ok(asked('masterPutIndex').length === 1 && asked('masterPutFile').length === 1 && [...asked('masterPutIndex'), ...asked('masterPutFile')].every(c => c.token && c.status === 200), 'the index and the file record were written once each, with the token');
  const uploads = calls.filter(c => c.fn === 'charmNestOutput' && ['sign', 'put', 'finalize'].includes(c.op));
  ok(uploads.length > 0 && uploads.every(c => c.token && c.status === 200), 'every one of the ' + uploads.length + ' uploads carried the token and was accepted');
  ok(st.list('Charm_Master_Index').filter(e => /^BR-TST/.test(e._id)).length === fx.charms.length, 'the new SKUs are in the (in-memory) index');
  ok((await page.evaluate(() => window.__attempts)).map(a => a.reason).join() === 'wrong,ok', 'the page told the Alert about both tries (wrong, ok)');

  // 3 · a change (the upload token serves it), a download (its own password), a removal (single use, asks every time)
  const before = asked('repoUnlock').length;
  await page.locator('input[data-up]').first().click();
  await page.evaluate(() => { const i = document.querySelector('input[data-up]'); i.value = '33'; i.dispatchEvent(new Event('change', { bubbles: true })); });
  ok(asked('masterPatch').length >= 1 && asked('masterPatch').every(c => c.token && c.status === 200), 'a change: masterPatch went with the token and the real door accepted it, with no new password (' + asked('repoUnlock').length + ' unlocks, ' + before + ' before)');
  ok(asked('repoUnlock').length === before, 'the upload token covered the change (no second dialog)');

  await page.evaluate(() => { window.__dl = []; window.download = (b, name) => window.__dl.push(name); CharmNestAssets.bytes = async () => new Uint8Array([1, 2, 3]); });
  await page.locator('[data-dl]').first().click();
  await page.waitForFunction(() => window.__dl.length === 1, null, { timeout: 15000 });
  ok(!(await page.evaluate(() => !!document.querySelector('dialog.repoDlg'))) && asked('repoUnlock').length === before, 'a download needs no second password: the one unlock (a session, since Paul\'s 11 Oct words) covers it');
  ok(asked('masterDownload').length === 1 && asked('masterDownload')[0].token && asked('masterDownload')[0].status === 200, 'the real masterDownload answered once, with the session token, and the file was saved');

  // a removal still asks its confirm sentence every time, but takes a single-use token from the session with no password
  const filesNow = await page.evaluate(() => B.master.files.length);
  await page.evaluate(() => { document.getElementById('mHist').open = true; const r = document.querySelector('#mFiles .masterFileRow'); r.closest('.masterDay').open = true; r.open = true; });
  await page.locator('[data-rm]').first().click();
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
  ok(await page.evaluate(() => document.querySelector('#repoField').hidden && /\S/.test(document.querySelector('#repoP').textContent)), 'a removal shows its confirm sentence and no password field');
  await page.locator('dialog.repoDlg [data-k=go]').click();
  await page.waitForFunction(f => B.master.files.length === f - 1, filesNow, { timeout: 15000 });
  ok(asked('masterRemoveFile').length === 1 && asked('masterRemoveFile')[0].token && asked('masterRemoveFile')[0].status === 200, 'a removal: the real door took the single-use delete token and removed the file');
  const delUnlock = asked('repoUnlock').filter(c => c.action === 'delete');
  ok(delUnlock.length === 1 && delUnlock[0].viaSession && !delUnlock[0].pw && delUnlock[0].status === 200, 'that token came from the session: no password was sent');
  await page.evaluate(() => { document.getElementById('mHist').open = true; const r = document.querySelector('#mFiles .masterFileRow'); r.closest('.masterDay').open = true; r.open = true; });
  await page.locator('[data-rm]').first().click();
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
  ok(true, 'the next removal asks its confirm again (a delete token is never reused)');
  await page.locator('dialog.repoDlg [data-k=cancel]').click();
  ok(asked('masterRemoveFile').length === 1, 'cancelled: nothing more was removed');

  // 4 · nothing the page or the door kept holds a password or a token
  const all = JSON.stringify([...st.docs.entries()]);
  ok(!all.includes(REPO_TEST.password) && !all.includes(WRONG), 'the in-memory Firestore holds neither password');
  const stored = await page.evaluate(() => JSON.stringify([sessionStorage, localStorage].map(s => Object.keys(s).map(k => s.getItem(k)))));
  ok(!stored.includes(REPO_TEST.password) && !stored.includes(WRONG), 'the tab\'s storage holds neither password');
  ok(!calls.some(c => c.status === 401), 'the real door refused no call the page made (no 401 repo-locked)');
  ok(!errors.length, 'no page errors: ' + errors.slice(0, 2).join(' | '));
  await browser.close(); srv.close(); fs.rmSync(tmp, { recursive: true, force: true });
  console.log('\nrepo-protect-door: ' + n + ' checks passed');
  process.exit(0);
})().catch(e => { console.error('FAIL ' + (e && e.stack || e)); process.exit(1); });
