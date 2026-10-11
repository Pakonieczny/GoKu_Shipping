// The Settings menu's lock, in the sorter page, against the REAL password door (netlify/functions/_charmNestRepoAuth.js) on the in-memory Firestore
// (ADMINSESSION; plans/save-to-repo-1011/ADMINSESSION-settings.md). The page is served locked, the server is strict (st.repoStrict), the password is typed into the real
// dialog. Proves: Settings opens locked and a typed name (Paul K) alone never opens it; a wrong password stays locked; the right one opens it and buys a standing
// unlock kept in localStorage (survives a reload and a new tab) that the repo actions and the Settings buttons use with no second password; a stored session
// the server ended closes the menu and asks again; the time running out and "Lock now" re-lock (server side too); nothing the page stores, logs or sends holds
// the password. The only password is the stand-in operator's, made fresh in bridge-server.cjs (REPO_TEST); nothing real is read, written or printed.
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/admin-session-ui.cjs      (skipped without a browser)
const fs = require('fs'), path = require('path'), assert = require('assert'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, Timestamp, REPO_TEST } = require('./bridge-server.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const WRONG = 'w' + crypto.randomBytes(9).toString('hex');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');

(async () => {
  const srv = await start({ receipts: [], repoSeed: false });
  const { st, sorterOrigin } = srv; st.repoStrict = true;
  st.put('Charm_Master_Files', 'f1', { masterHash: 'f1', name: 'AS-0.ai', indexedAt: Timestamp.fromMillis(Date.UTC(2026, 9, 8, 12)), charms: 1, labelled: 1, skus: ['AS-0'], indexedBy: 'browser' });
  st.put('Charm_Master_Index', 'AS-0', { sku: 'AS-0', masterHash: 'f1', widthPt: 40, heightPt: 50, areaPt2: 1500, members: 3, holes: 1, upAngle: 0, aiPath: 'charmnest/master/AS-0.ai', aiUrl: srv.blobUrl('charmnest/master/AS-0.ai'), thumbPath: 'charmnest/master/AS-0.png' });
  st.blobs.set('charmnest/master/AS-0.ai', { buf: Buffer.from('%PDF-1.4 test'), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });

  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 200, contentType: 'application/octet-stream', body: 'x' }));
  const calls = [];   // what reached the real handlers (never a password or a token: only whether the call carried one)
  for (const fn of ['charmNestLibrary', 'charmNestOutput']) {
    const h = st.handlers[fn], real = h.handler;
    h.handler = async ev => { const out = await real(ev); let b = {}; try { b = JSON.parse(ev.body || '{}') || {}; } catch (_) { /* not JSON */ } calls.push({ fn, op: b.op, token: !!b.repoToken, action: b.action || null, session: b.session === true, viaSession: !!b.sessionToken, pw: !!b.password, status: out.statusCode || 200 }); return out; };
  }
  // the person is signed in as "Paul K": the typed name every admin check of the page reads; the page may never treat it as a key
  await ctx.addInitScript(() => {
    try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Paul K'); } catch (_) {}
    window.confirm = () => true; window.prompt = () => 'x'; window.alert = () => {};
    window.__attempts = []; window.addEventListener('repo-auth-attempt', e => window.__attempts.push(e.detail));
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  });
  const logs = [], errors = [];
  const watch = p => { p.on('pageerror', e => errors.push('pageerror: ' + e.message)); p.on('console', m => logs.push(m.text())); };
  const ready = async p => { await p.goto(`${sorterOrigin}/charm-nest-1.html`); await p.waitForFunction(() => window.CN && window.CN.S && window.CharmNestRepoLock && CN.S.cloud.ok !== null, null, { timeout: 60000 }); await p.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; }); };
  const openSettings = async p => { await p.evaluate(() => { const m = document.getElementById('moreMenu'); if (m) m.open = true; }); await p.click('#btnSettings'); };
  const settingsOpen = p => p.evaluate(() => !!document.querySelector('#dlgSettings[open]'));
  const lockDlg = p => p.evaluate(() => { const d = document.querySelector('dialog.repoDlg[open]'); return d ? d.dataset.repoAction : ''; });
  const typeAndGo = async (p, pw) => { const f = p.locator('dialog.repoDlg[open] #repoPw'); await f.fill(pw); await f.press('Enter'); };
  const asked = op => calls.filter(c => c.op === op);
  const session = p => p.evaluate(() => { try { return JSON.parse(localStorage.getItem('cn.repoSession.v1') || 'null'); } catch (_) { return null; } });
  const sessionDocs = () => [...st.docs.keys()].filter(k => k.startsWith('CharmRepo_Tokens/') && (st.docs.get(k) || {}).action === 'session');

  const page = await ctx.newPage(); watch(page);
  await ready(page);
  ok(await page.evaluate(() => (localStorage.getItem('cn.employee') || '') === 'Paul K'), 'the person is signed in with the typed name Paul K');

  // 1 · Settings opens locked: the typed name Paul K is no key; a cancel keeps it shut
  await openSettings(page);
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=settings]');
  ok(!(await settingsOpen(page)), 'Settings opens locked: the password dialog came, the menu did not');
  ok((await page.evaluate(() => document.querySelector('dialog.repoDlg h3').textContent)) === 'Settings', 'the dialog is the ONE repo dialog, titled Settings');
  ok(!(await page.evaluate(() => CharmNestRepoLock.unlocked('settings'))) && !(await page.evaluate(() => CharmNestRepoLock.unlocked('upload'))), 'and the lock reports locked for Settings and for the repo');
  await page.locator('dialog.repoDlg [data-k=cancel]').click();
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'));
  ok(!(await settingsOpen(page)) && asked('repoUnlock').length === 0, 'cancelled: still locked, and nothing was sent');

  // 2 · a wrong password stays locked and is recorded; the right one opens it
  await openSettings(page);
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=settings]');
  await typeAndGo(page, WRONG);
  await page.waitForFunction(() => /not right/i.test((document.querySelector('dialog.repoDlg .repoMsg') || {}).textContent || ''), null, { timeout: 15000 });
  ok(!(await settingsOpen(page)) && (await page.evaluate(() => document.querySelector('#repoPw').value === '')), 'a wrong password: the dialog stays, the menu stays shut, the field is clear');
  ok(asked('repoUnlock').length === 1 && asked('repoUnlock')[0].action === 'settings' && asked('repoUnlock')[0].session && asked('repoUnlock')[0].pw && asked('repoUnlock')[0].status === 200, 'the real door was asked once (action settings, session)');
  ok(st.list('CharmRepo_AuthAttempts').length === 1 && st.list('CharmRepo_Alerts').length === 1 && st.list('CharmRepo_Alerts')[0].ok === false && st.list('CharmRepo_Alerts')[0].action === 'settings', 'the wrong try is recorded and alerted by the real door');
  ok(!(await session(page)) && !(await page.evaluate(() => CharmNestRepoLock.unlocked('settings'))), 'and no session exists');
  await typeAndGo(page, REPO_TEST.password);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg') && !!document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  ok(await settingsOpen(page), 'the right password opens the Settings menu');
  const s1 = await session(page);
  ok(s1 && typeof s1.t === 'string' && s1.t.length > 60 && s1.e > Date.now() + 60000, 'a standing unlock is kept in this computer\'s localStorage with its end time');
  ok(sessionDocs().length === 1 && st.docs.has('CharmRepo_Tokens/' + sha(s1.t)), 'the server keeps only its sha256');
  const line = await page.evaluate(() => { const l = document.getElementById('stLockLine'); return { hidden: l.hidden, text: l.textContent.replace(/\s+/g, ' ').trim() }; });
  ok(!line.hidden && /^Unlocked until \d{1,2}:\d{2} [ap]m · Lock now$/.test(line.text), 'the quiet line reads "' + line.text + '"');
  ok((await page.evaluate(() => window.__attempts.map(a => a.reason).join())) === 'wrong,ok', 'the Alert was told about both tries (wrong, ok)');
  ok(st.list('CharmRepo_Alerts').length === 2, 'two Alerts in all: one per password try');

  // 3 · the same unlock serves the repo and the Settings buttons, with no second password and no new Alert
  await page.evaluate(() => document.getElementById('dlgSettings').close());
  const unlocksBefore = asked('repoUnlock').length, alertsBefore = st.list('CharmRepo_Alerts').length;
  const dl = await page.evaluate(async () => { try { const r = await CharmNestRepoLock.call('download', { what: 'x' }, tok => CN.api('charmNestLibrary', { op: 'masterDownload', sku: 'AS-0', repoToken: tok })); return { ok: !!(r && r.ok), asked: !!document.querySelector('dialog.repoDlg') }; } catch (e) { return { err: e.message }; } });
  ok(dl.ok && !dl.asked && asked('masterDownload').length === 1 && asked('masterDownload')[0].status === 200, 'a download uses the same unlock: no dialog, the real door answered 200');
  const up = await page.evaluate(() => CharmNestRepoLock.unlocked('upload') && CharmNestRepoLock.unlocked('modify') && CharmNestRepoLock.unlocked('settings') && !CharmNestRepoLock.unlocked('delete'));
  ok(up, 'the lock reports upload, change and Settings unlocked, and delete never');
  const sb = await page.evaluate(async () => { try { const r = await CN.api('charmNestLibrary', { op: 'sandboxReset', sandbox: true }); return { ok: r && r.ok !== false, asked: !!document.querySelector('dialog.repoDlg') }; } catch (e) { return { err: e.message }; } });
  ok(sb.ok && !sb.asked && asked('sandboxReset').length === 1 && asked('sandboxReset')[0].token && asked('sandboxReset')[0].status === 200, 'a Settings button op (sandboxReset) carries the unlock by itself and the real door lets it in');
  const pg = await page.evaluate(async () => { try { await CN.api('charmNestLibrary', { op: 'purgeHistory', code: 'not-the-code' }); return { ok: true }; } catch (e) { return { status: e.status, msg: e.message, repoLocked: !!(e.data && e.data.error === 'repo-locked') }; } });
  ok(pg.status === 403 && !pg.repoLocked && asked('purgeHistory')[0].token, 'purgeHistory passes the repo door and meets its own passcode rule, which still refuses (' + pg.status + ')');
  // (the Settings row of SANDBOXN, how many newest orders the sandbox pulls: a repo "modify" write, asked for exactly as the row asks for it)
  const pl = await page.evaluate(async () => { try { const r = await CharmNestRepoLock.call('modify', { title: 'Change a sandbox setting', what: 'x', target: 'sandbox pulls 120 newest', go: 'Save' }, tok => CN.api('charmNestLibrary', { op: 'sandboxPullLimitSet', limit: 120, by: 'Paul', repoToken: tok }, { quiet: true })); return { ok: !!(r && r.ok), asked: !!document.querySelector('dialog.repoDlg') }; } catch (e) { return { err: e.message }; } });
  ok(pl.ok && !pl.asked && asked('sandboxPullLimitSet').length === 1 && asked('sandboxPullLimitSet')[0].token && asked('sandboxPullLimitSet')[0].status === 200, 'the pull-number row (a repo "modify" write) uses the same unlock: no dialog, the real door answered 200');
  ok(asked('repoUnlock').length === unlocksBefore && st.list('CharmRepo_Alerts').length === alertsBefore, 'none of that asked the server for a password or raised an Alert');

  // 4 · it survives a reload and a new tab of this computer
  await ready(page);
  await openSettings(page);
  await page.waitForFunction(() => !!document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  ok(!(await lockDlg(page)), 'after a reload Settings opens with no password');
  const tab2 = await ctx.newPage(); watch(tab2); await ready(tab2);
  await openSettings(tab2);
  await tab2.waitForFunction(() => !!document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  ok(!(await lockDlg(tab2)) && asked('repoUnlock').length === unlocksBefore, 'a new tab opens Settings with no password too');
  ok(asked('repoSession').length >= 2 && asked('repoSession').every(c => c.status === 200), 'opening Settings asked the server once whether the session still stands (it does)');

  // 5 · a session the server ended: the menu closes and asks again
  await tab2.evaluate(() => document.getElementById('dlgSettings').close()); await tab2.close();
  await page.evaluate(() => document.getElementById('dlgSettings').close());
  for (const k of sessionDocs()) st.docs.delete(k);
  await openSettings(page);
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=settings]', { timeout: 15000 });
  ok(!(await settingsOpen(page)) && !(await session(page)), 'a session the server no longer knows: the menu closes, the stored copy is dropped, the password is asked');
  await typeAndGo(page, REPO_TEST.password);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg') && !!document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  const s2 = await session(page); ok(s2 && s2.t !== s1.t, 'a new one is given');

  // 6 · Lock now: this computer and the server both forget it; every door asks again
  await page.click('#stLockNow');
  await page.waitForFunction(() => !document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  ok(!(await session(page)) && !(await page.evaluate(() => CharmNestRepoLock.unlocked('settings'))), 'Lock now: Settings shut, the stored session is gone');
  await new Promise(r => setTimeout(r, 500));
  ok(sessionDocs().length === 0 && asked('repoLock').length === 1 && asked('repoLock')[0].status === 200, 'and the server deleted it');
  const old = await page.evaluate(async t => { try { await CN.api('charmNestLibrary', { op: 'sandboxReset', sandbox: true, repoToken: t }); return 'ran'; } catch (e) { return e.data && e.data.error === 'repo-locked' ? 'locked:' + e.data.why : 'other:' + e.message; } }, s2.t);
  ok(old === 'locked:invalid', 'the old token opens nothing now, at the real door (' + old + ')');
  const resets = asked('sandboxReset').length;
  const blocked = page.evaluate(async () => { try { await CN.api('charmNestLibrary', { op: 'sandboxReset', sandbox: true }); return 'ran'; } catch (e) { return e.cancelled ? 'cancelled' : 'error:' + e.message; } });
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=settings]', { timeout: 15000 });
  await page.locator('dialog.repoDlg [data-k=cancel]').click();
  ok((await blocked) === 'cancelled' && asked('sandboxReset').length === resets, 'a Settings button op after Lock now asks the password; cancelled, it never reaches the server');
  await openSettings(page);
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=settings]');
  ok(!(await settingsOpen(page)), 'Settings asks again');

  // 7 · the time running out re-locks (the client closes the menu by itself)
  await typeAndGo(page, REPO_TEST.password);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg') && !!document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  await page.evaluate(() => { const o = JSON.parse(localStorage.getItem('cn.repoSession.v1')); o.e = Date.now() + 7000; localStorage.setItem('cn.repoSession.v1', JSON.stringify(o)); window.dispatchEvent(new StorageEvent('storage', { key: 'cn.repoSession.v1' })); });
  await page.waitForFunction(() => !document.querySelector('#dlgSettings[open]'), null, { timeout: 15000 });
  ok(!(await page.evaluate(() => CharmNestRepoLock.unlocked('settings'))) && !(await session(page)), 'when the unlock runs out the menu closes by itself and the stored session is dropped');
  await openSettings(page);
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=settings]');
  ok(!(await settingsOpen(page)), 'and Settings asks again');
  await page.locator('dialog.repoDlg [data-k=cancel]').click();

  // 8 · nothing the page kept, logged or sent holds the password; the token is only where it should be
  const all = JSON.stringify([...st.docs.entries()]);
  ok(!all.includes(REPO_TEST.password) && !all.includes(WRONG), 'the in-memory Firestore holds neither password');
  const stored = await page.evaluate(() => JSON.stringify([sessionStorage, localStorage].map(s => Object.keys(s).map(k => k + '=' + s.getItem(k)))));
  ok(!stored.includes(REPO_TEST.password) && !stored.includes(WRONG), 'this computer\'s storage holds neither password');
  ok(!logs.join('\n').includes(REPO_TEST.password) && !logs.join('\n').includes(WRONG) && !logs.join('\n').includes(s1.t) && !logs.join('\n').includes(s2.t), 'the console never showed a password or a token');
  ok(!all.includes(s1.t) && !all.includes(s2.t), 'the server stores only token hashes');
  ok(!calls.some(c => c.op === 'masterDownload' && c.status === 401), 'the repo action was never refused');
  ok(!errors.length, 'no page errors: ' + errors.slice(0, 2).join(' | '));
  await browser.close(); srv.close();
  console.log('\nadmin-session-ui: ' + n + ' checks passed');
  process.exit(0);
})().catch(e => { console.error('FAIL ' + (e && e.stack || e)); process.exit(1); });
