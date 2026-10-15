// Delete from the Charm Repo (Paul, 15 Oct 2026: "remove all of these broken keys ... add the ability to delete individual charms ... make sure the delete is password protected").
//   SERVER   charmNestLibrary masterRemoveSkus on the real handler and the real password door (_charmNestRepoAuth.js), in-memory Firestore: no token, a wrong kind of token and a spent token never delete;
//            a delete token removes exactly the SKUs it names (and says which were not there); a bad name, an empty list or more than 200 names refuse the WHOLE call; the design files stay in storage.
//   PAGE     the Master tab in headless Chromium, the server strict: every tile has a locked Delete; it asks for the password in the one repo dialog (cancel and a wrong password delete nothing); a charm that
//            carries two SKUs goes with both; while a search narrows the grid a "Delete these N charms" button deletes them all behind ONE password, and is not offered for more than 200 SKUs.
//   PW_DIR=<dir with playwright-core> node tests/charm-nest/master-delete.cjs        (the page part is skipped without a browser)
'use strict';
const assert = require('assert'), path = require('path'), crypto = require('crypto'), fs = require('fs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

/* ───────────────────────────── SERVER ───────────────────────────── */
async function server() {
  const F = require('./_sandboxFakes.cjs'); F.install();
  const fnDir = path.join(__dirname, '../../netlify/functions');
  const Lib = require(path.join(fnDir, 'charmNestLibrary.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js')), Admins = require(path.join(fnDir, '_stationAdmins.js'));
  const { store, blobs, writes } = F;
  const rnd = k => crypto.randomBytes(k).toString('hex'), PW = 'test-' + rnd(6);
  { const salt = crypto.randomBytes(32), hash = crypto.pbkdf2Sync(PW, salt, 200000, 64, 'sha512'); store.set('EtsyMail_Operators/paul', { role: 'owner', passwordHash: hash.toString('hex'), salt: salt.toString('hex'), iterations: 200000, username: 'paul', displayName: 'Paul K' }); }
  store.set('config/stationAdmins', { names: ['Paul K'] });
  let T = 1800000000000; RA.deps.now = () => T; Admins.deps.now = () => T;
  const call = body => Lib.handler({ httpMethod: 'POST', headers: { 'user-agent': 'master-delete-test/1.0' }, body: JSON.stringify(body) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
  const token = async (action, target) => { const r = await call({ op: 'repoUnlock', password: PW, action, by: 'Ana', target: target || 'T', tab: 'tab1' }); assert(r.body.ok && r.body.token, 'the door gave a ' + action + ' token: ' + JSON.stringify(r.body)); return r.body.token; };
  const seed = (sku, ai) => { store.set('Charm_Master_Index/' + sku, { sku, masterHash: 'aaaa1111', widthPt: 40, heightPt: 50, areaPt2: 800, aiPath: ai || 'charmnest/master/' + sku + '.ai' }); blobs.set(ai || 'charmnest/master/' + sku + '.ai', { buf: Buffer.from('%PDF-test'), contentType: 'application/illustrator', meta: {} }); };
  const has = sku => store.has('Charm_Master_Index/' + sku), count = () => [...store.keys()].filter(k => k.startsWith('Charm_Master_Index/')).length;
  for (const s of ['KEY-A', 'KEY-B', 'KEY-C', 'KEEP-1']) seed(s);
  seed('TWO-1', 'charmnest/master/TWO.ai'); seed('TWO-2', 'charmnest/master/TWO.ai');
  let r;

  // 1 · locked: no token, junk, the wrong kinds of token; nothing is written
  const before = writes.length, blobsBefore = blobs.size;
  r = await call({ op: 'masterRemoveSkus', skus: ['KEY-A'] }); ok(r.status === 401 && r.body.error === 'repo-locked' && r.body.why === 'missing', 'no token: locked: ' + JSON.stringify(r.body));
  r = await call({ op: 'masterRemoveSkus', skus: ['KEY-A'], sandbox: true }); ok(r.status === 401, 'the sandbox flag does not open it');
  r = await call({ op: 'masterRemoveSkus', skus: ['KEY-A'], repoToken: 'not-a-token' }); ok(r.status === 401 && r.body.why === 'invalid', 'a junk token: ' + JSON.stringify(r.body));
  for (const kind of ['upload', 'modify', 'download', 'other']) { r = await call({ op: 'masterRemoveSkus', skus: ['KEY-A'], repoToken: await token(kind) }); ok(r.status === 401 && r.body.why === 'scope', `a ${kind} token never deletes: ` + JSON.stringify(r.body)); }
  r = await Lib.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: { op: 'masterRemoveSkus' }, body: null }); ok(r.statusCode === 401 || r.statusCode === 405, 'a GET is refused');
  ok(['KEY-A', 'KEY-B', 'KEY-C', 'KEEP-1', 'TWO-1', 'TWO-2'].every(has) && count() === 6 && blobs.size === blobsBefore, 'every refused call left all six entries and every file as they were (' + count() + ' entries)');
  void before;

  // 2 · a delete token removes what it names, in any case, and says what was not there
  const del1 = await token('delete', 'KEY-A, KEY-B');
  r = await call({ op: 'masterRemoveSkus', skus: ['key-a', 'KEY-B', 'KEY-B', 'NOT-HERE'], repoToken: del1 });
  ok(r.status === 200 && r.body.ok && r.body.removed.join() === 'KEY-A,KEY-B' && r.body.missing.join() === 'NOT-HERE', 'removes KEY-A and KEY-B (once each), says NOT-HERE was not there: ' + JSON.stringify(r.body));
  ok(!has('KEY-A') && !has('KEY-B') && has('KEY-C') && has('KEEP-1') && count() === 4, 'and only those: ' + [...store.keys()].filter(k => k.startsWith('Charm_Master_Index/')).join(' '));
  ok(blobs.has('charmnest/master/KEY-A.ai') && blobs.has('charmnest/master/KEY-B.ai'), 'the design files stay in storage');
  r = await call({ op: 'masterRemoveSkus', skus: ['KEY-C'], repoToken: del1 }); ok(r.status === 401 && r.body.why === 'used' && has('KEY-C'), 'the same token a second time is refused: ' + JSON.stringify(r.body));

  // 3 · a charm that carries two SKUs goes with both, behind one token
  r = await call({ op: 'masterRemoveSkus', skus: ['TWO-1', 'TWO-2'], repoToken: await token('delete', 'TWO') }); ok(r.status === 200 && r.body.removed.length === 2 && !has('TWO-1') && !has('TWO-2') && blobs.has('charmnest/master/TWO.ai'), 'both SKUs of a shared design removed in one call, its file kept');

  // 4 · a call that cannot be done in full is refused whole
  r = await call({ op: 'masterRemoveSkus', skus: ['KEY-C', 'bad/name'], repoToken: await token('delete') }); ok(r.status !== 401 && /bad sku/.test(r.body.error || '') && !r.body.removed && has('KEY-C'), 'a name that is no SKU: the whole call refused, KEY-C stays: ' + JSON.stringify(r.body));
  r = await call({ op: 'masterRemoveSkus', skus: [], repoToken: await token('delete') }); ok(/no SKU/.test(r.body.error || '') && count() === 2, 'an empty list is refused');
  r = await call({ op: 'masterRemoveSkus', repoToken: await token('delete') }); ok(/no SKU/.test(r.body.error || ''), 'no list at all is refused');
  for (let i = 0; i < 201; i++) seed('BULK-' + String(i).padStart(3, '0'));
  r = await call({ op: 'masterRemoveSkus', skus: Array.from({ length: 201 }, (_, i) => 'BULK-' + String(i).padStart(3, '0')), repoToken: await token('delete') }); ok(/at most 200/.test(r.body.error || '') && count() === 2 + 201, '201 names are refused and none is removed: ' + JSON.stringify(r.body).slice(0, 120));
  r = await call({ op: 'masterRemoveSkus', skus: Array.from({ length: 200 }, (_, i) => 'BULK-' + String(i).padStart(3, '0')), repoToken: await token('delete', 'BULK') }); ok(r.status === 200 && r.body.removed.length === 200 && count() === 2 + 1, '200 names are removed in one call (batches under the 500 limit)');

  // 5 · the library's own answer follows: a page that holds a copy of the index sees the count change
  r = await call({ op: 'masterList', limit: 50 }); ok(r.status === 200 && r.body.index && r.body.index.count === 3, 'masterList counts what is left: ' + JSON.stringify(r.body.index));
  // 6 · every attempt is recorded by the door, and no password is stored
  const attempts = [...store].filter(([k]) => k.startsWith('CharmRepo_AuthAttempts/')).map(([, v]) => v);
  ok(attempts.some(a => a.action === 'delete' && a.ok === true) && !JSON.stringify([...store]).includes(PW), 'the door recorded the delete unlocks, and holds no password');
}

/* ───────────────────────────── PAGE ───────────────────────────── */
async function page() {
  const root = path.join(__dirname, '../..'), pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('page part skipped: no playwright-core (' + pwDir + ')'); return; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; if (!fs.existsSync(exe)) { console.log('page part skipped: no browser at ' + exe); return; }
  const { start, Timestamp, REPO_TEST } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [], repoSeed: false }), { st, sorterOrigin } = srv; st.repoStrict = true;
  const seed = (sku, ai) => { ai = ai || 'charmnest/master/' + sku + '.ai'; st.put('Charm_Master_Index', sku, { sku, masterHash: 'f1', widthPt: 40, heightPt: 50, areaPt2: 1500, members: 3, holes: 1, upAngle: 0, aiPath: ai, aiUrl: srv.blobUrl(ai), thumbPath: 'charmnest/master/' + sku + '.png', indexedAt: Timestamp.fromMillis(Date.UTC(2026, 9, 8, 12)) }); st.blobs.set(ai, { buf: Buffer.from('%PDF-1.4 test'), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } }); };
  for (const s of ['HK-A', 'HK-B', 'HK-C', 'KEEP-1']) seed(s); seed('TWO-1', 'charmnest/master/TWO.ai'); seed('TWO-2', 'charmnest/master/TWO.ai');
  const inIndex = () => st.list('Charm_Master_Index').map(e => e._id).sort().join();
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 200, contentType: 'application/octet-stream', body: 'x' }));
  const calls = [], real = st.handlers.charmNestLibrary.handler;
  st.handlers.charmNestLibrary.handler = async ev => { const out = await real(ev); let b = {}; try { b = JSON.parse(ev.body || '{}') || {}; } catch (_) { /* not JSON */ } calls.push({ op: b.op, skus: b.skus, token: !!b.repoToken, status: out.statusCode }); return out; };
  await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Paul K'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'x'; window.alert = () => {}; if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} } });
  const p = await ctx.newPage(), errors = []; p.on('pageerror', e => errors.push(e.message));
  const typeAndGo = async pw => { const f = p.locator('dialog.repoDlg[open] #repoPw'); await f.fill(pw); await f.press('Enter'); };
  const tiles = () => p.evaluate(() => [...document.querySelectorAll('#mGrid .skuTile')].map(t => t.dataset.sku).sort().join());
  const waitTiles = want => p.waitForFunction(w => [...document.querySelectorAll('#mGrid .skuTile')].map(t => t.dataset.sku).sort().join() === w, want, { timeout: 20000 });
  const asked = op => calls.filter(c => c.op === op);
  try {
    await p.goto(`${sorterOrigin}/charm-nest-1.html`);
    await p.waitForFunction(() => window.CN && window.CN.S && window.CharmNestRepoLock && window.Session && Session.ready() && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await p.evaluate(() => CN.setMode('master')); await p.waitForSelector('#mGrid .skuTile');
    await waitTiles('HK-A,HK-B,HK-C,KEEP-1,TWO-1');
    ok(await p.evaluate(() => [...document.querySelectorAll('#mGrid .skuTile')].every(t => { const b = t.querySelector('[data-del]'); return b && b.dataset.repo === 'delete' && b.classList.contains('repoLocked'); })), 'every tile has a Delete button, locked (the padlock)');
    ok((await p.locator('#mBulk button').count()) === 0, 'no "delete these" button while nothing is searched');

    // 1 · one charm: cancel, a wrong password, the right one
    await p.locator('.skuTile[data-sku="KEEP-1"] [data-del]').click();
    await p.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
    const said = await p.evaluate(() => document.querySelector('dialog.repoDlg').innerText);
    ok(/Delete from the charm repo/.test(said) && /KEEP-1/.test(said) && /for good/.test(said) && /storage/.test(said), 'the dialog says what is deleted and that the files stay: ' + said.replace(/\s+/g, ' ').slice(0, 160));
    await p.locator('dialog.repoDlg [data-k=cancel]').click(); await p.waitForFunction(() => !document.querySelector('dialog.repoDlg'));
    ok(inIndex().includes('KEEP-1') && asked('masterRemoveSkus').length === 0, 'cancelled: nothing was sent, nothing deleted');
    await p.locator('.skuTile[data-sku="KEEP-1"] [data-del]').click(); await p.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
    await typeAndGo('wrong-' + crypto.randomBytes(4).toString('hex'));
    await p.waitForFunction(() => /not right/i.test((document.querySelector('dialog.repoDlg .repoMsg') || {}).textContent || ''), null, { timeout: 15000 });
    ok(inIndex().includes('KEEP-1') && asked('masterRemoveSkus').length === 0, 'a wrong password deletes nothing and sends no delete');
    await typeAndGo(REPO_TEST.password);
    await waitTiles('HK-A,HK-B,HK-C,TWO-1');
    ok(!inIndex().includes('KEEP-1') && asked('masterRemoveSkus').length === 1 && asked('masterRemoveSkus')[0].skus.join() === 'KEEP-1' && asked('masterRemoveSkus')[0].status === 200, 'the right password deletes KEEP-1 (one call, 200) and its tile goes');

    // 2 · a charm that carries two SKUs goes with both
    await p.locator('.skuTile[data-sku="TWO-1"] [data-del]').click(); await p.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
    ok(/TWO-1, TWO-2/.test(await p.evaluate(() => document.querySelector('dialog.repoDlg').innerText)), 'the dialog names both SKUs of the shared charm');
    await typeAndGo(REPO_TEST.password); await waitTiles('HK-A,HK-B,HK-C');
    ok(!inIndex().includes('TWO') && asked('masterRemoveSkus')[1].skus.join() === 'TWO-1,TWO-2' && st.blobs.has('charmnest/master/TWO.ai'), 'both SKUs deleted in one call; its file is still in storage');

    // 3 · a search narrows the grid: delete them all behind one password
    await p.fill('#mSearch', 'HK-'); await waitTiles('HK-A,HK-B,HK-C');
    const bulk = p.locator('#mDelShown'); ok((await bulk.count()) === 1 && /Delete these 3 charms/.test(await bulk.innerText()) && (await bulk.getAttribute('data-repo')) === 'delete', 'the search shows "Delete these 3 charms", locked');
    await p.fill('#mSearch', 'NOTHING-LIKE-THIS'); await p.waitForFunction(() => /No indexed SKU contains/.test(document.querySelector('#mGrid').innerText));
    ok((await p.locator('#mBulk button').count()) === 0, 'a search that finds nothing leaves no delete button behind');
    await p.fill('#mSearch', 'HK-'); await waitTiles('HK-A,HK-B,HK-C');
    await p.locator('#mDelShown').click(); await p.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
    ok(/3 SKUs \(HK-A, HK-B, HK-C\)/.test(await p.evaluate(() => document.querySelector('dialog.repoDlg').innerText)), 'the dialog counts them and names them');
    await typeAndGo(REPO_TEST.password);
    await p.waitForFunction(() => /No charms indexed yet|No indexed SKU/.test(document.querySelector('#mGrid').innerText), null, { timeout: 20000 });
    ok(inIndex() === '' && asked('masterRemoveSkus')[2].skus.join() === 'HK-A,HK-B,HK-C' && asked('masterRemoveSkus').length === 3, 'ONE password, ONE call: the three are deleted together');
    ok(st.list('CharmRepo_AuthAttempts').filter(a => a.action === 'delete' && a.ok === true).length === 3 && st.list('CharmRepo_AuthAttempts').filter(a => a.ok === false).length === 1, 'the real door recorded 3 delete unlocks and the 1 wrong try');

    // 4 · more than 200 SKUs shown: no button, the page says to narrow the search
    for (let i = 1; i <= 201; i++) seed('BULK-' + String(i).padStart(3, '0'));
    await p.fill('#mSearch', ''); await p.evaluate(() => Master.load(true).then(() => document.querySelector('#mSearch').dispatchEvent(new Event('input'))));
    await p.fill('#mSearch', 'BULK-'); await p.waitForFunction(() => /201 match/.test(document.querySelector('#mCount').innerText), null, { timeout: 30000 });
    ok((await p.locator('#mDelShown').count()) === 0 && /narrow the search to 200 SKUs or fewer/.test(await p.locator('#mBulk').innerText()), '201 SKUs shown: no delete button, "narrow the search to 200 SKUs or fewer"');
    await p.fill('#mSearch', 'BULK-00'); await p.waitForFunction(() => /9 match/.test(document.querySelector('#mCount').innerText), null, { timeout: 30000 });
    ok((await p.locator('#mDelShown').count()) === 1 && /Delete these 9 charms/.test(await p.locator('#mDelShown').innerText()), 'narrowed to 9: the button is back');
    ok(errors.length === 0, 'no page error: ' + errors.slice(0, 2).join(' | '));
  } finally { await browser.close(); srv.close(); }
}

(async () => { await server(); await page(); console.log('master-delete OK ·', n, 'checks'); })().catch(e => { console.error(e); process.exit(1); });
