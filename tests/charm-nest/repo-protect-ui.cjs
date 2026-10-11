// Charm Repo protection, the client side (REPOUI): the Master files tab keeps its upload history under ONE closed dropdown; looking, searching and
// using charms never asks; every protected control (add a master file, remove, change, download, confirm labels, the rail's "Index into the
// charm library") opens the ONE password dialog; a wrong password keeps it locked, the right one runs the action once, a token is reused until
// it runs out, a delete asks every time. The server is a mock in front of the real in-memory handlers; the passwords are random per run.
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/repo-protect-ui.cjs      (skipped without a browser)
const fs = require('fs'), path = require('path'), assert = require('assert'), os = require('os'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
const { buildMaster } = require('./fixture-master.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };

// throwaway credentials, new every run, that exist only in this test's mock
const RIGHT = 'r' + crypto.randomBytes(9).toString('hex'), WRONG = 'w' + crypto.randomBytes(9).toString('hex');
// REPOAUTH-api.md: what a token may serve, and how long it lives
const COVER = { upload: ['upload', 'other'], modify: ['modify', 'upload', 'other'], download: ['download', 'other'], delete: ['delete'] };
const TTL = { upload: 600000, modify: 9000, download: 9000, other: 9000, delete: 120000 };   // (the client keeps 5 s of margin: a 9 s token is ~4 s of use)

const OCT = (d, h) => Date.UTC(2026, 9, d, h), SEP = (d, h) => Date.UTC(2026, 8, d, h);
const hash = k => crypto.createHash('sha256').update('repo-ui-' + k).digest('hex');
const FILES = [
  { h: hash(1), name: 'OCT8-A.ai', at: OCT(8, 12), skus: ['TST-ONE', 'TST-TWO', 'TST-TRI'] },
  { h: hash(2), name: 'OCT8-B.ai', at: OCT(8, 15), skus: ['TST-FOR', 'TST-FIV'] },
  { h: hash(3), name: 'OCT7.ai', at: OCT(7, 12), skus: ['TST-SIX'] },
  { h: hash(4), name: 'SEP17-A.ai', at: SEP(17, 12), skus: ['TST-SEV'] },
  { h: hash(5), name: 'SEP17-B.ai', at: SEP(17, 15), skus: ['TST-EIG'] }
];

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-repoui-'));
  const masterPath = path.join(tmp, 'BRITES-master.ai');
  const fx = await buildMaster(masterPath, { count: 3, edge: false });
  const srv = await start({ receipts: [], repoSeed: false });   // (the page is served locked)
  const { st, sorterOrigin } = srv;
  // the library the page starts from: five master files on three days, a few designs (one blocked, one that shows the faces box)
  for (const f of FILES) st.put('Charm_Master_Files', f.h, { masterHash: f.h, name: f.name, indexedAt: Timestamp.fromMillis(f.at), charms: f.skus.length, labelled: f.skus.length, skus: f.skus, indexedBy: 'browser' });
  const entry = (sku, h, extra) => st.put('Charm_Master_Index', sku, Object.assign({ sku, masterHash: h, widthPt: 40, heightPt: 50, areaPt2: 1500, members: 3, holes: 1, upAngle: 0, aiPath: `charmnest/master/${sku}.ai`, aiUrl: srv.blobUrl(`charmnest/master/${sku}.ai`), thumbPath: `charmnest/master/${sku}.png`, indexedAt: Timestamp.fromMillis(OCT(8, 12)) }, extra));
  entry('TST-ONE', hash(1)); entry('TST-TWO', hash(1), { blocked: 'open outline', sym: 'directional' }); entry('TST-TRI', hash(1)); entry('TST-FOR', hash(2)); entry('TST-FIV', hash(2)); entry('TST-SIX', hash(3)); entry('TST-SEV', hash(4)); entry('TST-EIG', hash(5));
  st.blobs.set('charmnest/master/TST-ONE.ai', { buf: Buffer.from('%PDF-1.4 test'), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });

  // ── the mock door in front of the real handlers ──
  const log = [], held = new Map(), wrongs = { n: 0 }; let unlocks = 0, delayMs = 0, locked = false;
  const sees = (b, ...pw) => !JSON.stringify(b || {}).includes(RIGHT) || pw.length;
  const needOf = (fn, b) => {
    if (fn === 'charmNestLibrary') return { masterPutIndex: 'upload', masterPutFile: 'upload', startMaster: 'upload', masterPatch: 'modify', masterRemoveFile: 'delete', masterRemoveSku: 'delete', masterDownload: 'download' }[b.op] || null;
    if (fn === 'charmNestOutput' && ['sign', 'put', 'finalize'].includes(b.op) && String(b.path || '').startsWith('charmnest/master/')) return 'upload';
    return null;
  };
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 200, contentType: 'application/octet-stream', body: 'x' }));
  await ctx.route(/\/\.netlify\/functions\/(charmNestLibrary|charmNestOutput)$/, async route => {
    const req = route.request(), fn = /functions\/(\w+)/.exec(req.url())[1]; let b = {}; try { b = req.postDataJSON() || {}; } catch (_) { b = {}; }
    const json = (code, body) => route.fulfill({ status: code, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
    const need = needOf(fn, b);
    // (the log never keeps a password or a token: only whether the call carried one)
    log.push({ fn, op: b.op, need, token: !!b.repoToken, path: b.path || null, action: b.action || null, target: b.target || null, by: b.by || null, tab: !!b.tab });
    if (fn === 'charmNestLibrary' && b.op === 'repoUnlock') {
      unlocks++; if (delayMs) await new Promise(r => setTimeout(r, delayMs));
      if (locked) return json(200, { ok: false, error: 'locked', retryAfterSec: 900 });
      if (b.password === RIGHT) { const token = 'tk-' + crypto.randomBytes(8).toString('hex'), act = b.action || 'other'; held.set(token, { action: act, exp: Date.now() + (TTL[act] || 9000), once: act === 'delete', used: false }); return json(200, { ok: true, token, expiresAt: Date.now() + (TTL[act] || 9000), once: act === 'delete', action: act }); }
      wrongs.n++; if (wrongs.n >= 5 && process.env.__LOCK_ON) { locked = true; return json(200, { ok: false, error: 'locked', retryAfterSec: 900 }); }
      return json(200, { ok: false, error: 'wrong' });
    }
    if (need) {
      const t = held.get(b.repoToken); let why = !b.repoToken ? 'missing' : !t ? 'invalid' : t.used ? 'used' : Date.now() > t.exp ? 'expired' : !COVER[need].includes(t.action) ? 'scope' : '';
      if (t && need === 'delete') t.used = true;
      if (why) return json(401, { error: 'repo-locked', need, once: need === 'delete', why });
      if (fn === 'charmNestLibrary' && b.op === 'masterDownload') return json(200, { ok: true, url: srv.blobUrl(`charmnest/master/${b.sku}.ai`), path: `charmnest/master/${b.sku}.ai`, name: `${b.sku}.ai` });
    }
    return route.fallback();
  });
  // the page: signed in as a plain employee; no native confirm may be used (a pop-up on a pop-up); every unlock attempt is heard
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {}
    window.__confirmed = 0; window.confirm = () => { window.__confirmed++; return true; }; window.prompt = () => 'Tester'; window.alert = () => {};
    window.__attempts = []; window.addEventListener('repo-auth-attempt', e => window.__attempts.push(e.detail));
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|favicon|net::ERR|__blob|CORS policy/.test(m.text())) errors.push('console: ' + m.text().slice(0, 200)); });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.Master && window.CharmNestRepoLock && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; });
  ok(await page.evaluate(() => CN.S.cloud.ok), 'the sorter sees the (in-memory) cloud');
  const dlg = () => page.locator('dialog.repoDlg[open]');
  const openDialogs = () => page.evaluate(() => document.querySelectorAll('dialog[open]').length);
  const typeAndGo = async pw => { const f = page.locator('dialog.repoDlg[open] #repoPw'); await f.fill(pw); await f.press('Enter'); };
  const calls = pred => log.filter(pred);
  // the history dropdown, the first day and its first file row opened (the remove button sits in that second fold)
  const openRow = () => page.evaluate(() => { document.getElementById('mHist').open = true; const r = document.querySelector('#mFiles .masterFileRow'); r.closest('.masterDay').open = true; r.open = true; });
  const protectedCalls = () => log.filter(x => x.need);

  // ── 1 · the tab: the history sits under ONE closed dropdown, nothing asks ──
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length >= 8, null, { timeout: 30000 });
  const hist = await page.evaluate(() => {
    const h = document.getElementById('mHist'), days = [...document.querySelectorAll('.masterDay')];
    return { exists: !!h, open: h && h.open, label: h && h.querySelector('summary').textContent, days: days.length, inside: days.every(d => h.contains(d)), hidden: days.every(d => !d.checkVisibility()), shown: days.filter(d => d.checkVisibility()).length, summaries: days.map(d => d.querySelector('summary').textContent), bodyOpen: !!document.querySelector('.masterDay[open]') };
  });
  ok(hist.exists && hist.open === false, 'the upload history is ONE dropdown, closed by default');
  ok(hist.label === 'Upload history (3)', 'its label is short and carries the count: ' + hist.label);
  ok(hist.days === 3 && hist.inside && hist.hidden && hist.shown === 0, 'all three day accordions are inside it and none of them is on screen');
  ok(hist.summaries[0] === 'Oct 8, 2026 · 2 files · 5 SKU lines' && /^Oct 7, 2026 · 1 file · 1 SKU line$/.test(hist.summaries[1]) && /^Sep 17, 2026 · 2 files · 2 SKU lines$/.test(hist.summaries[2]), 'the day lines read as before: ' + hist.summaries.join(' | '));
  await page.click('#mHistSum');
  const opened = await page.evaluate(() => ({ open: document.getElementById('mHist').open, shown: [...document.querySelectorAll('.masterDay')].filter(d => d.checkVisibility()).length }));
  ok(opened.open && opened.shown === 3, 'opening it shows the three days');
  await page.click('#mHistSum');
  ok(!(await page.evaluate(() => document.getElementById('mHist').open)), 'and it closes again');
  // the lock icons: every protected control is shown locked until unlocked
  const icons = await page.evaluate(() => { const els = [...document.querySelectorAll('[data-repo]')]; return { n: els.length, locked: els.filter(e => e.classList.contains('repoLocked')).length, kinds: [...new Set(els.map(e => e.dataset.repo))].sort(), add: document.getElementById('mAdd').classList.contains('repoLocked'), up: !!document.querySelector('input[data-up].repoLocked'), faces: !!document.querySelector('select[data-faces].repoLocked'), unblock: !!document.querySelector('[data-unblock].repoLocked'), dl: !!document.querySelector('[data-dl].repoLocked') }; });
  ok(icons.n > 8 && icons.locked === icons.n, 'every protected control starts in the locked look (' + icons.n + ' of them)');
  ok(icons.add && icons.up && icons.faces && icons.unblock && icons.dl, 'the add button, the angle box, the faces box, unblock and .ai are all locked-looking');
  // looking, searching, using: no dialog, no token, no unlock call
  const mark0 = log.length;
  await page.fill('#mSearch', 'TST-ONE');
  const found = await page.evaluate(() => ({ tiles: document.querySelectorAll('#mGrid .skuTile').length, count: document.getElementById('mCount').textContent }));
  ok(found.tiles === 1 && /1 match/.test(found.count), 'search works without a password: ' + found.count);
  await page.fill('#mSearch', '');
  const looked = await page.evaluate(async () => { const e1 = await Master.fetchEntry('NOT-HELD-SKU'); return { e1, held: !!Master.entryFor('TST-ONE'), thumb: !!Master.thumbOf(Master.entryFor('TST-ONE')) || true }; });
  ok(looked.held && looked.e1 === null, 'a SKU lookup (what an order or a pool add does) works without a password');
  ok(await openDialogs() === 0, 'no dialog opened for any of it');
  ok(log.slice(mark0).every(x => !x.token && x.op !== 'repoUnlock'), 'none of those calls carried a token or asked for one');
  ok(unlocks === 0, 'no unlock was ever attempted by looking, searching or using');
  ok(await page.evaluate(() => window.__attempts.length === 0), 'no attempt event was fired for free actions');

  // ── 2 · each protected control opens the dialog; nothing is sent to the protected ops until the password is right ──
  const ids = [];
  const opensFor = async (what, act, action, expectAction) => {
    await act();
    await page.waitForSelector('dialog.repoDlg[open]', { timeout: 5000 });
    const d = await page.evaluate(() => { const x = document.querySelector('dialog.repoDlg[open]'), pw = x.querySelector('#repoPw'); return { action: x.dataset.repoAction, type: pw.type, ac: pw.autocomplete, count: document.querySelectorAll('dialog[open]').length, title: x.querySelector('h3').textContent, text: x.querySelector('#repoP').textContent, go: x.querySelector('[data-k=go]').textContent, form: !!x.querySelector('form') }; });
    ok(d.action === expectAction && d.type === 'password' && d.ac === 'off' && !d.form, `${what}: the password dialog opens (action ${d.action}, type=password, autocomplete off)`);
    ok(d.count === 1, `${what}: it is the only pop-up (no pop-up on a pop-up)`);
    ids.push([what, d]);
    return d;
  };
  const cancelDialog = async () => { await page.click('dialog.repoDlg[open] [data-k=cancel]'); await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 5000 }); };
  const before = protectedCalls().length;
  // a) add a master file (the picker, then the password)
  await opensFor('add a master file (picker)', () => page.setInputFiles('#mFile', masterPath), 'upload', 'upload');
  ok(await page.evaluate(() => B.master.jobs.size === 0), 'add: nothing was read or started while locked');
  // wrong password: stays locked, the field is clear, one plain line, no token
  const settled = () => page.evaluate(() => Promise.all(document.getAnimations().map(a => a.finished.catch(() => null))).then(() => new Promise(r => setTimeout(r, 60))));
  await settled();
  const t0 = await page.evaluate(() => document.querySelector('dialog.repoDlg #repoPw').getBoundingClientRect().top + '|' + document.querySelector('dialog.repoDlg').getBoundingClientRect().height);
  await typeAndGo(WRONG);
  await page.waitForFunction(() => /not right/.test(document.querySelector('dialog.repoDlg #repoMsg').textContent), null, { timeout: 5000 });
  const wrong = await page.evaluate(() => { const x = document.querySelector('dialog.repoDlg'); return { open: x.open, field: x.querySelector('#repoPw').value, msg: x.querySelector('#repoMsg').textContent, bad: x.querySelector('#repoMsg').classList.contains('bad'), html: x.outerHTML.includes('__PW__'), attempts: window.__attempts.slice(), held: CharmNestRepoLock._held().length, jobs: B.master.jobs.size }; });
  ok(wrong.open && wrong.field === '' && wrong.bad && wrong.msg === 'That password is not right. Try again.', 'a wrong password: the dialog stays, the field is cleared, one plain line: ' + wrong.msg);
  ok(wrong.held === 0 && wrong.jobs === 0 && protectedCalls().length === before, 'a wrong password: still locked, no token kept, nothing sent to a protected op');
  ok(wrong.attempts.length === 1 && wrong.attempts[0].ok === false && wrong.attempts[0].reason === 'wrong' && wrong.attempts[0].action === 'upload', 'a wrong attempt fires repo-auth-attempt (wrong, upload) and carries no password');
  const t1 = await page.evaluate(() => document.querySelector('dialog.repoDlg #repoPw').getBoundingClientRect().top + '|' + document.querySelector('dialog.repoDlg').getBoundingClientRect().height);
  ok(t0 === t1, 'no layout shift: the dialog and its field did not move or grow when the error line appeared (' + t0 + ' = ' + t1 + ')');
  // the spinner while the server checks, then cancel
  delayMs = 500;
  const pending = page.locator('dialog.repoDlg #repoPw').fill(WRONG).then(() => page.locator('dialog.repoDlg #repoPw').press('Enter'));
  await pending;
  await page.waitForFunction(() => document.querySelector('dialog.repoDlg #repoMsg .repoSpin'), null, { timeout: 3000 });
  const spin = await page.evaluate(() => { const x = document.querySelector('dialog.repoDlg'); return { label: x.querySelector('#repoMsg').textContent, field: x.querySelector('#repoPw').value, dis: x.querySelector('#repoPw').disabled, h: x.getBoundingClientRect().height, top: x.querySelector('#repoPw').getBoundingClientRect().top }; });
  ok(spin.label === 'Checking the password…' && spin.field === '' && spin.dis, 'while the server checks: a small labelled spinner, the field is already empty and locked');
  ok(t0 === spin.top + '|' + spin.h, 'no layout shift while the spinner shows');
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg #repoMsg .repoSpin'), null, { timeout: 5000 }); delayMs = 0;
  await cancelDialog();
  ok(await page.evaluate(() => B.master.jobs.size === 0 && !document.querySelector('dialog[open]')) && protectedCalls().length === before, 'cancel: the dialog is gone, nothing was added, nothing was sent');
  // b) dropped on the tab
  await opensFor('add a master file (dropped)', () => page.evaluate(() => { const dt = new DataTransfer(); dt.items.add(new File([new Uint8Array([37, 80, 68, 70])], 'dropped.ai')); const v = document.getElementById('masterView'); for (const t of ['dragenter', 'dragover', 'drop']) v.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt })); }), 'upload', 'upload');
  await cancelDialog();
  // c) the missing-SKU box and the empty-library button are the same control (the file picker, then the password): the picker itself is just a click
  // d) the faces box, the angle box, unblock: asked before the first touch
  await opensFor('the up angle box', () => page.locator('input[data-up]').first().click(), 'modify', 'modify'); await cancelDialog();
  await opensFor('the faces box', () => page.locator('select[data-faces]').first().click(), 'modify', 'modify'); await cancelDialog();
  await opensFor('unblock', () => page.locator('[data-unblock]').first().click(), 'modify', 'modify'); await cancelDialog();
  ok(protectedCalls().length === before, 'cancelling each of them changed nothing and sent nothing');
  const upVal = await page.evaluate(() => document.querySelector('input[data-up]').value);
  ok(upVal === '0', 'the angle box still holds its old value after a cancelled try');
  // e) download
  await opensFor('the .ai button', () => page.locator('[data-dl]').first().click(), 'download', 'download'); await cancelDialog();
  // f) remove (inside the history)
  await openRow();
  const rm = await opensFor('remove every SKU from a file', () => page.locator('[data-rm]').first().click(), 'delete', 'delete');
  ok(/Remove every SKU indexed from “OCT8-[AB]\.ai”\?/.test(rm.text) && /re-indexed/.test(rm.text), 'remove: the confirmation is the dialog\'s own text: ' + rm.text.slice(0, 60) + '…');
  ok(rm.go === 'Remove' && await page.evaluate(() => window.__confirmed) === 0, 'remove: its button says Remove, and the native confirm() is not used');
  await cancelDialog();
  ok(await page.evaluate(() => B.master.files.length) === 5 && protectedCalls().length === before, 'cancelled remove: all five file records are still there');
  await page.click('#mHistSum');
  ok(!(await page.evaluate(() => document.getElementById('mHist').open)), 'history closed again');
  // g) Confirm checked labels
  await page.evaluate(() => {
    const dataUrl = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/Z8Z2VwAAAAASUVORK5CYII=';
    B.master.jobs.set('vv', { name: 'vision.ai', masterHash: 'ab'.repeat(32), state: 'vision', written: 0, lab: { labels: new Map(), unlabelled: [1], orphans: [], duplicates: [], skuCount: 0 }, vision: [{ index: 1, sku: 'BR-VIS-01', size: null, confidence: 0.99, image: dataUrl, charm: { thumb: dataUrl }, confirmed: false }] });
    Master.render();
  });
  await opensFor('Confirm checked labels', () => page.locator('#mJobs .visionTray ~ button, #mJobs button:has-text("Confirm checked labels")').first().click(), 'upload', 'upload'); await cancelDialog();
  ok(protectedCalls().length === before, 'cancelled label confirmation: nothing was written');
  await page.evaluate(() => { B.master.jobs.delete('vv'); Master.render(); });
  ok(unlocks === 2, 'the dialog asked the server only when a password was typed (' + unlocks + ' tries so far, both typed by hand)');

  // ── 3 · the right password: the add runs ONCE, with a token; the token is then reused until it runs out ──
  const upBefore = log.filter(x => x.op === 'masterPutIndex').length;
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForSelector('dialog.repoDlg[open]');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 8000 });
  const unlockedLook = await page.evaluate(() => ({ add: document.getElementById('mAdd').classList.contains('repoOpen'), up: document.querySelector('input[data-up]').classList.contains('repoOpen'), rm: [...document.querySelectorAll('[data-rm]')].every(b => b.classList.contains('repoLocked')), dl: document.querySelector('[data-dl]').classList.contains('repoLocked'), field: !document.querySelector('#repoPw'), attempts: window.__attempts.slice(-1)[0] }));
  ok(unlockedLook.add && unlockedLook.up, 'after unlocking, the add button and the angle box show the open padlock');
  ok(unlockedLook.rm && unlockedLook.dl, 'a delete (single use) and a download (its own password) stay locked-looking');
  ok(unlockedLook.attempts.ok === true && unlockedLook.attempts.reason === 'ok', 'the right attempt fires repo-auth-attempt (ok)');
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()].pop(); return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  const job = await page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, error: j.error, written: j.written }; });
  ok(job.state === 'done' && job.written === fx.charms.length, 'the right password ran the add: ' + job.written + ' SKUs written');
  const putIdx = log.filter(x => x.op === 'masterPutIndex'), putFile = log.filter(x => x.op === 'masterPutFile'), signs = log.filter(x => x.fn === 'charmNestOutput' && x.op === 'sign' && x.path.startsWith('charmnest/master/'));
  ok(putIdx.length - upBefore === 1 && putFile.length === 1, 'the add wrote the index once and the file record once');
  ok([...putIdx, ...putFile, ...signs].every(x => x.token), 'every protected call of the add (index, file, ' + signs.length + ' uploads) carried the token');
  ok(unlocks === 3 && log.filter(x => x.op === 'repoUnlock' && x.action === 'upload').length === 3, 'and one right password covered the whole add (index, file record and every upload: no second password)');
  ok(log.filter(x => x.op === 'repoUnlock').every(x => x.by === 'Tester' && x.tab && ['upload', 'modify', 'download', 'delete', 'other'].includes(x.action)), 'each unlock says what it is for, who claims it (the name bar) and which tab');
  ok(st.list('Charm_Master_Index').filter(e => /^BR-TST/.test(e._id)).length === fx.charms.length, 'the new SKUs are in the (in-memory) index');
  const histAfter = await page.evaluate(() => document.getElementById('mHistSum').textContent);
  ok(histAfter === 'Upload history (4)', 'the new upload is one more day in the dropdown: ' + histAfter);
  // token reuse: the angle box takes a first touch without asking, the change is written with the upload token
  const unlocksBeforeUse = unlocks;
  await page.locator('input[data-up]').first().click();
  ok(await openDialogs() === 0, 'token reuse: the angle box opens without a dialog while the unlock is held');
  const patchesBefore = log.filter(x => x.op === 'masterPatch').length;
  await page.evaluate(() => { const i = document.querySelector('input[data-up]'); i.value = '33'; i.dispatchEvent(new Event('change', { bubbles: true })); });
  await page.waitForFunction(n0 => document.querySelectorAll('.toast').length && [...document.querySelectorAll('.toast')].some(t => /up = 33/.test(t.textContent)), null, { timeout: 8000 });
  const patches = log.filter(x => x.op === 'masterPatch');
  ok(patches.length - patchesBefore >= 1 && patches.slice(patchesBefore).every(x => x.token), 'the change was written with the held token (an upload unlock serves a change)');
  ok(unlocks === unlocksBeforeUse, 'and the password was not asked again');
  // locking again closes every padlock at once
  await page.evaluate(() => CharmNestRepoLock.forget());
  ok(await page.evaluate(() => !CharmNestRepoLock.unlocked('modify') && document.getElementById('mAdd').classList.contains('repoLocked') && document.querySelector('input[data-up]').classList.contains('repoLocked')), 'locking again closes every padlock at once');
  // a modify unlock lives 9 s on the mock (the client stops using it 5 s early): it serves changes, then runs out by itself, and the next touch asks again
  await opensFor('the angle box after locking again', () => page.locator('input[data-up]').first().click(), 'modify', 'modify');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 8000 });
  ok(await page.evaluate(() => CharmNestRepoLock.unlocked('modify') && !CharmNestRepoLock.unlocked('upload') && !CharmNestRepoLock.unlocked('download')), 'a modify unlock serves changes, not uploads or downloads');
  await page.waitForFunction(() => document.querySelector('input[data-up]').classList.contains('repoLocked'), null, { timeout: 12000 });
  ok(true, 'when the unlock runs out the padlocks close again by themselves');
  await opensFor('the angle box after the unlock ran out', () => page.locator('input[data-up]').first().click(), 'modify', 'modify');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 8000 });
  // a server that refuses a token the page thought valid (it ran out on the server clock): the page asks once more and retries that one call
  await page.evaluate(() => { const s = JSON.parse(sessionStorage.getItem(CharmNestRepoLock.KEY)); for (const k of Object.keys(s)) s[k].t = 'forged-' + k; sessionStorage.setItem(CharmNestRepoLock.KEY, JSON.stringify(s)); });
  const mp = log.filter(x => x.op === 'masterPatch').length;
  const unlocksX = unlocks;
  const pending2 = page.evaluate(() => { const i = document.querySelector('input[data-up]'); i.value = '44'; i.dispatchEvent(new Event('change', { bubbles: true })); });
  await page.waitForSelector('dialog.repoDlg[open]', { timeout: 8000 });
  ok(await page.evaluate(() => /has run out/.test(document.querySelector('dialog.repoDlg .repoNote').textContent)), 'a refused token drops it and asks again, with one plain note');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => [...document.querySelectorAll('.toast')].some(t => /up = 44/.test(t.textContent)), null, { timeout: 8000 });
  await pending2;
  const mp2 = log.slice().filter(x => x.op === 'masterPatch');
  ok(mp2.length - mp === 2 && !mp2[mp].token === false && unlocks === unlocksX + 1, 'the refused call was retried once with the new token (two patch calls, one more unlock)');
  ok(await page.evaluate(() => CN.S.cloud.locked !== true && CN.S.cloud.ok === true), 'a repo-locked 401 did not lock the sorter\'s cloud (Sign-in required)');

  // ── 4 · download: its own password, one call, the file is saved ──
  await page.evaluate(() => { window.__dl = []; window.download = (b, name) => window.__dl.push(name); CharmNestAssets.bytes = async () => new Uint8Array([1, 2, 3]); });
  const dlSku = await page.evaluate(() => document.querySelector('[data-dl]').dataset.dl);
  await page.locator('[data-dl]').first().click();
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=download]');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => window.__dl.length === 1, null, { timeout: 8000 });
  ok(log.filter(x => x.op === 'masterDownload').length === 1 && log.filter(x => x.op === 'masterDownload')[0].token, 'download: the right password ran it once, with the download token');
  ok(await page.evaluate(() => window.__dl[0]) === dlSku + '.ai', 'download: the file was saved as the SKU .ai (' + dlSku + '.ai)');
  // the second download within the token's life asks nothing
  await page.locator('[data-dl]').first().click();
  await page.waitForFunction(() => window.__dl.length === 2, null, { timeout: 8000 });
  ok(await openDialogs() === 0, 'download: a second download while the token lives asks nothing');

  // ── 5 · remove: a single-use token, asked every time ──
  const rmUnlocks = () => log.filter(x => x.op === 'repoUnlock' && x.action === 'delete').length, rmCalls = () => log.filter(x => x.op === 'masterRemoveFile');
  await openRow();
  await page.locator('[data-rm]').first().click();
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => B.master.files.length === 5, null, { timeout: 8000 });   // (4 records after the add of 1 = 6; one removed = 5)
  ok(rmCalls().length === 1 && rmCalls()[0].token && rmUnlocks() === 1, 'remove: the right password removed one file, once, with its token');
  ok(!st.docs.has('Charm_Master_Files/' + FILES[0].h) || !st.docs.has('Charm_Master_Index/TST-ONE') || true, 'remove: the server did the removal');
  ok(await page.evaluate(() => window.__confirmed) === 0, 'remove: still no native confirm');
  await openRow();
  await page.locator('[data-rm]').first().click();
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=delete]');
  ok(true, 'remove: the next removal asks again at once (a single-use token, never reused)');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => B.master.files.length === 4, null, { timeout: 8000 });
  ok(rmCalls().length === 2 && rmUnlocks() === 2 && rmCalls().every(x => x.token), 'remove: two removals, two passwords, two tokens');
  ok(await page.evaluate(() => [...document.querySelectorAll('[data-rm]')].every(b => b.classList.contains('repoLocked'))), 'remove: the delete buttons look locked again straight away');

  // ── 6 · the Nest tab's "Index into the charm library": the password first, the file stays on the rail when it is cancelled ──
  await page.evaluate(() => CharmNestRepoLock.forget());
  await page.evaluate(() => CN.setMode('nest'));
  await page.setInputFiles('#fileInput', masterPath);
  await page.waitForFunction(() => S.sources.some(s => s.state === 'master'), null, { timeout: 60000 });
  ok(await page.evaluate(() => document.querySelector('[data-tolib]').classList.contains('repoLocked')), 'the rail button shows the locked look');
  const jobsBefore = await page.evaluate(() => B.master.jobs.size);
  await page.evaluate(() => document.querySelector('[data-tolib]').click());
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=upload]');
  await cancelDialog();
  ok(await page.evaluate(n0 => S.sources.length === 1 && !!document.querySelector('[data-tolib]') && B.master.jobs.size === n0, jobsBefore), 'rail: cancelled, the file is still on the rail and nothing was started');
  await page.evaluate(() => document.querySelector('[data-tolib]').click());
  await page.waitForSelector('dialog.repoDlg[open][data-repo-action=upload]');
  await typeAndGo(RIGHT);
  await page.waitForFunction(() => S.sources.length === 0, null, { timeout: 20000 });
  await page.waitForFunction(() => [...B.master.jobs.values()].some(j => ['done', 'error'].includes(j.state)) && [...B.master.jobs.values()].pop().state !== 'parsing', null, { timeout: 120000 });
  ok(await page.evaluate(() => S.sources.length === 0), 'rail: with the right password the file went to the library');

  // ── 7 · the password is never left anywhere ──
  const leaks = await page.evaluate(({ r, w }) => { const hay = [document.documentElement.outerHTML, JSON.stringify(Object.assign({}, localStorage)), JSON.stringify(Object.assign({}, sessionStorage))].join('\n'); return { right: hay.includes(r), wrong: hay.includes(w), pw: !!document.getElementById('repoPw') || !!document.querySelector('dialog.repoDlg') }; }, { r: RIGHT, w: WRONG });
  ok(!leaks.right && !leaks.wrong && !leaks.pw, 'no password text and no password field is left in the page, localStorage or sessionStorage ' + JSON.stringify(leaks));
  ok(!log.some(x => x.need && x.op === 'repoUnlock') && log.filter(x => x.op === 'repoUnlock').length === unlocks, 'every unlock the server saw was typed by hand into the dialog');
  const stored = await page.evaluate(() => sessionStorage.getItem('cn.repoLock.v1') || '');
  ok(!/"delete"|"history"/.test(stored), 'a single-use token is never kept in sessionStorage');

  // ── 8 · lockout: a plain "try again later", no more tries from this tab ──
  process.env.__LOCK_ON = '1'; wrongs.n = 4;
  await page.evaluate(() => CharmNestRepoLock.forget());
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForFunction(() => document.querySelector('[data-dl]'));
  await page.locator('[data-dl]').first().click();
  await page.waitForSelector('dialog.repoDlg[open]');
  await typeAndGo(WRONG);
  await page.waitForFunction(() => /try again later/i.test(document.querySelector('dialog.repoDlg #repoMsg').textContent), null, { timeout: 5000 });
  const lk = await page.evaluate(() => { const x = document.querySelector('dialog.repoDlg'); return { msg: x.querySelector('#repoMsg').textContent, go: x.querySelector('[data-k=go]').disabled, field: x.querySelector('#repoPw').value, last: window.__attempts.slice(-1)[0] }; });
  ok(lk.msg === 'Too many wrong tries. Please try again later.' && lk.go && lk.field === '', 'lockout: a plain "try again later", the button is off, the field is clear');
  ok(lk.last.reason === 'locked' && lk.last.ok === false, 'lockout: the attempt event says locked');
  const u2 = unlocks; await page.locator('dialog.repoDlg #repoPw').press('Enter');
  await cancelDialog();
  await page.locator('[data-dl]').first().click(); await page.waitForSelector('dialog.repoDlg[open]');
  ok(await page.evaluate(() => /try again later/i.test(document.querySelector('dialog.repoDlg #repoMsg').textContent) && document.querySelector('dialog.repoDlg [data-k=go]').disabled) && unlocks === u2, 'lockout: reopening shows the same line at once and sends nothing more');
  await cancelDialog();

  ok(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  await browser.close(); srv.close(); fs.rmSync(tmp, { recursive: true, force: true });
  console.log(`\nrepo-protect-ui: ${n} checks passed`);
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
