// LETTERS-HOOPS (Paul, 9 Nov 2026): what Paul sees and presses for the 52 new letter charms, in real Chromium on the stand-in site (tests/charm-nest/bridge-server.cjs: the REAL charmNestLibrary and
// charmNestOutput handlers on an in-memory Firestore and Storage; the repo password door is the real one with the stand-in's throwaway operator password, REPO_TEST). OFFLINE: every request that is not
// loopback is aborted and counted (there must be none: no Etsy, no Firebase, nothing).
// It proves: Workspace › Master files shows ONE line under "＋ Add a master file": "New charms ready: Letter charms with hoops …" with "Add these charms to the repo" (marked as a repo upload control) and a link to
// the contact sheet (which opens and shows the 26 letters with their 52 pictures); looking sends nothing; the press shows a plain confirm; "Yes, add them" opens the ONE password dialog; with the password the
// 52 charms land in the library (index, files, upload history) and the line says "Done: 52 charms added …"; pressing again says "Already there" and writes nothing; no page error.
//   node tests/charm-nest/repo-adds-ui.cjs      (PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> to choose the browser; skipped without one)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
let chromium = null, exe;
try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(root, 'node_modules'), 'playwright-core'))); exe = process.env.CHROMIUM; } catch (_) { try { ({ chromium } = require('playwright')); } catch (e) { console.log('skipped: no playwright'); process.exit(0); } }
const { start, REPO_TEST } = require('./bridge-server.cjs');
const TITLE = JSON.parse(fs.readFileSync(path.join(root, 'charm-repo-adds/queue.json'), 'utf8')).adds.find(a => a.id === 'letters-hoops-2026-11-09').title;
const rx = t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const SHOTS = process.env.SHOTS_DIR || '';

(async () => {
  const srv = await start({ receipts: [], repoSeed: false });
  const { st, sorterOrigin } = srv;
  st.repoStrict = true; process.env.CHARM_REPO_PASSWORD = REPO_TEST.password;
  const browser = await chromium.launch(Object.assign({ args: ['--no-sandbox'] }, exe ? { executablePath: exe } : {}));
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 }, timezoneId: 'UTC', locale: 'en-US' });
  const outside = [];
  await ctx.route(/^https?:\/\/(?!127\.0\.0\.1|localhost)/, r => { outside.push(r.request().url()); return r.abort(); });
  // (the page is served the letters add offered, to show the press; then marked done, to show the line goes away)
  let queueNow = false;
  await ctx.route(/\/charm-repo-adds\/queue\.json/, r => { const q = JSON.parse(fs.readFileSync(path.join(root, 'charm-repo-adds/queue.json'), 'utf8')); for (const a of q.adds) { if (queueNow) a.done = 'test'; else delete a.done; } return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(q) }); });
  const lib = [];
  await ctx.route(/\/\.netlify\/functions\/(charmNestLibrary|charmNestOutput)$/, async route => { let b = {}; try { b = route.request().postDataJSON() || {}; } catch (_) {} lib.push({ fn: route.request().url().split('/').pop(), op: b.op }); return route.fallback(); });
  await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Paul K'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'Paul K'; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('pageerror: ' + e.message));
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.Master && window.CharmNestRepoLock && window.CharmNestRepoAdds && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; });
  // Paul's way in: the Workspace menu (top right) › Master files
  await page.click('#moreMenu > summary');
  await page.click('#moreMenu button[data-mode="master"]');
  await page.waitForSelector('#mAdds .raBox', { timeout: 30000 });
  const box = await page.evaluate(() => { const b = document.querySelector('#mAdds .raBox'), r = b.getBoundingClientRect(), add = document.querySelector('#mAdd'); const go = [...b.querySelectorAll('button')].find(x => !x.hidden);
    return { head: b.querySelector('.raHead').textContent, go: go && go.textContent, repo: go && go.dataset.repo, link: b.querySelector('a') && b.querySelector('a').textContent, href: b.querySelector('a') && b.querySelector('a').getAttribute('href'), seen: r.width > 0 && r.height > 0, below: add && add.getBoundingClientRect().top < r.top, count: document.querySelectorAll('#mAdds .raBox').length }; });
  ok(box.count === 1 && box.seen && box.below, 'Workspace › Master files shows ONE new-charms line, under "＋ Add a master file"');
  ok(box.head === 'New charms ready: ' + TITLE + '.', 'it says: ' + box.head);
  ok(box.go === 'Add these charms to the repo' && box.repo === 'upload', 'its button: "Add these charms to the repo", a repo upload control');
  ok(box.link === 'See them first (contact sheet)' && box.href === 'charm-repo-adds/letters-hoops-2026-11-09/contact-sheet.html', 'and a link to the contact sheet');
  if (SHOTS) await page.locator('#masterView').screenshot({ path: path.join(SHOTS, 'repo-adds-line.png') });
  ok(!lib.some(x => /^master(PutIndex|PutFile)$/.test(x.op) || x.fn === 'charmNestOutput' || x.op === 'repoUnlock'), 'looking sent nothing to the library and asked for no password');

  // the contact sheet opens and shows every letter with its two new charms
  { const cs = await ctx.newPage(); await cs.goto(`${sorterOrigin}/${box.href}`); await cs.waitForLoadState('load');
    const facts = await cs.evaluate(async () => { const imgs = [...document.images]; await Promise.all(imgs.map(i => i.complete ? 0 : new Promise(r => { i.onload = i.onerror = r; }))); return { rows: document.querySelectorAll('section.r').length, imgs: imgs.length, broken: imgs.filter(i => !i.naturalWidth).map(i => i.getAttribute('src')), title: document.title }; });
    ok(facts.title === 'Letter charms with hoops' && facts.rows === 26 && facts.imgs === 78 && !facts.broken.length, 'the contact sheet shows 26 letters: each source and its two new charms (78 pictures, none broken)');
    if (SHOTS) await cs.screenshot({ path: path.join(SHOTS, 'contact-sheet.png'), fullPage: false });
    await cs.close(); }

  // the press: a plain confirm, then ONE password dialog
  await page.click('#mAdds .raBox button:has-text("Add these charms to the repo")');
  const say1 = await page.textContent('#mAdds .raSay');
  ok(/^These charms will be put in the charm repo, each with its picture\. Only their own earlier drawings are replaced, never another charm; one already there is skipped\. You will be asked for the repo password once\.$/.test(say1), 'the press shows one plain confirm line');
  ok(!lib.some(x => x.op === 'masterPutIndex'), 'nothing is sent before "Yes"');
  await page.click('#mAdds .raBox button:has-text("Yes, add them")');
  await page.waitForSelector('dialog.repoDlg[open] #repoPw', { timeout: 15000 });
  ok(await page.evaluate(() => document.querySelectorAll('dialog[open]').length) === 1, '"Yes, add them" opens the ONE repo password dialog');
  await page.fill('dialog.repoDlg[open] #repoPw', REPO_TEST.password); await page.press('dialog.repoDlg[open] #repoPw', 'Enter');
  await page.waitForFunction(() => /^(Done|Stopped|Nothing|The files|\d+ of)/.test((document.querySelector('#mAdds .raSay') || {}).textContent || ''), null, { timeout: 120000 });
  const done = await page.textContent('#mAdds .raSay');
  ok(done === `Done: 52 charms added to the charm repo. All 52 of ${TITLE} are in the Library now.`, 'the line says: ' + done);
  const ix = st.list('Charm_Master_Index');
  ok(ix.length === 52 && ix.every(e => e.masterName === 'LETTER-HOOPS_2026-1109.ai' && e.aiPath && st.blobs.has(e.aiPath) && st.blobs.has(e.thumbPath)), 'the library holds the 52 charms, each with its .ai and picture in Storage');
  ok(st.list('Charm_Master_Files').some(f => f.name === 'LETTER-HOOPS_2026-1109.ai' && f.charms === 52), 'and the upload history has the file');
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForFunction(() => [...document.querySelectorAll('#mGrid .skuTile')].some(t => /without hoop/.test(t.textContent)), null, { timeout: 30000 }).catch(() => {});
  const meta = await page.evaluate(() => { const t = document.querySelector('#mGrid .skuTile[data-sku="LETTER_NECKLACE-10 (HUGGIE)"]'); const m = t && t.querySelector('.meta'); return m ? m.textContent : ''; });
  ok(/^5\.5 × 7\.0 mm without hoop/.test(meta), 'a letter card shows its size WITHOUT the hoop: ' + meta.slice(0, 40));
  if (SHOTS) await page.locator('#masterView').screenshot({ path: path.join(SHOTS, 'repo-adds-done.png') });

  // the second press
  const before = lib.length;
  await page.click('#mAdds .raBox button:has-text("Add these charms to the repo")');
  await page.click('#mAdds .raBox button:has-text("Yes, add them")');
  await page.waitForFunction(() => /^Already there/.test((document.querySelector('#mAdds .raSay') || {}).textContent || ''), null, { timeout: 30000 });
  ok((await page.textContent('#mAdds .raSay')) === `Already there: all 52 charms of ${TITLE} are in the charm repo. Nothing was written.`, 'the second press says "Already there"');
  ok(!lib.slice(before).some(x => x.op === 'masterPutIndex' || x.op === 'masterPutFile' || x.fn === 'charmNestOutput'), 'and writes nothing');
  // the queue as committed now (the letters add done): after a reload the line is gone
  queueNow = true; await page.reload(); await page.waitForFunction(() => window.CN && window.CharmNestRepoAdds && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mAdd', { timeout: 30000 }); await page.waitForTimeout(1500);
  ok(await page.evaluate(() => document.querySelectorAll('#mAdds .raBox').length) === 0, 'with the add marked done, the line is gone from Master files');
  ok(!outside.length, 'no request left this computer (' + outside.length + ')');
  ok(!errors.length, 'no page error' + (errors.length ? ': ' + errors.join(' | ') : ''));
  await browser.close(); await srv.close();
  console.log(`repo-adds-ui: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
