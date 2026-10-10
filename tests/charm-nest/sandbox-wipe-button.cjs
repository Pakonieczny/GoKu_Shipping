// The Settings tab's three buttons are ONE complete wipe (Paul, 10 Oct 2026: "None of these options actually fully delete all
// memory of the Sandbox testing ... The only thing should remain is the employee efficiency and the Charm repo").
// The real page in real headless Chromium, offline, against the repo's fake server (bridge-server.cjs: the real
// charmNestLibrary handler over an in-memory Firestore and Storage). Nothing here touches a live site.
// A dirty sandbox is built from the ONE registry of families (charm-nest-sandbox-families.js: every Sandbox_ collection, the
// shared places' sandbox marks, the stream and snapshot pointer, the sandbox's files), beside a production twin of each
// and the protected records (Charm repo, employee efficiency, config, the Etsy pull budget). Then, one fresh server and
// page each, the real buttons are pressed:
//   1 · "Reset sandbox records"            (sandbox on)
//   2 · "Reset the sandbox…"               (sandbox on)       -> the same requests as 1
//   3 · "Purge all run history…"           (sandbox on)       -> production's run history as before, then the same wipe
//   4 · "Reset sandbox records"            (sandbox OFF: a production page) -> same wipe, no reload, its own state untouched
//   5 · "Purge all run history…" with a wrong passcode -> nothing is deleted
// and, each time: every family of the registry reads 0 in the "In the sandbox now" line (not only the nine that held
// something), the line "Kept, never wiped" shows the Charm repo and employee efficiency counts, nothing was written after
// the final delete (the line is a read), no sandbox document or file is left, and every production and protected
// document is byte-identical. The words of the buttons say what is wiped and what stays.
//   node tests/charm-nest/sandbox-wipe-button.cjs [playwright-core dir]
'use strict';
process.env.CHARM_NEST_DELETE_CODE = 'wipebutton-test-code';   // (the purge's passcode for this run only, set before the handlers load)
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const FAM = require(path.join(root, 'charm-nest-sandbox-families.js'));
const CODE = process.env.CHARM_NEST_DELETE_CODE;
const WRITES = /^(pool|back|set|putSheet|deleteSheet|run|release|arrival|custom|cancel|timelineAdd|sandboxStream|sandboxPut|sandboxCancel|put|alias|noDesign|optionMap|start|laser|rose|flow|purge)/;
const READS = /(Status)$/;

/** A dirty sandbox, its production twin and the protected records, in the fake's Firestore and Storage. */
function seed(st) {
  const now = Date.now(), blob = s => ({ buf: Buffer.from(s), generation: 1, meta: { contentType: 'application/json', metadata: {} } });
  for (const f of FAM.server().filter(x => x.store === 'firestore')) {
    for (const id of ['seed-1', 'seed-2']) st.put('Sandbox_' + f.key, id, { seeded: true, id, at: now });
    for (const sub of f.subs || []) st.put(`Sandbox_${f.key}/seed-1/${sub}`, 'row-1', { seeded: true, at: now });
    st.put(f.key, 'prod-1', { prod: true, id: 'prod-1', family: f.key });   // (production's own record of the same family, under no sandbox mark)
  }
  st.put('EtsyMail_OrderLinks', 'olsb_4170000001_o_a1', { id: 'olsb_4170000001_o_a1', sandbox: true, receiptId: '4170000001' });
  st.put('EtsyMail_OrderLinks', 'ol_4170000001_o_a1', { id: 'ol_4170000001_o_a1', sandbox: false, receiptId: '4170000001' });
  st.put('Charm_Sandbox', 'current', { path: 'charmnest/sandbox/orders-old.json', count: 360, at: now, takenBy: 'test' });
  st.put('Charm_Sandbox', 'stream', { on: true, seed: 7, tick: 4 });
  st.put('Charm_Sandbox', 'pulls', { day: '2026-10-10', n: 2 });   // (the Etsy budget: stays)
  st.blobs.set('charmnest/sandbox/orders-old.json', blob('{"receipts":[]}'));
  st.blobs.set('charmnest/sandbox/sheets/a.json', blob('{"a":1}'));
  st.blobs.set('design-archive/sandbox/x.json', blob('{"x":1}'));
  st.blobs.set('charmnest/sandbox/master/keep.json', blob('{"master":1}'));
  st.blobs.set('charmnest/master/m1.json', blob('{"master":2}'));
  // what stays: the Charm repo (3 designs), employee efficiency (5 daily records, 2 of them the sandbox's), config
  for (let i = 1; i <= 3; i++) st.put('Charm_Master_Index', 'design-' + i, { sku: 'D' + i, at: now });
  for (let i = 1; i <= 5; i++) st.put('Efficiency_Daily', 'eff-' + i, { person: 'P' + i, day: '2026-10-0' + i });
  for (let i = 1; i <= 2; i++) st.put('Sandbox_Efficiency_Daily', 'seff-' + i, { person: 'S' + i, day: '2026-10-0' + i });
  st.put('Station_Activity', 'act-1', { person: 'P1', action: 'scan' });
  st.put('config', 'editPasscode', { passcode: 'x-not-a-real-code' });
}
const isSandboxDoc = k => /^Sandbox_/.test(k) && !/^Sandbox_(Efficiency_Daily|Station_Activity|Station_Sessions|Station_Live|Laser_Sheet_Times)\//.test(k)
  || k.startsWith('EtsyMail_OrderLinks/olsb_') || k === 'Charm_Sandbox/stream' || k === 'Charm_Sandbox/current';
const canon = v => JSON.stringify(v, (k, x) => (x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : 1))) : x));
const productionSnap = st => canon([...st.docs.entries()].filter(([k]) => !isSandboxDoc(k)).sort(([a], [b]) => (a < b ? -1 : 1)));
const sandboxLeft = st => [...st.docs.keys()].filter(isSandboxDoc);
const sandboxFilesLeft = st => [...st.blobs.keys()].filter(k => /^(charmnest\/sandbox\/|design-archive\/sandbox\/)/.test(k) && !k.startsWith('charmnest/sandbox/master/'));

async function main() {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const errors = [];

  async function scenario(name, { sandbox, button, passcode, expect }) {
    const srv = await start({ receipts: [] });
    const { st, sorterOrigin, stationOrigin } = srv;
    seed(st);
    const prodBefore = productionSnap(st), prodBlobsBefore = canon([...st.blobs.keys()].filter(k => !/^(charmnest\/sandbox\/|design-archive\/sandbox\/)/.test(k)).sort());
    const dirty = sandboxLeft(st).length, dirtyFiles = sandboxFilesLeft(st).length;
    assert(dirty > 60 && dirtyFiles === 3, `${name}: the sandbox is dirty first (control): ${dirty} documents, ${dirtyFiles} files`);
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.addInitScript(({ sorter, code }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => code; window.alert = () => {}; }, { sorter: sorterOrigin, code: passcode == null ? CODE : passcode });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(`${name}: ${e.message}`));
    const booted = p => p.waitForFunction(() => window.CN && window.Sandbox && window.Session && Session.ready() && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 });
    await page.evaluate(({ station, on }) => {
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: on ? 'on' : 'off', sandboxStream: 'on', dsOrigin: station, pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s));
      if (on) localStorage.setItem('cn.sandboxHold', JSON.stringify({ at: Date.now() }));   // (a quiet sandbox: nothing streams while the buttons are looked at)
    }, { station: stationOrigin, on: sandbox });
    await page.reload(); await booted(page);
    assert.strictEqual(await page.evaluate(() => WORKSPACE_SANDBOX), sandbox, `${name}: the page is in the ${sandbox ? 'sandbox' : 'production'}`);
    await page.evaluate(() => { window.__sameDocument = 'yes'; });

    // ── Settings before: the line shows what the sandbox holds, and what is kept ──
    const line = (p, id) => p.evaluate(i => (document.querySelector(`#stBuildNote [data-i="${i}"]`) || {}).textContent || '', id);
    const openSettings = async p => { await p.evaluate(() => openSettings()); await p.waitForFunction(() => /In the sandbox now: /.test((document.querySelector('#stBuildNote [data-i="now"]') || {}).textContent || '') && /Kept, never wiped/.test((document.querySelector('#stBuildNote [data-i="kept"]') || {}).textContent || ''), null, { timeout: 20000 }); };
    await openSettings(page);
    const before = await line(page, 'now'), keptBefore = await line(page, 'kept');
    assert(/\b[1-9]\d* (sheets|pool rows|runs)\b/.test(before) && !/nothing —/.test(before), `${name}: before, the line lists what the sandbox holds: ${before.slice(0, 200)}`);
    assert.strictEqual(keptBefore, 'Kept, never wiped: Charm repo 3 designs; employee efficiency 5 daily records (2 made in the sandbox).', `${name}: before, the kept line shows the Charm repo and employee efficiency counts`);

    // ── the words of the buttons ──
    const words = await page.evaluate(() => { const t = id => document.getElementById(id).title, f = id => document.getElementById(id).closest('.field').innerText;
      return { sb: t('stSbReset'), rs: t('stSandboxReset'), pg: t('stPurge'), on: t('stSbOn'), rsField: f('stSandboxReset'), pgField: f('stPurge'), build: document.getElementById('stBuildNote').parentElement.innerText }; });
    for (const k of ['sb', 'rs', 'pg']) assert(/Charm repo/.test(words[k]) && /employee efficiency/.test(words[k]), `${name}: the ${k} button's tooltip says what stays: ${words[k]}`);
    assert(/same complete wipe/.test(words.sb) && /same complete wipe/.test(words.rs) && /wipe the sandbox completely/.test(words.pg), `${name}: the three tooltips name the one wipe`);
    assert(/Wipes everything the sandbox made, in the cloud and in this browser\. Stays: the Charm repo, employee efficiency and every real record\./.test(words.rsField), `${name}: the Reset field says plainly what goes and what stays: ${words.rsField}`);
    assert(/Asks for the passcode\. Deletes the real run history .* then wipes the sandbox completely, the same as Reset sandbox records\..*Stays: the Charm repo, employee efficiency/.test(words.pgField) && !/snapshot stay/.test(words.pgField + words.pg), `${name}: the Purge help says what it deletes and what stays: ${words.pgField}`);
    assert(/all end in the same complete wipe/.test(words.build), `${name}: the line under the buttons says all three are one wipe`);
    if (sandbox) assert(/nothing is deleted/.test(words.on) && /Reset sandbox records/.test(words.on) && !/take a snapshot/.test(words.on), `${name}: "Back to the real orders" says it deletes nothing: ${words.on}`);

    // ── the press ──
    const mark = st.calls.length;
    const painted = sandbox && button !== '#stPurge' ? page.waitForFunction(() => /nothing — /.test((document.querySelector('#stBuildNote [data-i="now"]') || {}).textContent || ''), null, { timeout: 60000, polling: 40 }).then(() => true, () => false) : null;
    const nav = sandbox && expect !== 'refused' ? page.waitForNavigation({ waitUntil: 'load', timeout: 90000 }) : null;
    await page.click(button);
    if (expect === 'refused') {
      await page.waitForFunction(() => /Wrong passcode/.test(document.getElementById('stPurgeNote').textContent), null, { timeout: 20000 });
      assert.deepStrictEqual(sandboxLeft(st).length, dirty, `${name}: a wrong passcode deleted nothing`);
      assert.strictEqual(productionSnap(st), prodBefore, `${name}: production is as it was`);
      assert(!st.calls.slice(mark).some(c => c.op === 'sandboxReset'), `${name}: and no wipe was started`);
      await ctx.close(); srv.close(); console.log(`ok: ${name}`); return;
    }
    const why = async () => { const t = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent).concat(['note: ' + (document.getElementById('stSandboxResetNote') || {}).textContent, 'purge: ' + (document.getElementById('stPurgeNote') || {}).textContent])).catch(() => []); return JSON.stringify(t) + ' ops: ' + st.calls.slice(mark).map(c => c.op).filter(Boolean).join(','); };
    try {
      if (nav) { await nav.catch(async e => { throw new Error(`${name}: the page did not reload: ${await why()}`); }); await booted(page); }
      else await page.waitForFunction(() => /Sandbox cleaned/.test(document.getElementById('toasts').innerText), null, { timeout: 60000 }).catch(async () => { throw new Error(`${name}: no "Sandbox cleaned": ${await why()}`); });
    } catch (e) { await new Promise(r => setTimeout(r, 0)); throw e; }
    if (painted) assert.strictEqual(await painted, true, `${name}: the line read "nothing — …" before the page reloaded (the same read as the verify, shown at once)`);
    if (!sandbox) {
      assert.strictEqual(await page.evaluate(() => window.__sameDocument), 'yes', `${name}: a production page does not reload for it`);
      await page.waitForFunction(() => /nothing — /.test((document.querySelector('#stBuildNote [data-i="now"]') || {}).textContent || ''), null, { timeout: 20000 });
    }
    const calls = st.calls.slice(mark), ops = calls.map(c => c.op).filter(Boolean);

    // ── the cloud: nothing of the sandbox is left; production and what is kept are byte-identical ──
    assert.deepStrictEqual(sandboxLeft(st), [], `${name}: no sandbox document is left`);
    assert.deepStrictEqual(sandboxFilesLeft(st), [], `${name}: no sandbox file is left`);
    assert(st.blobs.has('charmnest/sandbox/master/keep.json') && st.blobs.has('charmnest/master/m1.json'), `${name}: the Charm repo's files stay`);
    assert.strictEqual(canon([...st.blobs.keys()].filter(k => !/^(charmnest\/sandbox\/|design-archive\/sandbox\/)/.test(k)).sort()), prodBlobsBefore, `${name}: production files are as they were`);
    const prodAfter = productionSnap(st);
    if (expect === 'purge') {
      // production's run history goes as it always did; nothing else of production changes
      const GONE = ['Charm_Nest_Runs', 'Charm_Nest_Run_Lines', 'Charm_Nest_Run_Live', 'Charm_Nest_Sheets', 'Charm_Nest_Sets', 'Charm_Pool', 'Charm_Pool_Back', 'Charm_Nest_Counters', 'Charm_Nest_Release', 'Design_Bridge'];
      const expectKept = JSON.parse(prodBefore).filter(([k]) => !GONE.some(g => k.startsWith(g + '/'))), got = JSON.parse(prodAfter);
      assert.strictEqual(canon(got), canon(expectKept), `${name}: after the purge production lost its run history and nothing else`);
      for (const g of GONE) assert(!st.docs.has(g + '/prod-1'), `${name}: production's ${g} was purged as before`);
    } else assert.strictEqual(prodAfter, prodBefore, `${name}: every production and protected document is byte-identical`);
    for (const k of ['Charm_Master_Index/design-1', 'Efficiency_Daily/eff-5', 'Sandbox_Efficiency_Daily/seff-2', 'Station_Activity/act-1', 'config/editPasscode', 'Charm_Sandbox/pulls']) assert(st.docs.has(k), `${name}: ${k} stays`);

    // ── the requests: the same wipe for every button; nothing written after the final delete ──
    const lastWipe = ops.lastIndexOf('sandboxReset');
    assert(lastWipe >= 0, `${name}: the page asked the cloud to wipe the sandbox`);
    const after = ops.slice(lastWipe + 1);
    assert(after.includes('sandboxStatus'), `${name}: the cloud was counted after the last delete: ${after}`);
    assert.deepStrictEqual(after.filter(o => WRITES.test(o) && !READS.test(o)), [], `${name}: nothing was written after the final delete (the line is a read): ${after}`);
    if (expect === 'purge') assert(ops.indexOf('purgeHistory') >= 0 && ops.indexOf('purgeHistory') < lastWipe, `${name}: the purge ran first, then the wipe`);
    else assert(!ops.includes('purgeHistory'), `${name}: a Reset never purges production's run history`);
    const shape = ops.filter((o, i) => i === 0 || o !== ops[i - 1]).filter(o => /^(purgeHistory|sandboxReset|sandboxStatus)$/.test(o)).join(' > ');

    // ── Settings after: EVERY family at 0, the kept line, the last-reset line ──
    await openSettings(page);
    const now = await line(page, 'now'), kept = await line(page, 'kept'), last = await line(page, 'last');
    const fams = FAM.server();
    assert(/^In the sandbox now: nothing — /.test(now), `${name}: the line says the sandbox holds nothing: ${now.slice(0, 160)}`);
    const items = now.replace(/^In the sandbox now: nothing — /, '').replace(/\.$/, '').split(/, (?=\d)/);
    assert(items.every(x => /^0\+? /.test(x)), `${name}: every item reads 0: ${items.filter(x => !/^0\+? /.test(x)).join(' | ')}`);
    const missing = fams.filter(f => !now.includes(`0 ${f.label}`)).map(f => f.label);
    assert.deepStrictEqual(missing, [], `${name}: every family of the registry is listed at 0 (${fams.length} of them), missing: ${missing.join(', ')}`);
    assert(!/Not counted by this server/.test(now), `${name}: the server counted every family: ${now.slice(-200)}`);
    assert.strictEqual(kept, 'Kept, never wiped: Charm repo 3 designs; employee efficiency 5 daily records (2 made in the sandbox).', `${name}: the kept line is unchanged by the wipe`);
    assert(/Last (reset|purge) .+ — \d+ cloud record\(s\) and \d+ file\(s\) removed .*nothing left in the cloud \(all \d+ kinds counted at 0\)/.test(last), `${name}: the last-reset line says nothing is left and how many kinds read 0: ${last}`);
    const inside = await page.evaluate(() => { const n = document.getElementById('stBuildNote'), d = document.getElementById('dlgSettings'); return { inside: d.contains(n), open: document.querySelectorAll('dialog[open]').length }; });
    assert(inside.inside && inside.open === 1, `${name}: the lines are inside the one open Settings dialog (no pop-up on a pop-up)`);
    console.log(`ok: ${name} — ${shape}; ${fams.length} families at 0; kept: ${kept}`);
    await ctx.close(); srv.close();
    return shape;
  }

  const a = await scenario('1 Reset sandbox records', { sandbox: true, button: '#stSbReset', expect: 'reset' });
  const b = await scenario('2 Reset the sandbox…', { sandbox: true, button: '#stSandboxReset', expect: 'reset' });
  assert.strictEqual(a, b, 'both Reset buttons make the very same requests: ' + a + ' / ' + b);
  const c = await scenario('3 Purge all run history…', { sandbox: true, button: '#stPurge', expect: 'purge' });
  assert(/sandboxReset/.test(c) && /purgeHistory/.test(c), 'the purge ends in the same wipe: ' + c);
  await scenario('4 Reset sandbox records from a production page', { sandbox: false, button: '#stSbReset', expect: 'reset' });
  await scenario('5 Purge with a wrong passcode', { sandbox: true, button: '#stPurge', passcode: 'wrong', expect: 'refused' });
  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('sandbox wipe button OK: three buttons, one complete wipe; every family at 0 and the kept line after it; production and protected records byte-identical');
  await browser.close(); process.exit(0);
}
main().catch(e => { console.error(e); process.exit(1); });
