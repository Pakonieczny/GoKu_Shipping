// ETSYSTOP (Paul, 20 Nov 2026: "a manual reset somewhere in the settings that will allow a user to reset all of the applications running on all
// computers to resume pulling from Etsy. I should be able to select this on my computer and then all the other computers should comply through the cloud.")
// The real page in headless Chromium against the repo's fake server (bridge-server.cjs); the cloud switch (etsyStopGet / etsyResume) is answered here.
//   1 · the switch on: Settings → Etsy reads "Stopped since …", names the cause, and shows Resume Etsy
//   2 · Resume Etsy: one etsyResume to the cloud, the line reads "Running · resumed …", the button goes
//   3 · the switch off: "Running", no button
//   4 · this computer's run, stopped by Etsy's 429, starts again once the switch was resumed after that stop; not for an older resume, not
//       for a run stopped for another reason
//   node tests/charm-nest/settings-etsy-stop.cjs [playwright-core dir]
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, unlockSettings } = require('./bridge-server.cjs');

async function main() {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope, onAuthStateChanged = nope, getFirestore = nope, doc = nope, getDoc = nope, setDoc = nope, collection = nope, query = nope, where = nope, getDocs = nope, onSnapshot = nope, serverTimestamp = nope, deleteObject = nope;";
  const errors = [];
  const srv = await start({ receipts: [] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.alert = () => {}; }, { sorter: srv.sorterOrigin });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));

    // the cloud switch
    const since = Date.now() - 3600e3;
    let sw = { stopped: true, since, fn: 'etsyOrderProxy', path: '/v3/application/shops/1/receipts/4229881522', detail: '{"error":"Exceeded daily rate limit"}', resumedAt: 0, resumedBy: '', rev: 1 };
    const sent = [];
    await page.route(/\/\.netlify\/functions\/charmNestLibrary/, r => {
      let b = null; try { b = JSON.parse(r.request().postData() || 'null'); } catch (_) { b = null; }
      if (b && b.op === 'etsyStopGet') { sent.push('get'); return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, ...sw }) }); }
      if (b && b.op === 'etsyResume') { sent.push('resume:' + (b.by || '')); sw = { ...sw, stopped: false, resumedAt: Date.now(), resumedBy: b.by || 'someone', rev: sw.rev + 1 }; return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, ...sw }) }); }
      return r.continue();
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.Session && Session.ready() && window.EtsyStopUI, null, { timeout: 60000 });
    const open = async () => { await page.evaluate(() => { const d = document.getElementById('dlgSettings'); if (d && d.open) closeDlg(d); openSettings(); }); await unlockSettings(page); };
    const read = () => page.evaluate(() => { const p = document.getElementById('stEtsyState'), b = document.getElementById('btnEtsyResume'), h = document.getElementById('stEtsyHelp'), sec = [...document.querySelectorAll('#dlgSettings .section')].map(x => x.textContent); return { pill: p.textContent, cls: p.className, button: !b.hidden && b.getBoundingClientRect().width > 0, label: b.textContent, help: h.textContent, first: sec[0] }; });

    /* 1 · stopped */
    await open();
    await page.waitForFunction(() => /Stopped since/.test(document.getElementById('stEtsyState').textContent), null, { timeout: 10000 });
    const s1 = await read(); console.log('1 stopped:', s1.pill, '|', s1.help.slice(0, 90));
    assert(s1.first === 'Etsy', 'the Etsy section is the first of Settings: ' + s1.first);
    assert(/pill bad/.test(s1.cls) && s1.button && s1.label === 'Resume Etsy', 'red "Stopped since", with the Resume Etsy button: ' + JSON.stringify(s1));
    assert(/429/.test(s1.help) && /etsyOrderProxy/.test(s1.help) && /daily rate limit/.test(s1.help), 'the cause is named: ' + s1.help);
    assert.deepStrictEqual(sent, ['get'], 'opening Settings only reads the switch');

    /* 2 · Resume Etsy */
    await page.click('#btnEtsyResume');
    await page.waitForFunction(() => /Running/.test(document.getElementById('stEtsyState').textContent), null, { timeout: 10000 });
    const s2 = await read(); console.log('2 resumed:', s2.pill);
    assert.strictEqual(sent.filter(x => x.startsWith('resume')).length, 1, 'one etsyResume to the cloud: ' + JSON.stringify(sent));
    assert(/pill ok/.test(s2.cls) && /resumed .* by Tester/.test(s2.pill) && sent.includes('resume:Tester') && !s2.button, 'green "Running · resumed …", no button: ' + JSON.stringify(s2));
    const toastSeen = await page.evaluate(() => /Etsy resumed for every computer/.test(document.body.innerText));
    assert(toastSeen, 'the press says it reached every computer');

    /* 3 · running, opened again */
    await open(); await page.waitForTimeout(600);
    const s3 = await read(); console.log('3 running:', s3.pill);
    assert(/Running/.test(s3.pill) && !s3.button, 'running: no button');

    /* 4 · the run this computer carries */
    const follow = await page.evaluate(async st => {
      const B = window.B, keep = B.run, RC = window.RunCtl, realResume = RC.resume; let n = 0; RC.resume = async () => { n++; };
      const out = {};
      try {
        const run = t => ({ runId: 'run-x', status: 'stopped', stoppedBy: 'New orders could not be added: Cannot verify Etsy open orders: HTTP 429 – {"error":"etsy_stopped"}', errors: [{ t, why: 'New orders could not be added: Cannot verify Etsy open orders: HTTP 429 – {"error":"etsy_stopped"}' }] });
        B.run = run(st.resumedAt - 60000); out.after = await EtsyStopUI.follow(st); out.nAfter = n;
        B.run = run(st.resumedAt + 60000); out.older = await EtsyStopUI.follow(st); out.nOlder = n;
        B.run = Object.assign(run(st.resumedAt - 60000), { stoppedBy: 'Nest and save this layout before preparing its contour' }); out.other = await EtsyStopUI.follow(st); out.nOther = n;
        B.run = run(st.resumedAt - 60000); out.still = await EtsyStopUI.follow({ ...st, stopped: true }); out.nStill = n;
      } finally { B.run = keep; RC.resume = realResume; }
      return out;
    }, sw);
    console.log('4 run:', JSON.stringify(follow));
    assert(follow.after === true && follow.nAfter === 1, 'a run stopped by Etsy before the resume starts again');
    assert(follow.older === false && follow.nOlder === 1, 'a run stopped after the last resume waits for the next one');
    assert(follow.other === false && follow.nOther === 1, 'a run stopped for another reason is left alone');
    assert(follow.still === false && follow.nStill === 1, 'a switch still on resumes nothing');

    assert.deepStrictEqual(errors, [], 'no script error: ' + errors.join(' | '));
    await ctx.close();
  } finally { srv.close(); await browser.close(); }
  console.log('settings-etsy-stop: all checks passed');
}
main().catch(e => { console.error(e); process.exit(1); });
