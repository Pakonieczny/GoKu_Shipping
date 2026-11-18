// TAKEOFFSOLID (Paul, 15 Nov 2026: "I want you to make sure it's absolutely rock solid ... between the cloud and all the computers"): ONE scenario of two computers.
// The first carries the run; the second follows with live sheets and takes ONE piece off a sheet (SheetWin.takeOffOrder, scope "sheet": the path of the button).
//  · cloud: the piece's row is abandoned with no sheet; the sheet record no longer lists it (poolIds, charms, placements) after the rewrite; its layoutRev went up
//  · the carrier: its in-memory sheet loses the piece, its run never stops, no second run, the order line is held on both computers
//   node tests/charm-nest/takeoff-two-computers.cjs
const assert = require('assert'), fs = require('fs'), path = require('path');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open, repo } = require('./day-sim/harness.cjs');
const W = require('./_autorunWorld.cjs');

const WRITES = /^(putSheet|runPut|runArchive|setUpdate|setAllocate|poolPut|poolUpdate|backPut|backInvalidate|archiveEmptySheet|deleteSheet|putCharms|arrivalRecord|laserDone)$/;
const layoutOf = doc => JSON.stringify((doc.placements || []).map(p => [p.id, +(+p.cxPt).toFixed(2), +(+p.cyPt).toFixed(2), +(+p.angle || 0).toFixed(2)]).sort());

/** A browser context that is another computer: the harness's own routes and start-up script, and storage of its own. */
async function otherComputer(H) {
  const st = H.st, ctx = await H.browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: process.env.SIM_TZ || 'America/Toronto' });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(repo, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const key = m ? decodeURIComponent(m[1]) : ''; const b = st.blobs.get(key); if (!b) return r.fulfill({ status: 404, body: 'no blob ' + key }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ station, sorter }) => {
    if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200 * 24 * 30)); localStorage.setItem('employee_name', 'Second'); }
    if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Second'); }
    window.confirm = () => true; window.prompt = () => 'Second'; window.alert = () => {};
    if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
  }, { station: H.stationOrigin, sorter: H.sorterOrigin });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${H.sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 90000 });
  await page.evaluate(({ station }) => { const s = CN.S.settings; s.dsOrigin = station; s.engine = 'solver'; s.budgetS = 2; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.autoCommit = 'on'; s.pullMode = 'all'; CN.saveSettings && CN.saveSettings(); }, { station: H.stationOrigin });
  return { ctx, page, errors };
}


(async () => {
  const receipts = W.wave(3930000101, 4);
  const H = await open({ receipts, masters: W.MASTERS, settings: { budgetS: +process.env.BUDGET || 2, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  const { page, st } = H;
  const t0 = Date.now(), at = () => Math.round((Date.now() - t0) / 1000) + 's', log = (...a) => console.log(at(), ...a);
  try {
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    const v = await W.rest(page, { log, timeoutMs: 420000 }); assert(!v.timeout, 'the first computer rests: ' + JSON.stringify(v));
    const runId = v.run.id; await page.evaluate(() => Session.flush());
    const runs0 = [...st.docs.keys()].filter(k => k.startsWith('Charm_Nest_Runs/')).length;
    const two = await otherComputer(H), p2 = two.page;
    await p2.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    await p2.waitForFunction(() => { const f = allSheets().filter(p => p.followed); return f.length > 0 && f.every(p => !p.recalled && (p.charms || []).length > 0); }, null, { timeout: 90000, polling: 500 });
    // the piece: of a sheet that keeps other pieces
    const sheets = [...st.docs.keys()].filter(k => k.startsWith('Charm_Nest_Sheets/')).map(k => st.doc('Charm_Nest_Sheets', k.split('/')[1])).filter(d => d.runId === runId && (d.poolIds || []).length >= 2 && !(+d.laserDoneAt > 0));
    assert(sheets.length, 'a sheet with two or more pieces');
    const sheet = sheets.find(d => d.draft) || sheets[0], pid = sheet.poolIds[0], rid = String(pid).split('_')[0], rev0 = +sheet.layoutRev || 0;
    log('taking', pid, 'off', sheet.id, 'from the second computer; layoutRev', rev0);
    await page.evaluate(() => { window.__stopped = false; window.__w = setInterval(() => { if (B.run && B.run.status === 'stopped') window.__stopped = true; }, 200); });
    const res = await p2.evaluate(({ rid, sheetId }) => SheetWin.takeOffOrder({ orderId: rid, sheetId, mode: 'hold', scope: 'sheet', by: 'Second', note: 'test' }), { rid, sheetId: sheet.id });
    log('takeOffOrder:', JSON.stringify(res).slice(0, 300));
    assert(res.ok && res.removed.some(x => x.id === pid), '1 · the piece came off: ' + JSON.stringify(res));
    const gone = res.removed.map(x => x.id);
    // cloud
    const row = st.doc('Charm_Pool', pid);
    assert(row.state === 'abandoned' && !row.sheetId, '1 · the cloud row is abandoned with no sheet: ' + JSON.stringify({ s: row.state, sh: row.sheetId }));
    const doc = () => st.doc('Charm_Nest_Sheets', sheet.id);
    for (let i = 0; i < 90 && !((+doc().layoutRev || 0) > rev0 && doc().dirty !== true); i++) await new Promise(r => setTimeout(r, 1000));
    const d = doc();
    assert((+d.layoutRev || 0) > rev0, '1 · layoutRev went up: ' + rev0 + ' -> ' + d.layoutRev);
    for (const id of gone) {
      assert(!(d.poolIds || []).includes(id), '1 · poolIds no longer lists ' + id);
      assert(!(d.charms || []).some(c => c.poolId === id), '1 · charms no longer hold ' + id);
    }
    assert.equal((d.placements || []).length, (d.charms || []).length, '1 · placements match charms');
    // the carrier loses the piece in memory within ~30 s, its run not stopped
    await page.waitForFunction(ids => !allSheets().some(p => (p.charms || []).some(c => ids.includes(c.poolId))), gone, { timeout: 40000, polling: 500 });
    const mine = await page.evaluate(() => ({ stopped: window.__stopped, status: B.run && B.run.status, run: B.run && B.run.runId }));
    assert(mine.run === runId && !mine.stopped && mine.status !== 'stopped', '1 · the carrier run never stopped: ' + JSON.stringify(mine));
    assert.equal([...st.docs.keys()].filter(k => k.startsWith('Charm_Nest_Runs/')).length, runs0, '1 · no second run');
    for (const [name, pg] of [['carrier', page], ['follower', p2]]) {
      // (the carrier takes the line another computer changed at its next look at the run's lines, a few seconds after the sheet itself changed: it is waited for, up to 40 s; this checked at once and failed on timing alone, 18 Nov 2026: held 0 after 5 s)
      let held = -1; for (let i = 0; i < 40; i++) { held = await pg.evaluate(gone => Orders.rows().filter(r => (r.poolIds || []).some(id => gone.includes(id))).length, gone); if (!held) break; await new Promise(r => setTimeout(r, 1000)); }
      assert.equal(held, 0, `1 · on the ${name} no order line still holds the piece (within 40 s)`);
    }
    assert.deepStrictEqual(two.errors, [], '1 · no page error on the second computer');
    console.log(at(), 'OK · takeoff-two-computers', runId);
  } finally { await H.close(); }
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
