// A SECOND COMPUTER opened for the first time while a run is open on the first one (Paul, 15 Nov 2026: "when she loaded the application for the first time all the nesting on the
// nest tab start nesting everything ... the nest thing on the nest tab does not match the nest thing in the library tab. The library tab is the correct one"; "Make sure that
// Michelle's computer doesn't override anything that my computer already has or that the cloud already had"; "This thing must happen seamlessly in the background").
// The real sorter and the framed Design Station in Chromium against the repo's fake site (day-sim/harness.cjs). The second computer is a browser context of its own: no saved
// workspace, no settings, nothing in its storage.
//  1 · AUTOASK: Auto is turned on there. It starts no run of its own and writes no sheet, run, set or piece record: the run open in the cloud is followed.
//  2 · CLOUDFIRST: its Nest cards show the run's sheets as the cloud has them (the same sheets, the same number of pieces), read from their records; nothing is nested there.
//  2b· ONERUN: a run started there by hand, or a new run's record sent straight to the cloud, is refused while one is open: the cloud keeps ONE run.
//  3 · QUIETLOAD + CLOUDFIRST: the run is taken over on the second computer. Every sheet comes back with every piece where the cloud has it (no sheet is nested again, no record's
//      layout changes), the pieces are read in one call and not one call each, and each sheet restored keeps the cloud's revision of its layout.
//   node tests/charm-nest/second-computer.cjs [playwright-core dir]      (CHROMIUM=/path/to/chrome when playwright's own is not installed)
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
  const receipts = W.wave(3930000001, 4);
  const H = await open({ receipts, masters: W.MASTERS, settings: { budgetS: +process.env.BUDGET || 2, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  const { page, st } = H;
  const t0 = Date.now(), at = () => Math.round((Date.now() - t0) / 1000) + 's', log = (...a) => console.log(at(), ...a);
  try {
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    let v = await W.rest(page, { log, timeoutMs: 420000 }); assert(!v.timeout, 'the first computer rests: ' + JSON.stringify(v));
    const runId = v.run.id; log('first computer at rest:', v.run.status, v.sheets.join(' '), v.sets.join(' '));
    await page.evaluate(() => Session.flush());
    const first = await page.evaluate(() => allSheets().filter(p => p.sheetId && p.runId === B.run.runId).map(p => [p.sheetId, p.placements.length]));
    assert(first.length >= 1, 'the first computer holds sheets of its run');
    const before = new Map(first.map(([id]) => [id, layoutOf(st.doc('Charm_Nest_Sheets', id))]));
    const runsBefore = [...st.docs.keys()].filter(k => k.startsWith('Charm_Nest_Runs/')).length;

    /* 1 · a second computer, Auto turned on */
    const n0 = st.calls.length;
    const two = await otherComputer(H), p2 = two.page;
    await p2.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    const seen = [];
    for (let i = 0; i < 16; i++) { await new Promise(r => setTimeout(r, 1500)); seen.push(await p2.evaluate(() => B.run ? `${B.run.status}/${B.run.step}` : 'no run').catch(() => '?')); }
    const states = [...new Set(seen)]; log('second computer says:', states.join(' → '));
    // (the first computer, at rest, still saves its sheets' set marks, CLOUDMARKS: a putSheet of id / joinedAt / joinedBy / leftSet only, which carries no layout. Every other write counts)
    const marksOnly = c => c.op === 'putSheet' && c.body && c.body.sheet && Object.keys(c.body.sheet).every(k => ['id', 'joinedAt', 'joinedBy', 'leftSet'].includes(k));
    const wrote = st.calls.slice(n0).filter(c => WRITES.test(String(c.op)) && !marksOnly(c)).map(c => c.op);
    if (wrote.length) log('writes seen:', JSON.stringify(st.calls.slice(n0).filter(c => WRITES.test(String(c.op))).map(c => ({ op: c.op, keys: Object.keys((c.body && (c.body.sheet || c.body.run)) || {}), working: c.body && c.body.sheet && c.body.sheet.working, layoutBase: c.body && c.body.layoutBase }))).slice(0, 1500));
    assert.deepStrictEqual(states, ['no run'], '1 · Auto on a second computer starts no run while one is open in the cloud: ' + states.join(' → '));
    assert.equal([...st.docs.keys()].filter(k => k.startsWith('Charm_Nest_Runs/')).length, runsBefore, '1 · no second run record');
    assert.deepStrictEqual(wrote, [], '1 · the second computer writes no sheet, run, set or piece record');
    assert.equal(st.calls.slice(n0).filter(c => c.op === 'poolGet').length, 0, '1 · and reads no piece: its cards are drawn from the sheets\' own records');
    log('1 · Auto on the second computer: no run, no write');

    /* 2 · its Nest cards are the cloud's sheets */
    await p2.waitForFunction(() => allSheets().some(p => p.followed), null, { timeout: 30000 });
    const shown = await p2.evaluate(() => allSheets().filter(p => p.followed).map(p => [p.sheetId, p.recalled ? p.recalled.placedCount : null, (p.charms || []).length]));
    assert.deepStrictEqual(shown.map(x => x[0]).sort(), first.map(x => x[0]).sort(), '2 · the second computer shows the run\'s sheets, the same ones: ' + JSON.stringify(shown));
    for (const [id, placed, charms] of shown) { const doc = st.doc('Charm_Nest_Sheets', id); assert.equal(placed, (doc.placements || []).length, `2 · sheet ${id} is shown with the cloud's ${(doc.placements || []).length} pieces`); assert.equal(charms, 0, '2 · drawn from its record: nothing was built or nested here'); }
    for (const [id, was] of before) assert.equal(layoutOf(st.doc('Charm_Nest_Sheets', id)), was, `2 · sheet ${id} in the cloud is as the first computer left it`);
    assert.deepStrictEqual(two.errors, [], '2 · no page error on the second computer');
    log('2 · the second computer shows', shown.length, 'sheet(s) as the cloud has them');

    /* 2b · ONERUN: a run started by hand there, and a new run's record sent straight to the cloud, are both refused: one run is open */
    const n2 = st.calls.length;
    const hand = await p2.evaluate(async () => { const r = await RunCtl.start({ mode: 'manual' }).catch(e => 'threw: ' + e.message); return { r: r == null ? null : String(r.runId || r), run: B.run ? B.run.runId : null }; });
    assert.deepStrictEqual(hand, { r: null, run: null }, '2b · a run started by hand on the second computer is refused and leaves no run there: ' + JSON.stringify(hand));
    const raw = await p2.evaluate(() => CN.api('charmNestLibrary', { op: 'runPut', run: { runId: 'run-2026-11-15-zzsecond01', day: '2026-11-15', step: 'pull', status: 'running', mode: 'auto', lines: {}, orders: [] } }, { quiet: true }).then(() => 'saved', e => `${e.status}:${(e.data && e.data.openRun) || ''}`));
    assert.equal(raw, '409:' + runId, '2b · the cloud refuses a second run\'s record and names the open run: ' + raw);
    assert.equal([...st.docs.keys()].filter(k => k.startsWith('Charm_Nest_Runs/')).length, runsBefore, '2b · still one run record in the cloud');
    assert.deepStrictEqual(st.calls.slice(n2).filter(c => WRITES.test(String(c.op)) && !marksOnly(c) && c.op !== 'runPut').map(c => c.op), [], '2b · and nothing else was written');
    log('2b · a second run is refused by the cloud, by hand and by a direct save');

    /* 3 · the run is taken over there: every piece where the cloud has it */
    const n1 = st.calls.length;
    await p2.evaluate(id => RunCtl.resumeRun(id), runId);
    await p2.waitForFunction(id => B.run && B.run.runId === id && ['processed', 'complete'].includes(B.run.status) && !allSheets().some(p => ['nesting', 'queued', 'finishing'].includes(p.status)), runId, { timeout: 180000, polling: 500 });
    const mine = await p2.evaluate(() => allSheets().filter(p => p.sheetId && p.runId === B.run.runId).map(p => ({ id: p.sheetId, placed: p.placements.length, charms: p.charms.length, followed: !!p.followed, recalled: !!p.recalled, rev: p.layoutRev })));
    log('second computer after the takeover:', JSON.stringify(mine));
    assert.deepStrictEqual(mine.map(x => x.id).sort(), first.map(x => x[0]).sort(), '3 · the same sheets after the takeover');
    for (const s of mine) {
      const doc = st.doc('Charm_Nest_Sheets', s.id), n = (first.find(x => x[0] === s.id) || [])[1];
      assert.equal(s.placed, n, `3 · sheet ${s.id}: every piece is back (${s.placed} of ${n})`);
      assert(!s.followed && !s.recalled && s.charms === n, `3 · sheet ${s.id} is a live sheet here now`);
      assert.equal(layoutOf(doc), before.get(s.id), `3 · sheet ${s.id}: the cloud's layout is unchanged by the takeover (nothing nested again, nothing written over)`);
      assert(Number.isFinite(s.rev), `3 · sheet ${s.id} keeps the cloud's revision of its layout (${s.rev})`);
    }
    const pieces = first.reduce((n, x) => n + x[1], 0), gets = st.calls.slice(n1).filter(c => c.op === 'poolGet').length;
    log(`3 · ${pieces} pieces on ${first.length} sheets were read in ${gets} poolGet call(s)`);
    assert(gets <= 2, `3 · the pieces are read together, not one call each (${gets} calls for ${pieces} pieces)`);
    assert.deepStrictEqual(two.errors, [], '3 · no page error on the second computer');
    /* 4 · CACHEONLY: the first computer, whose run was taken over, loads itself again and follows. Its saved workspace still holds its own copies of the run's sheets: they are NOT used: its cards show the cloud's */
    await page.waitForFunction(() => window.__followedNow === undefined && typeof B !== 'undefined' && !B.run && typeof allSheets === 'function' && allSheets().some(p => p.followed), null, { timeout: 120000, polling: 1000 });
    const back = await page.evaluate(() => ({ run: B.run ? B.run.runId : null, pages: allSheets().filter(p => p.sheetId).map(p => ({ id: p.sheetId, followed: !!p.followed, charms: (p.charms || []).length, placed: p.recalled ? p.recalled.placedCount : null })) }));
    log('first computer after its run was taken over:', JSON.stringify(back));
    assert.equal(back.run, null, '4 · the first computer carries no run after the takeover');
    assert.deepStrictEqual(back.pages.map(x => x.id).sort(), first.map(x => x[0]).sort(), '4 · it shows the run\'s sheets');
    for (const s of back.pages) assert(s.followed && s.charms === 0 && s.placed === (st.doc('Charm_Nest_Sheets', s.id).placements || []).length, `4 · sheet ${s.id} is shown from the cloud, not from this computer's saved copy: ${JSON.stringify(s)}`);
    for (const [id, was] of before) assert.equal(layoutOf(st.doc('Charm_Nest_Sheets', id)), was, `4 · sheet ${id} in the cloud is still as it was`);
    console.log(at(), `OK · run ${runId}`);
  } finally { await H.close(); }
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
