// PURGE (AND RESET) LEAVE THE LIBRARY EMPTY (Paul, 26 Oct 2026: "When I purge all history while running in Sandbox mode, that must also delete ALL records
// from the Library tab (Sets of Sheets) so that on the next run I do not get information contamination where I'm not sure which set is old from a
// previous test-run and which is new current run.")
// The real page in headless Chromium, offline, against the repo's fake site (bridge-server.cjs: the REAL charmNestLibrary and charmNestOutput
// handlers over an in-memory Firestore and Storage). Nothing here touches a live site.
//   1 · a sandbox Library as a run leaves it, built through the real ops (sandbox:true): a run still open, Set-1 of the simulated day 2026-10-27
//       (setAllocate, setUpdate with its labels) with a GF and an SS sheet (putSheet: .ai, .dxf, preview, QR label), a set the laser cut whole
//       (laserDone: the Completed tab), pool rows, the files in Storage (charmNestOutput); production's own set and sheet beside them
//   2 · two tabs of ONE browser in the sandbox: A shows the Library (control: Set-1 is drawn, the Completed tab counts the cut set, the order search
//       finds its sheet); B is the tab holding the run: its set and sheet are in its memory and it saves them back every 150 ms (Sets.save, putSheet
//       through api(), as a live run does), and the browser holds a QR label handed to the printer and B's saved workspace
//   3 · "Purge all run history…" is pressed in A (the real button: the passcode, and Give it up for the open run); then, on a fresh site, the same
//       with "Reset sandbox records", which ends in the same wipe
// What must hold:
//   · B stopped BEFORE anything was deleted: no write of B left after the wipe's first delete was sent, and B came back with no set in memory
//     (it used to keep saving Set-1 and its sheet straight back into the cloud while the purge deleted them)
//   · the cloud holds no sandbox set, sheet, counter, run, pool row or file; production's set and sheet are gone after the purge, untouched by the reset
//   · in BOTH tabs, after their reload, every read the Library makes comes back empty (Sets and Sheets of the Current tab, the Completed tab and its
//     counts, the order search) and the Sets view says "No sets yet."; the QR label and the saved workspace are gone
//   · the next run: a set made afterwards on the same simulated day is Set-1 again and holds only its own new sheet, never an old one
//   · a tab that missed every signal of a reset has its next write refused (api(), Session.stale) and reloads
// Each call of the wipe takes the cloud's time here (SLOW): on clean main both buttons then end "The sandbox was not fully reset ... (sheets 1,
// sets 1 ...)" with Set-1 and its GF sheet saved back by tab B, which is what Paul saw stand in the Library.
//   node tests/charm-nest/purge-library-clean.cjs [playwright-core dir]      (CHROMIUM=/path/to/chrome when it is not the default)
'use strict';
process.env.CHARM_NEST_DELETE_CODE = 'purge-library-test-code';   // (the purge's passcode for this run only, set before the handlers load)
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, unlockSettings } = require('./bridge-server.cjs');
const CODE = process.env.CHARM_NEST_DELETE_CODE;
const READ_OP = /^(get|list|ping|lookup|history|check|masterList)|(Get|List|Check|Status|Preview|Read|Url|Info|Files)$/;   // (the page's own rule, charm-nest-1.html, and the master file list a page reads as it opens: sandbox-wipe-computers.cjs)
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const SLOW = 1200;   // ms: the cloud's time for each call of the wipe (see open())

const DAY = '2026-10-27', RUN = 'run-20261027-1', SET1 = `set-${DAY}-1`, O1 = '4173300001', O2 = '4173300002', O3 = '4173300003';
const GF = 'sb-gf-1', SS = 'sb-ss-1', LSET = `set-${DAY}-2`, LRUN = 'run-20261027-laser';
const url = p => `https://storage.example/${encodeURIComponent(p)}`;
const outputs = base => ({ ai: { path: `charmnest/sandbox/sets/${DAY}/Set-1/${base}/${base}.ai`, url: url(base + '.ai') }, dxf: { path: `charmnest/sandbox/sets/${DAY}/Set-1/${base}/${base}.dxf`, url: url(base + '.dxf') }, preview: { path: `charmnest/sandbox/sets/${DAY}/Set-1/${base}/${base}.png`, url: url(base + '.png') } });
const label = (base, orders) => ({ files: [{ path: `charmnest/sandbox/sets/${DAY}/Set-1/${base}/${base}-qr.png`, url: url(base + '-qr.png'), payload: orders.join(','), orders }] });
const sheetOf = (id, metal, base, orders, i) => ({ id, metal, metalLabel: metal === 'gold' ? 'GF 14/20' : 'SS', day: DAY, runId: RUN, setId: SET1, setSeq: 1, sheetIndex: i, fileBase: base, folder: base, status: 'complete', charmCount: orders.length, placedCount: orders.length, density: 0.72,
  orders, poolIds: orders.map((o, k) => `${o}_${70000 + k}_1`), listings: ['1718000'], verification: { ok: true }, outputs: outputs(base), label: label(base, orders), cardStartedAt: Date.now() - 3600000 });

async function scenario(browser, button) {
  const purge = button === '#stPurge', what = purge ? 'the purge' : 'the reset';
  console.log(`${purge ? 'Purge all run history…' : 'Reset sandbox records'}, with a live tab:`);
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const lib = async (body, sb = true) => { const r = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({}, body, sb ? { sandbox: true } : {})), queryStringParameters: {} }); const b = JSON.parse(r.body || '{}'); assert.strictEqual(r.statusCode, 200, `${body.op}: ${r.body.slice(0, 300)}`); return b; };
  const put = async (p, sb = true) => { const r = await st.handlers.charmNestOutput.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({ op: 'put', path: p, contentType: 'application/octet-stream', base64: Buffer.from('file ' + p).toString('base64') }, sb ? { sandbox: true } : {})) }); assert.strictEqual(r.statusCode, 200, 'file ' + p + ': ' + r.body); };

  /* ═══ 1 · the Library a sandbox run leaves, through the real ops ═══ */
  await lib({ op: 'runPut', run: { runId: RUN, status: 'running', step: 'engrave', day: DAY, orders: [O1, O2], lines: { [`${O1}_70000`]: { orderId: O1, key: `${O1}_70000` } } } });
  await lib({ op: 'poolPut', pools: [O1, O2].map((o, k) => ({ poolId: `${o}_${70000 + k}_1`, orderId: o, transactionId: String(70000 + k), lineKey: `${o}_${70000 + k}`, runId: RUN, state: 'written', material: k ? 'silver' : 'gold', copy: 1, quantity: 1, setId: SET1, sheetId: k ? SS : GF })) });
  const a1 = await lib({ op: 'setAllocate', day: DAY, runId: RUN, group: 'dispatch' });
  assert.strictEqual(a1.setId, SET1, 'the run\'s first set of the simulated day is Set-1: ' + JSON.stringify(a1));
  const gfBase = 'GF_Oct.27.26_Set-1_Sheet-1', ssBase = 'SS_Oct.27.26_Set-1_Sheet-1';
  const GFSHEET = sheetOf(GF, 'gold', gfBase, [O1], 1), SSSHEET = sheetOf(SS, 'silver', ssBase, [O2], 1);
  await lib({ op: 'putSheet', sheet: GFSHEET }); await lib({ op: 'putSheet', sheet: SSSHEET });
  const SETPATCH = { runId: RUN, day: DAY, seq: 1, name: 'Set-1', folder: `charmnest/sets/${DAY}/Set-1`, materials: ['gold', 'silver'], sheetIds: [GF, SS], status: 'labelled', orders: { [O1]: { held: null, lines: [{ transactionId: '70000' }] }, [O2]: { held: null, lines: [{ transactionId: '70001' }] } },
    labels: { pdf: { url: url('labels.pdf') }, manifest: { url: url('manifest.csv') } }, labelFiles: [GFSHEET.label.files[0], SSSHEET.label.files[0]].map((f, i) => Object.assign({ sheet: i ? 'SS Sheet 1' : 'GF Sheet 1', sheetId: i ? SS : GF, part: 1, parts: 1 }, f)) };
  await lib({ op: 'setUpdate', setId: SET1, patch: SETPATCH });
  for (const base of [gfBase, ssBase]) for (const ext of ['ai', 'dxf', 'png']) await put(`charmnest/sets/${DAY}/Set-1/${base}/${base}.${ext}`);
  await put(`charmnest/sets/${DAY}/Set-1/labels.pdf`);
  // a set the laser cut whole (the Completed tab), from the records a laser-ready set holds (the fixture the guard test uses), marked by the real op
  const lineRows = {};
  st.put('Sandbox_Charm_Nest_Sets', LSET, { setId: LSET, seq: 2, day: DAY, name: 'Set-2', runId: LRUN, sheetIds: ['gl-1', 'gl-2'], materials: ['gold'], status: 'complete', current: true });
  for (const [sheetId, index, orders] of [['gl-1', 1, [O3]], ['gl-2', 2, ['4173300004']]]) {
    const poolIds = orders.map((o, i) => `${o}_${80000 + index * 10 + i}_1`);
    for (const p of poolIds) lineRows[p.slice(0, p.lastIndexOf('_'))] = { orderId: p.split('_')[0], state: 'written', quantity: 1, poolIds: [p], engraveCandidate: false };
    st.put('Sandbox_Charm_Nest_Sheets', sheetId, { id: sheetId, metal: 'gold', day: DAY, setId: LSET, setSeq: 2, runId: LRUN, sheetIndex: index, fileBase: `GF_Oct.27.26_Set-2_Sheet-${index}`, orders, poolIds, placements: [], placedCount: poolIds.length, status: 'complete', verification: { ok: true }, current: true,
      outputs: { ai: { path: sheetId + '.ai', url: url(sheetId + '.ai') }, preview: { path: sheetId + '.png', url: url(sheetId + '.png') } }, label: { files: [{ path: sheetId + '-qr.png', url: url(sheetId + '-qr.png'), payload: orders.join(','), orders }] } });
  }
  st.put('Sandbox_Charm_Nest_Runs', LRUN, { runId: LRUN, status: 'complete', lines: lineRows });
  const cut = await lib({ op: 'laserDone', stage: 'laser', kind: 'set', id: LSET, by: 'Cara' });
  assert(cut.ok && cut.setDone, 'the laser cut Set-2 whole: ' + JSON.stringify(cut).slice(0, 200));
  // production's own Library beside it (the purge deletes its run history too)
  const p1 = await lib({ op: 'setAllocate', day: '2026-10-20', runId: 'run-20261020-1' }, false);
  await lib({ op: 'putSheet', sheet: Object.assign(sheetOf('prod-gf-1', 'gold', 'GF_Oct.20.26_Set-1_Sheet-1', ['4170000901'], 1), { day: '2026-10-20', runId: 'run-20261020-1', setId: p1.setId }) }, false);
  await lib({ op: 'setUpdate', setId: p1.setId, patch: { runId: 'run-20261020-1', day: '2026-10-20', seq: 1, name: 'Set-1', sheetIds: ['prod-gf-1'], materials: ['gold'], status: 'labelled' } }, false);
  const libDocs = () => [...st.docs.keys()].filter(k => /^(Sandbox_)?Charm_Nest_(Sets|Sheets|Counters|Runs|Run_Lines|Run_Live)\/|^(Sandbox_)?Charm_Pool(_Back)?\//.test(k));
  const sbFiles = () => [...st.blobs.keys()].filter(k => k.startsWith('charmnest/sandbox/') && !k.startsWith('charmnest/sandbox/master/'));
  ok(libDocs().filter(k => k.startsWith('Sandbox_')).length >= 8 && libDocs().some(k => k.startsWith('Charm_Nest_Sets/')) && sbFiles().length >= 7, `the sandbox Library holds a run's sets, sheets, counters, run and pool rows (${libDocs().length} records) and their files (${sbFiles().length}), production its own`);

  /* ═══ 2 · two tabs of one browser in the sandbox ═══ */
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/firebaseOrders/, r => (r.request().method() === 'POST' ? r.abort() : r.continue()));
  await ctx.addInitScript(({ sorter, code }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => code; window.alert = () => {}; }, { sorter: sorterOrigin, code: CODE });
  const errors = [], sent = { A: [], B: [] };
  const open = async name => {
    const p = await ctx.newPage(); p.on('pageerror', e => errors.push(`${name}: ${e.message}`));
    // what each tab sends to the cloud, and when (a write is any op the page's own rule does not call a read). The wipe's own calls take the cloud's
    // time (SLOW): the real purge, reset and count each take seconds of Firestore work, during which a live tab used to go on saving; the in-memory
    // stand-in answers in a millisecond, and without it the race the purge lost in the shop never shows
    await p.route(/charmNestLibrary/, async r => {
      let b = {}; try { b = r.request().postDataJSON() || {}; } catch (_) {}
      sent[name].push({ op: b.op, check: !!b.check, at: Date.now(), write: !READ_OP.test(String(b.op || '')) });
      if ((b.op === 'purgeHistory' && !b.check) || b.op === 'sandboxReset' || (b.op === 'sandboxStatus' && !b.light)) await new Promise(res => setTimeout(res, SLOW));
      return r.continue();
    });
    await p.goto(`${sorterOrigin}/charm-nest-1.html`); return p;
  };
  const booted = p => p.waitForFunction(() => window.CN && window.Sandbox && window.Session && Session.ready() && window.Sets && CN.S.cloud.ok !== null, null, { timeout: 60000 });
  const A = await open('A');
  await A.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 });
  await A.evaluate(station => {
    const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'on', sandboxStream: 'on', dsOrigin: station, pollOrders: 'off', runMode: 'manual' }); localStorage.setItem('cn.settings', JSON.stringify(s));
    localStorage.setItem('cn.sandboxHold', JSON.stringify({ at: Date.now() }));   // (no stream plays meanwhile: the tab that "runs" is B's writer below)
  }, stationOrigin);
  await A.reload(); await booted(A);
  ok(await A.evaluate(() => WORKSPACE_SANDBOX), 'tab A is in the sandbox');
  const B = await open('B'); await booted(B);

  // what the Library reads, as the page asks it (its own api(): the sandbox flag on every call)
  const reads = p => p.evaluate(async ({ O1, O3 }) => {
    const L = b => CN.api('charmNestLibrary', b, { quiet: true });
    const [sets, sheets, doneSets, doneSheets, counts, foundCur, foundDone] = await Promise.all([L({ op: 'setList', includeSheets: true, limit: 200, excludeDone: true }), L({ op: 'listSheets', limit: 500, excludeDone: true }), L({ op: 'laserDoneList', kind: 'sets' }), L({ op: 'laserDoneList', kind: 'sheets' }), L({ op: 'laserDoneList', countOnly: true }), L({ op: 'findSheets', q: O1 }), L({ op: 'findSheets', q: O3 })]);
    return { sets: (sets.sets || []).map(s => s.setId), setSheets: (sets.sheets || []).map(s => s.id), sheets: (sheets.sheets || []).map(s => s.id), doneSets: (doneSets.rows || []).length, doneSheets: (doneSheets.rows || []).length, counts: counts.counts,
      found: [].concat(foundCur.sheets || [], foundCur.rows || [], foundCur.sets || [], foundDone.sheets || [], foundDone.rows || [], foundDone.sets || [], foundDone.setRows || []).length };
  }, { O1, O3 });
  const setsView = async p => {
    await p.evaluate(() => { CN.S.library.kind = 'sets'; CN.setMode('library'); if (window.LibraryDone && LibraryDone.tab() === 'done') LibraryDone.setTab('current'); document.getElementById('libBody').innerHTML = ''; CN.loadLibrary().catch(() => {}); });   // (emptied first: the wait below sees this read's drawing, never the last one)
    await p.waitForFunction(() => { const b = document.getElementById('libBody'); return b && !/Loading sets/.test(b.innerText) && (b.querySelector('.setCard') || /No sets yet|Could not load/.test(b.innerText)); }, null, { timeout: 30000 });
    return p.evaluate(() => ({ cards: document.querySelectorAll('#libBody .setCard').length, titles: [...document.querySelectorAll('#libBody .setCard [data-set-title]')].map(e => e.textContent), text: document.getElementById('libBody').innerText.slice(0, 300) }));
  };
  const before = await reads(A), viewBefore = await setsView(A);
  ok(before.sets.includes(SET1) && before.sheets.includes(GF) && before.sheets.includes(SS) && before.doneSets >= 1 && before.counts.sets >= 1 && before.found >= 2, `control: before ${what} the Library reads Set-1, its two sheets, the cut Set-2 under Completed and both orders: ` + JSON.stringify(before));
  ok(viewBefore.cards >= 1 && viewBefore.titles.some(t => /Set-1/.test(t) && t.includes(DAY)), 'control: the Sets view draws "Set-1 · ' + DAY + '": ' + JSON.stringify(viewBefore));

  // B: the tab holding the run. Its set and sheet are in its memory and it saves them back, as a live run does (the QR label it handed the printer, its saved workspace)
  await B.evaluate(({ SET1, RUN, DAY, SETPATCH, GFSHEET }) => {
    const set = Object.assign({ setId: SET1, group: 'dispatch', committedAt: null, completedAt: null, completionDay: null, committed: null, refused: null, backCount: 0, sheetNos: {} }, SETPATCH, { orders: Object.fromEntries(Object.entries(SETPATCH.orders).map(([rid, o]) => [rid, { held: null, lines: Object.fromEntries(o.lines.map(l => [l.transactionId, l])) }])) });
    Sets.byRun().set(RUN + '|dispatch', set);
    localStorage.setItem('qrPrintAll', JSON.stringify({ sandbox: true, dispatchDate: DAY, items: [{ receipt_id: GFSHEET.orders[0] }] }));
    window.__saved = 0;
    window.__writer = setInterval(() => { Sets.save(set).then(() => window.__saved++, () => {}); CN.api('charmNestLibrary', { op: 'putSheet', sheet: GFSHEET }, { quiet: true }).then(() => window.__saved++, () => {}); }, 150);
    Session.schedule(); return Session.flushNow();
  }, { SET1, RUN, DAY, SETPATCH, GFSHEET });
  await B.waitForFunction(() => window.__saved >= 6, null, { timeout: 20000 });
  ok(true, 'tab B is saving its set and sheet back every 150 ms (the control: a live writer)');

  /* ═══ 3 · the button in tab A ═══ */
  await A.evaluate(() => { openSettings(); }); await unlockSettings(A);   // (PERSON, 26 Oct: Settings asks the admin password at every opening)
  await A.waitForFunction(b => document.getElementById('dlgSettings').open && document.querySelector(b), button, { timeout: 20000 });
  if (purge) {
    const help = await A.evaluate(() => document.getElementById('stPurge').closest('.field').innerText + ' | ' + document.getElementById('stPurge').title);
    ok(/Library is left with no set of sheets, real or sandbox/.test(help) && /every open tab of this browser stops before anything is deleted/.test(help) && /sets of sheets go, real and sandbox/.test(help), 'the Purge help and tooltip say the Library\'s sets of sheets go and every tab stops first');
  }
  const reloaded = p => p.waitForNavigation({ waitUntil: 'load', timeout: 60000 }).then(() => true, () => false), navA = reloaded(A), navB = reloaded(B);
  await B.evaluate(() => { window.__stale = 1; });
  await A.click(button);
  const why = async () => JSON.stringify(await A.evaluate(id => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent).concat(['note: ' + (document.getElementById(id) || {}).textContent]), purge ? 'stPurgeNote' : 'stSandboxResetNote').catch(() => [])) + ' · cloud: ' + JSON.stringify(libDocs().filter(k => k.startsWith('Sandbox_')).slice(0, 8));
  // (it used to stop here: the tab holding the run saved Set-1 and its sheet back while the wipe deleted them, and the count found them: "not fully reset")
  ok(await navA || assert.fail(`tab A did not reload after ${what}: ` + await why()), `${what} finished and tab A reloaded`);
  ok(await navB, 'tab B stood down and reloaded');
  await booted(A); await booted(B);

  // B stopped before anything was deleted, and came back with nothing in memory
  const firstDelete = (sent.A.find(x => purge ? x.op === 'purgeHistory' && !x.check : x.op === 'sandboxReset') || {}).at;
  if (purge) ok(firstDelete > 0 && sent.A.some(x => x.op === 'purgeHistory' && x.check && x.at < firstDelete), 'the purge asked its question first (passcode, open run), then made its delete');
  const late = sent.B.filter(x => x.write && x.at >= firstDelete);
  ok(firstDelete > 0 && !late.length, `tab B wrote nothing once ${what}'s first delete was sent (it stood down at the new epoch first): ${JSON.stringify(late.slice(0, 5))}`);
  ok(sent.B.filter(x => x.write && x.at < firstDelete).length >= 6, 'and it was writing until then');
  const memB = await B.evaluate(() => ({ sets: Sets.byRun().size, designs: Object.keys(B.customDesigns || {}).length, stale: window.__stale || 0 }));
  ok(memB.sets === 0 && memB.designs === 0 && !memB.stale, 'tab B reloaded with no set in memory: ' + JSON.stringify(memB));

  // the cloud
  const left = libDocs().filter(k => k.startsWith('Sandbox_'));
  ok(!left.length, 'the cloud holds no sandbox set, sheet, counter, run or pool row: ' + JSON.stringify(left.slice(0, 8)));
  ok(!sbFiles().length, 'and no sandbox file (sheets, labels): ' + JSON.stringify(sbFiles().slice(0, 5)));
  if (purge) ok(!st.docs.has('Charm_Nest_Sets/' + p1.setId) && !st.docs.has('Charm_Nest_Sheets/prod-gf-1'), 'production\'s set and sheet went with its run history');
  else ok(st.docs.has('Charm_Nest_Sets/' + p1.setId) && st.docs.has('Charm_Nest_Sheets/prod-gf-1'), 'production\'s set and sheet are untouched by the reset');

  // every read the Library makes, in both tabs, and the Sets view
  for (const [name, p] of [['A', A], ['B', B]]) {
    const r = await reads(p);
    ok(!r.sets.length && !r.setSheets.length && !r.sheets.length && !r.doneSets && !r.doneSheets && r.counts.sets === 0 && r.counts.sheets === 0 && !r.found, `tab ${name}: every Library read is empty (Current sets and sheets, Completed and its counts, the order search): ` + JSON.stringify(r));
    const v = await setsView(p);
    ok(v.cards === 0 && /No sets yet/.test(v.text), `tab ${name}: the Sets view says "No sets yet.": ` + JSON.stringify(v));
  }
  const kept = await A.evaluate(async () => ({ label: localStorage.getItem('qrPrintAll'), held: localStorage.getItem('cn.sandboxHold') != null, ws: await new Promise(res => { const r = indexedDB.open('charm-nest-workspace', 1); r.onupgradeneeded = () => r.result.createObjectStore('workspaces'); r.onsuccess = () => { const g = r.result.transaction('workspaces', 'readonly').objectStore('workspaces').get('sandbox'); g.onsuccess = () => res(g.result || null); g.onerror = () => res(null); }; r.onerror = () => res(null); }) }));
  ok(kept.label === null, 'the QR label handed to the printer is gone');
  ok(!kept.ws || (!(kept.ws.sets && kept.ws.sets.size) && !(kept.ws.run)), 'the saved workspace holds no set and no run');
  ok(kept.held, 'the sandbox waits for Start');

  /* ═══ 4 · the next run: Set-1 again, with its own sheet only ═══ */
  const a2 = await lib({ op: 'setAllocate', day: DAY, runId: 'run-20261027-2', group: 'dispatch' });
  await lib({ op: 'putSheet', sheet: Object.assign(sheetOf('sb-gf-new', 'gold', 'GF_Oct.27.26_Set-1_Sheet-1', ['4173300099'], 1), { runId: 'run-20261027-2', setId: a2.setId }) });
  await lib({ op: 'setUpdate', setId: a2.setId, patch: { runId: 'run-20261027-2', day: DAY, seq: a2.seq, name: 'Set-' + a2.seq, sheetIds: ['sb-gf-new'], materials: ['gold'], status: 'open' } });
  const next = await reads(A), viewNext = await setsView(A);
  ok(a2.setId === SET1 && a2.seq === 1, 'the next run\'s first set of the same simulated day is Set-1 again: ' + JSON.stringify(a2));
  ok(JSON.stringify(next.sheets) === JSON.stringify(['sb-gf-new']) && JSON.stringify(next.setSheets) === JSON.stringify(['sb-gf-new']), 'and it holds only its own new sheet, no old one: ' + JSON.stringify(next));
  ok(viewNext.cards === 1, 'the Library draws that one set: ' + JSON.stringify(viewNext));

  // a tab that missed every signal of a reset (a frozen tab whose timer runs before its wake-up events): its next write is refused and it stands down
  // (api() asks Session.stale before a write; the epoch is changed here from inside the tab itself, so no storage event or channel message reaches it)
  const navB2 = reloaded(B);
  const refused = await B.evaluate(async id => { localStorage.setItem('cn.resetEpoch.sandbox', 'missed-' + Date.now()); try { await CN.api('charmNestLibrary', { op: 'setUpdate', setId: id, patch: { name: 'Set-OLD' } }, { quiet: true }); return 'written'; } catch (e) { return e.message; } }, SET1);
  ok(/reset in another tab/.test(refused) && await navB2, 'a tab that missed the reset has its next write refused, and it reloads: ' + refused);
  ok(!/Set-OLD/.test(JSON.stringify(st.docs.get('Sandbox_Charm_Nest_Sets/' + SET1) || {})), 'and nothing of it reached the cloud');

  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  await ctx.close(); srv.close();
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  await scenario(browser, '#stPurge');
  await scenario(browser, '#stSbReset');
  console.log(`purge-library-clean OK: ${n} checks — Purge all run history… and Reset sandbox records in the sandbox leave the Library with no set, sheet, cut record, file or QR label of the earlier run, in every tab, and the next run starts clean`);
  await browser.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
