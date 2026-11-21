// CUSTOMSEND (task A, Paul 21 Nov 2026): Send to Sheet (and Monogram Approve, which calls it) on a staff computer (a viewer) in mode "on". It said "send saved for retry — Workspace
// saving is unavailable", the cloud refused the request ("made into pieces only on a computer"), and the page still released the Review hold, so a wrong old piece stayed on its sheet unheld.
//   C  the cloud (offline, _cloudFakes.cjs): a viewer's customSheetPut (phase sent) and runCmdPut customsend are taken in mode on; the driver's pass makes the card's design (a real .ai,
//      read from the fake bucket and traced by the page's own charm-nest-pdf.js) into the line's pool piece, takes the swapped old piece off its sheet (superseded), places the new one on
//      a sheet record and answers done in words; asked again it changes nothing; a cut sheet keeps its piece (refused in words, nothing moved); lanes off: refused in words
//   P  the page (charm-nest-bridge.js): on a viewer the browser checkpoint is skipped; the send is sealed / released / said "sent" only after the cloud answered done, else the cloud's words;
//      the guard is the viewer's only (mode off unchanged); the bridge is loaded under a new token
// Offline only: no Etsy, no live write. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-customsend.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 21, 16, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console), quiet = !process.env.VCS_LOUD; if (quiet) { console.log = () => {}; console.warn = () => {}; }
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0002', R = 'run-2026-11-21-csend', RID = '4250000001', TX = '42500000011', KEY = `${RID}_${TX}`, OLD = `${KEY}_1`, OTHER = '4250000002_42500000021_1';
const SA = 'gold-a-csend', CK = 'card:' + KEY, PDF = `charmnest/custom/${RID}/abc123def456-COMET.pdf`;

async function seed(steps, o = {}) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'off', runMode: 'manual', cloudSteps: [], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [] });
  const put = await P({ op: 'runPut', merge: true, run: { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true,
    lines: { [KEY]: { state: 'pooled', poolIds: [OLD], orderId: RID, material: 'gold', hold: { by: 'Michelle', at: T0 - 600e3, why: 'Wrong charm' }, arrivedAt: T0 - 86400e3 },
             '4250000002_42500000021': { state: 'pooled', poolIds: [OTHER], orderId: '4250000002', material: 'gold', arrivedAt: T0 - 86400e3 } } } });
  if (put.http !== 200) throw new Error('run not seeded: ' + JSON.stringify(put));
  const charm = (id, poolId, x) => ({ id, poolId, order: poolId.split('_')[0], name: id, sku: 'OLDSKU', metal: 'gold', areaPt2: 300, widthPt: 20, heightPt: 20, lineKey: poolId.replace(/_\d+$/, '') });
  F.seed('Charm_Nest_Sheets/' + SA, Object.assign({ id: SA, runId: R, metal: 'gold', page: 1, draft: true, status: 'complete', layoutRev: 3, contentRev: 3, fileBase: 'GF Draft 1',
    charms: [charm('old1', OLD), charm('oth1', OTHER)], poolIds: [OLD, OTHER], placements: [{ id: 'old1', cxPt: 30, cyPt: 30, angle: 0 }, { id: 'oth1', cxPt: 80, cyPt: 30, angle: 0 }], orders: [RID, '4250000002'] }, o.sheet || {}));
  F.seed('Charm_Pool/' + OLD, { poolId: OLD, runId: R, orderId: RID, transactionId: TX, lineKey: KEY, sku: 'OLDSKU', material: 'gold', state: 'nested', sheetId: SA, copy: 1, quantity: 1 });
  F.seed('Charm_Pool/' + OTHER, { poolId: OTHER, runId: R, orderId: '4250000002', lineKey: '4250000002_42500000021', sku: 'OLDSKU', material: 'gold', state: 'nested', sheetId: SA, copy: 1, quantity: 1 });
  F.seed('Charm_World_Lines/' + KEY, { at: T0 - 60e3, data: JSON.stringify({ order: { receiptId: RID, createTs: T0 - 86400e3 }, line: { transactionId: TX, sku: 'OLDSKU', quantity: 1 }, spec: { designSku: 'OLDSKU', material: 'gold', quantity: 1, form: 'charm' }, problems: [] }) });
  await F.bucket.file(PDF).save(fs.readFileSync(path.join(__dirname, 'fixtures/COMET_9427.ai')), { contentType: 'application/pdf' });
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
}
const runLines = async () => ((await P({ op: 'runGet', runId: R })).run || {}).lines || {};
async function pass() {
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};
const record = () => ({ ck: CK, rid: RID, at: T0 - 5000, phase: 'sent', files: [{ id: 'f1', name: 'COMET.ai', kind: 'ai', size: 1000, hash: 'abc123def456', cloud: { path: PDF, url: '' }, metal: 'gold', qty: 1, pieces: 1, wMm: 20, hMm: 20, maxPt: 60, minPt: 40, maxAreaPt2: 2000, state: 'ready' }],
  sent: { id: `custom-sheet:${CK}:${T0 - 5000}`, at: T0 - 5000, by: 'Michelle', lines: { [KEY]: [{ f: 'f1', i: 0 }] } } });
async function send() {
  const s = await P({ op: 'customSheetPut', record: record(), dev: DEV, cv: CV });
  const c = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: { type: 'customsend', ck: CK, ids: [KEY], label: `Place the designs sent for order ${RID}` }, cv: CV });
  return { s, c };
}
async function untilAnswered(id, n = 6) { let c = {}; for (let i = 0; i < n; i++) { await pass(); c = cmdOf(id); if (c.state === 'done' || c.state === 'failed') break; F.clock.advance(60e3); } return c; }

async function partC() {
  const STEPS = ['nest', 'pool', 'cmd:customsend'];
  await seed(STEPS);
  const { s, c } = await send();
  ok('C1 mode on: the viewer\'s send is recorded (customSheetPut, phase sent) and its ONE request (runCmdPut customsend) is taken', s.http === 200 && s.ok && s.record && s.record.phase === 'sent' && c.http === 200 && c.ok && c.id, { s, c });
  const ans = await untilAnswered(c.id);
  const sh = F.doc('Charm_Nest_Sheets/' + SA), RL = await runLines(), line = RL[KEY] || {}, newRow = F.doc('Charm_Pool/' + OLD);
  const onSheet = F.list('Charm_Nest_Sheets').filter(x => !x.archived && (x.poolIds || []).includes(OLD));
  ok('C2 the cloud answers done, in words: the new piece is on a sheet (the old one taken off)', ans.state === 'done' && /its new piece is on .+\(the old piece was taken off\)/.test(ans.text || ''), ans);
  ok('C3 the line\'s piece is made from the card\'s design: a custom pool row (its file\'s path, its size from the traced outline), the line pooled with it, its hold lifted',
    newRow && newRow.custom === true && newRow.aiPath === PDF && newRow.state !== 'superseded' && +newRow.widthPt > 0 && +newRow.areaPt2 > 0 && line.state === 'pooled' && (line.poolIds || []).includes(OLD) && !line.hold, { newRow, line });
  ok('C4 the swapped old piece came off: the sheet no longer carries the OLDSKU charm, the other order stays where it was',
    !(sh.charms || []).some(x => x.id === 'old1') && (sh.charms || []).some(x => x.id === 'oth1') && JSON.stringify((sh.placements || []).find(p => p.id === 'oth1')) === JSON.stringify({ id: 'oth1', cxPt: 80, cyPt: 30, angle: 0 }), { charms: (sh.charms || []).map(x => x.id), placements: sh.placements });
  ok('C5 the new piece is listed on exactly one sheet record of the run, as a custom charm of its line', onSheet.length === 1 && onSheet[0].charms.some(x => x.poolId === OLD && x.custom === true && x.lineKey === KEY) && !!onSheet[0].nestOwed, onSheet.map(x => ({ id: x.id, charms: x.charms.map(c => [c.id, c.custom]) })));
  ok('C6 only its own line was made up and placed: the other order\'s line untouched', RL['4250000002_42500000021'].state === 'pooled' && F.doc('Charm_Pool/' + OTHER).sheetId === SA);
  // asked again: nothing changes
  const before = JSON.stringify(F.list('Charm_Nest_Sheets').map(x => [x.id, x.poolIds, x.layoutRev]));
  const again = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cmd: { type: 'customsend', ck: CK, ids: [KEY], label: 'again' }, cv: CV });
  const a2 = await untilAnswered(again.id);
  ok('C7 asked again: done at once, nothing moved', a2.state === 'done' && /its new piece is on/.test(a2.text || '') && !/taken off/.test(a2.text || '') && JSON.stringify(F.list('Charm_Nest_Sheets').map(x => [x.id, x.poolIds, x.layoutRev])) === before, { a2 });

  // a cut sheet keeps its piece
  await seed(STEPS, { sheet: { roseCutAt: T0 - 60e3 } });
  const r3 = await send(); const a3 = await untilAnswered(r3.c.id);
  ok('C8 the old piece on a CUT sheet: refused in words, nothing moved, the hold kept', a3.state === 'failed' && /was already cut/.test(a3.text || '') && F.doc('Charm_Nest_Sheets/' + SA).poolIds.includes(OLD) && F.doc('Charm_Pool/' + OLD).sku === 'OLDSKU' && !!(await runLines())[KEY].hold, a3);

  // lanes off: words, nothing done
  await seed(['cmd:customsend']);
  const r4 = await send(); const a4 = await untilAnswered(r4.c.id, 2);
  ok('C9 the intake / placement lanes not the cloud\'s: refused in words, nothing moved', a4.state === 'failed' && /not on yet/.test(a4.text || '') && F.doc('Charm_Nest_Sheets/' + SA).poolIds.includes(OLD), a4);
}

function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const cp = src.slice(src.indexOf('async function checkpoint() {'), src.indexOf('async function checkpoint() {') + 200);
  ok('P1 a viewer\'s send skips the browser checkpoint (no "Workspace saving is unavailable" before the cloud hears of it)', /const viewerSend = \(\) => \{[^\n]*!B\.run && !!\(window\.CNViewer/.test(src) && /async function checkpoint\(\) \{\s*if \(viewerSend\(\)\) return;/.test(cp), cp);
  const ask = src.indexOf('remoteOut=await askCarrier(e'), guard = src.indexOf('if(viewerSend() && (!remoteOut || !remoteOut.ok))'), sent = src.indexOf('e.sent={id:intent.id,at:intent.at,by:intent.by,lines:intent.lines};delete e.sendIntent;delete e.sendError;ver++;\n      keepSheetSeal(e)');
  const block = guard > 0 ? src.slice(guard, src.indexOf('}else{', guard)) : '';
  ok('P2 on a viewer the order is sealed / released / said "sent" only after the cloud answered done: otherwise the cloud\'s words, the hold kept (return before e.sent)',
    ask > 0 && guard > ask && sent > guard && /return null;/.test(block) && /toast\(/.test(block) && !/resolve|ReviewRelease|keepSheetSeal|onDone/.test(block), block.slice(0, 400));
  ok('P3 the cloud as the carrier: no 25 s "has not started it yet" (it starts at its next pass), its own words', /if\(!cloud && ?Date\.now\(\)-t0>=25000\)/.test(src) && /cloud=(?:s4On \|\| )?String\(c\??\.carrier\?\.dev \|\| ""\)==="cloud"/.test(src));
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P4 charm-nest-1.html loads the changed bridge under a new token', /charm-nest-bridge\.js\?v=[^"]*-cs1(?:-\w+)*"/.test(html));
}

(async () => {
  try { await partC(); } catch (e) { ok('C run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  partP();
  log0(failed ? `FAIL viewer-customsend (${failed})` : 'PASS viewer-customsend');
  process.exit(failed ? 1 : 0);
})();
