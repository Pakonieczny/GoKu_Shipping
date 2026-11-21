// CESEND (21 Nov 2026): the Charm Editor's "Send to sheet" pop-up on a staff computer (a viewer) in mode "on": "Replace on the order…", another order and no order used to close as
// "sent" while the piece went on a local sheet copy the viewer can never save, and the cloud's placement skipped custom rows. Now the page writes the rows for the cloud
// (customPiecePut, cloudPlace, the run the cloud carries) and asks ONE request (runCmdPut placewaiting { ids, "off:<replaced>" }); the cloud takes the replaced piece off and places the
// new one, and the pop-up says what the cloud said. Offline (_cloudFakes.cjs):
//   A  mode on: the viewer's customPiecePut (cloudPlace) and runCmdPut are taken; the page's own sheet writes are still refused
//   B  Replace on the order: the driver's pass takes the replaced piece off its sheet and places the new piece on a sheet record (frozen, its design file a source), answered in words
//   C  another order, and no order (CUSTOM-…): placed by the cloud, answered in words
//   D  kept skip: a custom row the page placed itself (no cloudPlace) is never placed by the cloud; the request for it is refused in words; lane off: refused in words
//   E  the page's send (charm-nest-custom-piece.js, run in node with stubs): on a viewer it asks the cloud and returns the cloud's words, throws the cloud's refusal; mode off: no cloud call
//   NODE_PATH=<deps> node tests/charm-nest/viewer-cesend.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js'), PlaceUnit = F.load('_charmNestPlaceUnit.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0001', R = 'run-2026-11-21-cesend', A = 'gf-a-cesend', RID = '4227069460', OLD = `${RID}_9100000001_1`;
function seed(steps) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true,
    lines: { [`${RID}_9100000001`]: { state: 'written', orderId: RID, material: 'gold', poolIds: [OLD] } } });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  // sheet A: the order's own piece (the one "Replace on the order…" makes again), nested, not cut
  const c = { id: 'a1', poolId: OLD, order: RID, name: 'a1', areaPt2: 300, widthPt: 20, heightPt: 20, sku: 'HEART_3750' };
  F.seed('Charm_Nest_Sheets/' + A, { id: A, runId: R, metal: 'gold', page: 1, draft: true, status: 'complete', layoutRev: 3, contentRev: 3, fileBase: A, charms: [c], poolIds: [OLD], orders: [RID],
    placements: [{ id: 'a1', cxPt: 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 }] });
  F.seed('Charm_Pool/' + OLD, { poolId: OLD, runId: R, orderId: RID, lineKey: `${RID}_9100000001`, state: 'nested', sheetId: A, material: 'gold', sku: 'HEART_3750' });
}
async function pass() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};
let reqN = 0;
const piecePut = (o) => P({ op: 'customPiecePut', requestId: 'cpreq' + (++reqN) + 'x', by: 'Michelle', dev: DEV, cv: CV, piece: Object.assign({ designId: 'pc-' + OLD, version: 2, designHash: 'dh-1', geomHash: 'gh-abcdef123456', customName: 'Heart', metal: 'gold', quantity: 1,
  orderId: RID, widthMm: 12.5, heightMm: 11.2, aiPath: 'charmnest/custom/designs/pc/v2-abc.pdf', charmHash: 'ch-1', runId: R, cloudPlace: true, widthPt: 35.4, heightPt: 31.7, areaPt2: 900, penHash: 'ph-1' }, o || {}) });
const sheetsListing = id => F.list('Charm_Nest_Sheets').map(x => x.data || x).filter(s => (s.poolIds || []).includes(id));

(async () => {
  const STEPS = ['nest', 'takeoff', 'cmd:placewaiting'];
  /* A */
  seed(STEPS);
  let r = await P({ op: 'putSheet', sheet: { id: A, charms: [], placements: [], poolIds: [] }, dev: DEV, cv: CV });
  ok('A · mode on: the page\'s own sheet write is still refused', r.http === 409 && r.cloudCarries === true, r);
  r = await piecePut();
  const NEW = (r.poolIds || [])[0], row = F.doc('Charm_Pool/' + NEW) || {};
  ok('A · the viewer\'s customPiecePut is taken: a row for the run, cloudPlace, ready, its size in points, on no sheet', r.http === 200 && r.ok && row.custom === true && row.cloudPlace === true && row.runId === R && row.state === 'ready' && !row.sheetId && row.areaPt2 === 900 && row.customPiece.penHash === 'ph-1', { r, row });
  r = await P({ op: 'customPiecePut', requestId: 'cpbad1', by: 'Michelle', dev: DEV, cv: CV, piece: { designId: 'd1', version: 1, geomHash: 'gh-abcdef123456', metal: 'gold', quantity: 1, orderId: RID, runId: R, cloudPlace: true } });
  ok('A · a piece for the cloud without its size is refused in words', r.http >= 400 && /size/.test(r.error || ''), r);
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cv: CV, cmd: { type: 'placewaiting', ids: [NEW, 'off:' + OLD], label: 'Place the custom piece' } });
  ok('A · the viewer\'s ONE request (runCmdPut placewaiting with the ids) is taken', r.http === 200 && r.ok && r.id && r.cmd.args.ids.length === 2, r);
  /* B */
  const id1 = r.id; await pass(); let c = cmdOf(id1);
  const a = F.doc('Charm_Nest_Sheets/' + A), on = sheetsListing(NEW), rec = on[0] || {}, ch = (rec.charms || []).find(x => x.poolId === NEW) || {};
  ok('B · the request is answered done, in words: placed, and the replaced piece taken off', c.state === 'done' && /1 custom piece placed on .+ \(the cloud nests it there\) · the piece it replaces was taken off/.test(c.text || ''), c);
  ok('B · the replaced piece left sheet A (its record and its row)', !a.poolIds.includes(OLD) && !(a.charms || []).some(x => x.poolId === OLD) && !(F.doc('Charm_Pool/' + OLD) || {}).sheetId, { a: a.poolIds, row: F.doc('Charm_Pool/' + OLD) });
  ok('B · the new piece is on exactly one sheet record, owed a nest, its row says so', on.length === 1 && rec.nestOwed > 0 && (F.doc('Charm_Pool/' + NEW) || {}).sheetId === (rec.id), { on: on.map(s => s.id), row: F.doc('Charm_Pool/' + NEW) });
  ok('B · kept as designed: frozen (design, hash, exact size), custom, its design file a source of the record', ch.custom === true && ch.frozen && ch.frozen.geomHash === 'gh-abcdef123456' && ch.frozen.widthMm === 12.5 && ch.frozen.penHash === 'ph-1'
    && (rec.sources || []).some(s => s.id === ch.sourceId && s.path === 'charmnest/custom/designs/pc/v2-abc.pdf'), { ch, sources: rec.sources });
  /* C */
  r = await piecePut({ orderId: '5550001111', customName: 'Spare' });
  const OTHER = r.poolIds[0];
  r = await P({ op: 'customPiecePut', requestId: 'cpnone1', by: 'Michelle', dev: DEV, cv: CV, piece: { designId: 'd-none', version: 1, geomHash: 'gh-abcdef999999', customName: 'Replacement heart for Emma', metal: 'silver', quantity: 2, orderId: null,
    widthMm: 10, heightMm: 10, aiPath: 'charmnest/custom/designs/none/v1.pdf', runId: R, cloudPlace: true, widthPt: 28.3, heightPt: 28.3, areaPt2: 700 } });
  const NONE = r.poolIds;
  ok('C · no order: numbered CUSTOM-<day>-<n>, two rows for the cloud', r.ok && r.orderless === true && /^CUSTOM-\d{8}-\d+$/.test(r.orderId) && NONE.length === 2, r);
  const r1 = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cv: CV, cmd: { type: 'placewaiting', ids: [OTHER] } });
  const r2 = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cv: CV, cmd: { type: 'placewaiting', ids: NONE } });
  await pass();
  c = cmdOf(r1.id); const c2 = cmdOf(r2.id);
  ok('C · another order: placed by the cloud, said in words', c.state === 'done' && /1 custom piece placed on /.test(c.text || '') && sheetsListing(OTHER).length === 1, c);
  const nrec = sheetsListing(NONE[0])[0] || {}, nch = (nrec.charms || []).find(x => x.poolId === NONE[0]) || {};
  ok('C · no order: both copies placed on a Sterling Silver sheet, named "… · Custom · no order"', c2.state === 'done' && /2 custom pieces placed on /.test(c2.text || '') && NONE.every(id => sheetsListing(id).length === 1) && nrec.metal === 'silver' && /Custom · no order/.test(nch.name || '') && nch.frozen && nch.frozen.orderless === true, { c2, nch });
  /* D */
  seed(STEPS);
  r = await piecePut({ cloudPlace: false });
  const PAGE = r.poolIds[0];
  ok('D · a custom row the page places itself carries no cloudPlace', r.ok && !(F.doc('Charm_Pool/' + PAGE) || {}).cloudPlace, F.doc('Charm_Pool/' + PAGE));
  const run = Object.assign({}, F.doc('Charm_Nest_Runs/' + R)); run.lines = Object.assign({}, run.lines, { [`${RID}_x`]: { state: 'pooled', orderId: RID, poolIds: [PAGE] } });
  const ctx = { db: F.db, runId: R, run, read: b => lib.call ? lib.call(b) : P(b), call: async () => ({ status: 500, error: 'nothing may be written' }), budgetEnd: Date.now() + 30000 };
  let threw = null; try { await PlaceUnit.placeAll(ctx); } catch (e) { threw = e.message; }
  ok('D · kept skip: the placement unit never places a custom row the page placed itself (nothing written, nothing tried)', threw === null && !sheetsListing(PAGE).length, threw);
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cv: CV, cmd: { type: 'placewaiting', ids: [PAGE] } });
  await pass(); c = cmdOf(r.id);
  ok('D · a request naming such a row is refused in words, nothing placed', c.state === 'failed' && /not a custom piece sent for the run/.test(c.text || '') && !sheetsListing(PAGE).length, c);
  seed(['nest']);
  r = await piecePut();
  const r3 = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cv: CV, cmd: { type: 'placewaiting', ids: [r.poolIds[0]] } });
  await pass(); c = cmdOf(r3.id);
  ok('D · lane "cmd:placewaiting" off: answered in words by the driver, nothing placed', c.state === 'failed' && /placewaiting/.test(c.text || '') && !sheetsListing(r.poolIds[0]).length, c);
  ok('rule · Etsy was never called', F.etsy.calls === 0);

  /* E (the page's send, in node, with stubs: CN.api, Pool.addCustomPiece, CNViewer) */
  const g = globalThis, calls = [];
  let mode = 'on', answer = 'done';
  g.CNViewer = { on: () => mode === 'on', lane: () => true };
  g.CharmNestRunFollow = { state: () => ({ runId: R }), dev: () => DEV, now: () => {} };
  g.CNEmployee = { name: () => 'Michelle' };
  g.CNDesignLog = { event: () => {} };
  g.Pool = { addCustomPiece: async o => (calls.push(['add', o]), mode === 'on' ? { poolIds: ['4227069460_202611210001_1'], cloud: true, runId: R, orderId: RID } : ['4227069460_202611210001_1']) };
  let listed = 0;
  g.CN = { api: async (fn, b) => { calls.push([b.op, b]); if (b.op === 'runCmdPut') return { ok: true, id: 'c1' }; if (b.op === 'runCmdList') { listed++; return { cmds: [listed < 2 ? { id: 'c1', state: 'running', step: 'Placing the custom piece…' } : answer === 'done' ? { id: 'c1', state: 'done', text: '1 custom piece placed on GF Sheet 2 (the cloud nests it there) · the piece it replaces was taken off GF Sheet 1' } : { id: 'c1', state: 'failed', text: 'Not placed: it waits for GF Sheet 1' }] }; } return {}; } };
  delete require.cache[require.resolve('../../charm-nest-custom-piece.js')];
  const CP = require('../../charm-nest-custom-piece.js'), Cp = CP && CP.send ? CP : g.CharmNestCustomPiece;
  const design = { id: 'pc-' + OLD, version: 2, name: 'Heart', faces: { front: { parts: [] } }, widthMm: 12.5, heightMm: 11.2 };
  const steps = [];
  const out = await Cp.send({ designId: design.id, version: 2, design, metal: 'gold', quantity: 1, orderId: RID, by: 'Michelle', replaces: OLD, onStep: t => steps.push(t), followMs: 1 });
  const put = calls.find(x => x[0] === 'runCmdPut');
  ok('E · viewer: ONE request, placewaiting with the new piece and "off:" the replaced one', calls.filter(x => x[0] === 'runCmdPut').length === 1 && put[1].cmd.type === 'placewaiting' && put[1].cmd.ids.join() === `4227069460_202611210001_1,off:${OLD}` && put[1].runId === R, put);
  ok('E · viewer: the pop-up is told the cloud\'s steps, and the answer is the cloud\'s words (never "sent" before)', out.cloud === true && /placed on GF Sheet 2/.test(out.cloudText) && steps.some(t => /The cloud: Placing/.test(t)), { out, steps });
  answer = 'failed'; listed = 0; let err = null;
  try { await Cp.send({ designId: design.id, version: 2, design, metal: 'gold', quantity: 1, orderId: null, customName: 'Spare', by: 'Michelle', followMs: 1 }); } catch (e) { err = e.message; }
  ok('E · viewer: the cloud\'s refusal is thrown in its words (the pop-up stays open and says it)', /the cloud did not place it: Not placed: it waits for GF Sheet 1/.test(err || ''), err);
  mode = 'off'; calls.length = 0;
  const off = await Cp.send({ designId: design.id, version: 2, design, metal: 'gold', quantity: 1, orderId: RID, by: 'Michelle', replaces: OLD });
  ok('E · mode off: unchanged (the page places it; no cloud request, no "off:")', !calls.some(x => x[0] === 'runCmdPut' || x[0] === 'runCmdList') && !off.cloud && off.poolIds.length === 1 && calls[0][0] === 'add', calls.map(x => x[0]));
  const pool = fs.readFileSync(path.join(__dirname, '../../charm-nest-custom-pool.js'), 'utf8'), vb = pool.slice(pool.indexOf('if (viewer) {   // (CESEND'), pool.indexOf('const at = Date.now(), n = rep.pools.length'));
  ok('E · Pool.addCustomPiece on a viewer: rows for the cloud (cloudPlace, the run), returned before any sheet, nest or save', /cloudPlace: true/.test(pool) && /return \{ poolIds: rep\.poolIds\.slice\(\), cloud: true/.test(vb) && !/placeOn|sheetFor|startNest|Session/.test(vb));
  const html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  ok('E · charm-nest-1.html loads both changed files under new tokens', /charm-nest-custom-pool\.js\?v=[^"]*-ce1(?:-\w+)*"/.test(html) && /charm-nest-custom-piece\.js\?v=[^"]*-apv1-ce1(?:-\w+)*"/.test(html));
  log0(failed ? `FAIL viewer-cesend (${failed})` : 'PASS viewer-cesend');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL crashed: ' + (e && e.stack || e)); process.exit(1); });
