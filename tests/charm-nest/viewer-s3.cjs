// S3 (21 Nov 2026): on a staff computer (a viewer) in mode "on":
//   1. artwork added by hand (a dropped or chosen .ai / .pdf / .zip) went onto the page's newest sheet and "nested in the cloud" while the cloud nested ITS record without the new
//      pieces (a viewer saves no sheet): now each design is saved as its own file, recorded as a piece of no order the cloud places (customPiecePut { cloudPlace, hand }) and ONE
//      request asks the cloud to place it (runCmdPut placewaiting); the cloud places it (never frozen, its own file a source of the record) and the person is told the cloud's words;
//   2. the charm window's Exclude / Include / Move to <metal> of a piece of no order changed only page memory: now a press saved on the sheet's record (sheetLayoutPatch { charms }),
//      honoured by the cloud's nest, a move placed by the cloud on a sheet of the new metal.
// Offline (_cloudFakes.cjs):
//   A  the server: customPiecePut hand (refusals in words), the cloud places hand pieces (not frozen, hand, own source each, named by the person's file), nest owed
//   B  sheetCharmPatch: Exclude (kept on the record, out of the layout, the cloud's nest owes it and leaves it out), Include (nest owed), Move (row to the new metal, the cloud places it
//      on a sheet of that metal, answered in words), refused in words for an order's piece and a laser-cut sheet; a piece dropped before the cloud carried the run: Exclude taken,
//      Include / Move refused in words
//   C  the page (charm-nest-viewer.js in node, its api wired to the fake cloud): handArt (one file per design, copies as quantity, ONE request, the cloud's words), refused in
//      words with nothing sent; charmPress (exclude / include / move followed to the cloud's answer, refused in words)
//   D  charm-nest-1.html glue: flushManualIntake on a viewer sends to the cloud and puts nothing on a sheet (run with stubs); handToCloud: success said, failure gives the pieces back
//      to the rail; autoNest nests nothing on a viewer; the charm window's viewer presses; mode off unchanged; new cache tokens
//   NODE_PATH=<deps> node tests/charm-nest/viewer-s3.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 21, 18, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js'), Nest = F.load('_charmNestNest.js');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0003', R = 'run-2026-11-21-s3', A = 'gf-a-s3', RID = '4227069999', ORDERPID = `${RID}_9100000009_1`;
const STEPS = ['nest', 'takeoff', 'cmd:placewaiting'];
function seed(steps) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true,
    lines: { [`${RID}_9100000009`]: { state: 'written', orderId: RID, material: 'gold', poolIds: [ORDERPID] } } });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  // sheet A (Gold Filled): an order's piece, nested; and a piece dropped on a computer before the cloud carried the run (no pool row)
  F.seed('Charm_Nest_Sheets/' + A, { id: A, runId: R, metal: 'gold', page: 1, draft: true, status: 'complete', layoutRev: 3, contentRev: 3, fileBase: A, archived: false,
    charms: [{ id: 'a1', poolId: ORDERPID, order: RID, name: 'a1', areaPt2: 300, widthPt: 20, heightPt: 20, sku: 'HEART_3750' }, { id: 'old:0', name: 'Old star', order: 'old:0', sourceId: 'old', sourceName: 'old.ai', areaPt2: 200, widthPt: 15, heightPt: 15 }],
    poolIds: [ORDERPID], orders: [RID], placements: [{ id: 'a1', cxPt: 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 }, { id: 'old:0', cxPt: 70, cyPt: 30, angle: 0, wPt: 15, hPt: 15, scale: 1 }] });
  F.seed('Charm_Pool/' + ORDERPID, { poolId: ORDERPID, runId: R, orderId: RID, lineKey: `${RID}_9100000009`, state: 'nested', sheetId: A, material: 'gold', sku: 'HEART_3750' });
}
async function pass() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};
const sheetsListing = id => F.list('Charm_Nest_Sheets').map(x => x.data || x).filter(s => (s.poolIds || []).includes(id));
let reqN = 0;
const handPut = o => P({ op: 'customPiecePut', requestId: 'hdreq' + (++reqN) + 'x', by: 'Michelle', dev: DEV, cv: CV, piece: Object.assign({ designId: 'hand-ab12cd34', version: 0, designHash: null, geomHash: 'ab12cd34', customName: 'star-01', metal: 'gold', quantity: 1,
  orderId: null, widthMm: 10, heightMm: 10, aiPath: 'charmnest/charms/ab12cd34.ai', charmHash: 'ab12cd34', runId: R, cloudPlace: true, hand: true, widthPt: 28.35, heightPt: 28.35, areaPt2: 600 }, o || {}) });
const patch = (key, change, o) => P(Object.assign({ op: 'sheetLayoutPatch', id: A, by: 'Michelle', dev: DEV, cv: CV, charms: { [key]: change } }, o || {}));

(async () => {
  /* ═══ A · the server: a hand piece recorded and placed by the cloud ═══ */
  seed(STEPS);
  let r = await P({ op: 'putSheet', sheet: { id: A, charms: [], placements: [], poolIds: [] }, dev: DEV, cv: CV });
  ok('A · mode on: the page\'s own sheet write is still refused', r.http === 409 && r.cloudCarries === true, r);
  r = await handPut();
  const H1 = (r.poolIds || [])[0], row1 = F.doc('Charm_Pool/' + H1) || {};
  ok('A · customPiecePut hand: a row of no order (CUSTOM-…), for the cloud to place on the run, marked hand, its file and size', r.http === 200 && r.ok && r.orderless === true && /^CUSTOM-\d{8}-\d+$/.test(r.orderId)
    && row1.hand === true && row1.customPiece.hand === true && row1.cloudPlace === true && row1.runId === R && row1.aiPath === 'charmnest/charms/ab12cd34.ai' && !row1.sheetId && row1.areaPt2 === 600, { r, row1 });
  r = await handPut({ cloudPlace: false });
  ok('A · hand artwork not for the cloud is refused in words', r.http >= 400 && /cloudPlace|no order/.test(r.error || ''), r);
  r = await handPut({ orderId: '5550001111' });
  ok('A · hand artwork is never an order\'s piece: refused in words', r.http >= 400 && /no order/.test(r.error || ''), r);
  r = await handPut({ geomHash: 'ef56ab78', charmHash: 'ef56ab78', designId: 'hand-ef56ab78', aiPath: 'charmnest/charms/ef56ab78.ai', quantity: 2 });   // (another design given the same name, two copies)
  const H2 = r.poolIds || [];
  r = await P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Michelle', cv: CV, cmd: { type: 'placewaiting', ids: [H1].concat(H2), label: 'Place the artwork added by hand' } });
  ok('A · ONE request names the three pieces', r.http === 200 && r.ok && r.cmd.args.ids.length === 3, r);
  await pass(); let c = cmdOf(r.id);
  const on1 = sheetsListing(H1), rec1 = on1[0] || {}, ch1 = (rec1.charms || []).find(x => x.poolId === H1) || {}, ch2 = (rec1.charms || []).find(x => x.poolId === H2[0]) || {};
  ok('A · the cloud placed them and said so in words', c.state === 'done' && /3 custom pieces placed on /.test(c.text || '') && H2.every(id => sheetsListing(id).length === 1), c);
  ok('A · on exactly one Gold Filled record, owed a nest, the rows say where', on1.length === 1 && rec1.metal === 'gold' && rec1.nestOwed > 0 && (F.doc('Charm_Pool/' + H1) || {}).sheetId === rec1.id, { on: on1.map(s => s.id), rec: rec1.nestOwed });
  ok('A · a hand piece is NOT frozen (no design-library hash checked in its files), marked hand, custom, named by the person\'s file', !ch1.frozen && ch1.hand === true && ch1.custom === true && /^star-01 · by hand · no order$/.test(ch1.name || '') && /star-01 · by hand · no order · 1\/2/.test(ch2.name || ''), { ch1, ch2 });
  const s1 = (rec1.sources || []).find(s => s.id === ch1.sourceId), s2 = (rec1.sources || []).find(s => s.id === ch2.sourceId);
  ok('A · each design\'s own file is a source of the record, under a name of its own (the same name given twice never mixes them up)', s1 && s2 && s1 !== s2 && s1.path === 'charmnest/charms/ab12cd34.ai' && s2.path === 'charmnest/charms/ef56ab78.ai' && s1.name !== s2.name
    && (rec1.sources || []).find(s => s && (s.id === ch2.sourceId || s.name === ch2.sourceName)) === s2, { sources: rec1.sources, ch2 });
  ok('A · the cloud\'s nest owes the sheet (its new pieces wait to be nested)', Nest.owedNest(rec1, F.doc('Charm_Nest_Runs/' + R), Date.now()).owed === true, Nest.owedNest(rec1, F.doc('Charm_Nest_Runs/' + R), Date.now()));

  /* ═══ B · Exclude / Include / Move, saved on the record ═══ */
  // the hand piece on sheet A, nested (every piece of A placed), as the cloud's nest leaves it (moved there first when the cloud chose another Gold Filled sheet)
  if (rec1.id !== A) {
    const ax = F.doc('Charm_Nest_Sheets/' + A); ax.charms.push(Object.assign({}, ch1)); ax.poolIds.push(H1); ax.sources = (ax.sources || []).concat((rec1.sources || []).filter(s => s.id === ch1.sourceId)); F.seed('Charm_Nest_Sheets/' + A, ax);
    const rs = F.doc('Charm_Nest_Sheets/' + rec1.id); rs.charms = rs.charms.filter(x => x.poolId !== H1); rs.poolIds = rs.poolIds.filter(x => x !== H1); F.seed('Charm_Nest_Sheets/' + rec1.id, rs);
    const row = F.doc('Charm_Pool/' + H1); row.sheetId = A; F.seed('Charm_Pool/' + H1, row);
  }
  { const ax = F.doc('Charm_Nest_Sheets/' + A); let k = 0; for (const x of ax.charms) if (!ax.placements.some(p => p.id === x.id)) ax.placements.push({ id: x.id, cxPt: 110 + 30 * k++, cyPt: 30, angle: 0, wPt: 28, hPt: 28, scale: 1 });
    ax.dirty = false; ax.nestOwed = null; F.seed('Charm_Nest_Sheets/' + A, ax); }
  const a0 = F.doc('Charm_Nest_Sheets/' + A), HID = ((a0.charms || []).find(x => x.poolId === H1) || {}).id;
  ok('B · (before) the sheet owes no nest', Nest.owedNest(a0, F.doc('Charm_Nest_Runs/' + R), Date.now()).owed === false);
  r = await patch(H1, { excluded: true });
  let a = F.doc('Charm_Nest_Sheets/' + A), hc = (a.charms || []).find(x => x.poolId === H1) || {};
  ok('B · Exclude: taken, the charm stays on the record marked excluded (by whom), its place leaves the layout, the sheet changed', r.http === 200 && r.ok && r.excluded.includes(H1) && hc.excluded === true && hc.excludedBy === 'Michelle'
    && HID && !(a.placements || []).some(p => p.id === HID) && a.dirty === true && a.layoutRev === a0.layoutRev + 1 && a.poolIds.includes(H1), { r, hc, a: { d: a.dirty, l: a.layoutRev } });
  const owedX = Nest.owedNest(a, F.doc('Charm_Nest_Runs/' + R), Date.now());
  ok('B · the cloud\'s nest honours it: the sheet is nested again, the excluded piece is not among what it nests', owedX.owed === true && !Nest.activeCharms(a).some(x => x.poolId === H1), owedX);
  ok('B · every computer reads it from the record (the row still names the sheet, nothing else moved)', (F.doc('Charm_Pool/' + H1) || {}).sheetId === A && (a.charms || []).some(x => x.poolId === ORDERPID));
  r = await patch(H1, { excluded: false });
  a = F.doc('Charm_Nest_Sheets/' + A); hc = (a.charms || []).find(x => x.poolId === H1) || {};
  ok('B · Include: taken, the mark is off and a nest is owed that places it', r.ok && r.included.includes(H1) && hc.excluded === false && a.nestOwed && a.nestOwed.why === 'included by hand' && Nest.owedNest(a, F.doc('Charm_Nest_Runs/' + R), Date.now()).owed === true
    && Nest.activeCharms(a).some(x => x.poolId === H1), { r, hc, owed: a.nestOwed });
  const before = JSON.stringify(F.doc('Charm_Nest_Sheets/' + A));
  r = await patch(ORDERPID, { excluded: true });
  ok('B · an order\'s piece is refused in words, nothing changed', r.http === 409 && r.refused === 'line' && /order/.test(r.error || '') && JSON.stringify(F.doc('Charm_Nest_Sheets/' + A)) === before, r);
  r = await patch('old:0', { excluded: false });
  ok('B · a piece dropped before the cloud carried the run, asked back in: refused in words (the cloud cannot cut it)', r.http === 409 && r.refused === 'legacy' || (r.ok && r.same === true), r);
  r = await patch('old:0', { excluded: true });
  a = F.doc('Charm_Nest_Sheets/' + A);
  ok('B · … but its Exclude is taken (the cloud\'s nest stops listing it)', r.ok && (a.charms.find(x => x.id === 'old:0') || {}).excluded === true && !a.placements.some(p => p.id === 'old:0'), r);
  r = await patch('old:0', { excluded: false });
  ok('B · … and asked back in again: refused in words, nothing changed', r.http === 409 && r.refused === 'legacy' && /dropped on a computer before the cloud carried the run/.test(r.error || ''), r);
  r = await patch('old:0', { metal: 'silver' });
  ok('B · … moved: refused in words', r.http === 409 && r.refused === 'legacy', r);
  r = await patch('nope', { excluded: true });
  ok('B · a piece no longer on the sheet: refused in words', r.http === 409 && r.refused === 'gone', r);
  // Move to Sterling Silver
  r = await patch(H1, { metal: 'silver' });
  a = F.doc('Charm_Nest_Sheets/' + A); let hrow = F.doc('Charm_Pool/' + H1) || {};
  ok('B · Move: taken, the piece leaves the record (charm, pool id, place), its row takes the new metal for the cloud to place, one request queued', r.http === 200 && r.ok && r.moved.length === 1 && r.cmdId
    && !(a.charms || []).some(x => x.poolId === H1) && !a.poolIds.includes(H1) && hrow.material === 'silver' && !hrow.sheetId && hrow.cloudPlace === true && hrow.runId === R && cmdOf(r.cmdId).args.ids[0] === H1, { r, hrow });
  const mv = r.cmdId; await pass(); c = cmdOf(mv);
  const onS = sheetsListing(H1);
  ok('B · the cloud placed it on a Sterling Silver sheet and said so in words', c.state === 'done' && /placed on /.test(c.text || '') && onS.length === 1 && onS[0].metal === 'silver' && (F.doc('Charm_Pool/' + H1) || {}).sheetId === onS[0].id, { c, onS: onS.map(s => [s.id, s.metal]) });
  // a laser-cut sheet
  { const x = F.doc('Charm_Nest_Sheets/' + A); x.laserDoneAt = T0; F.seed('Charm_Nest_Sheets/' + A, x); }
  r = await patch('old:0', { excluded: true });
  ok('B · a laser-cut sheet: refused in words', r.http === 409 && r.refused === 'cut', r);

  /* ═══ C · the page (charm-nest-viewer.js in node, its requests to the fake cloud) ═══ */
  seed(STEPS);
  const g = globalThis, toasts = [], sent = [];
  let who = 'Michelle', apiDown = false;
  g.toast = (t, tone) => { toasts.push([String(t), tone]); };
  g.employeeName = () => who;
  g.CharmNestRunFollow = { state: () => ({ runId: R }), dev: () => DEV, now: () => {} };
  g.CN = { api: async (fn, b) => {
    if (fn !== 'charmNestLibrary') throw new Error('only the library');
    sent.push(b.op);
    if (apiDown && b.op !== 'driveGet') throw new Error('offline');
    if (b.op === 'runCmdList') { await pass(); F.clock.advance(1000); }
    const out = await P(Object.assign({ cv: CV }, b));
    return out;
  } };
  delete require.cache[require.resolve('../../charm-nest-viewer.js')];
  require('../../charm-nest-viewer.js');
  const V = g.CNViewer; await V.read('test');
  ok('C · the page is a viewer (mode on, from the cloud)', V.on() === true && V.lane('cmd:placewaiting') === true, V.state());
  const uploads = [];
  const art = [{ id: 's:0', hash: '11aa22bb', name: 'moon-01', sourceName: 'moons.ai', index: 0, widthPt: 30, heightPt: 25, areaPt2: 500 }, { id: 's:1', hash: '11aa22bb', name: 'moon-01', sourceName: 'moons.ai', index: 1, widthPt: 30, heightPt: 25, areaPt2: 500 },
    { id: 's:2', hash: '33cc44dd', name: null, sourceName: 'moons.ai', index: 2, widthPt: 20, heightPt: 20, areaPt2: 300 }];
  const io = { fileOf: async c0 => new Uint8Array([37, 80, 68, 70, c0.index]), upload: async (p, bytes) => { uploads.push([p, bytes.length]); return { path: p, url: 'https://x/' + p }; }, day: '2026-11-21', followMs: 1 };
  sent.length = 0;
  let out = await V.handArt('silver', art, io);
  const rows = F.list('Charm_Pool').filter(x => x.hand === true);
  ok('C · handArt: one file per design saved in the cloud (charmnest/charms/<hash>.ai), the copies as its quantity', uploads.length === 2 && uploads.map(u => u[0]).sort().join() === 'charmnest/charms/11aa22bb.ai,charmnest/charms/33cc44dd.ai'
    && rows.length === 3 && rows.filter(x => x.charmHash === '11aa22bb').length === 2 && rows.every(x => x.material === 'silver' && x.cloudPlace === true && x.runId === R && x.orderless === true), { uploads, rows: rows.map(x => [x.poolId, x.charmHash, x.material]) });
  ok('C · a design with no name yet goes under its file\'s name', rows.some(x => x.charmHash === '33cc44dd' && x.customName === 'moons-03'), rows.map(x => x.customName));
  ok('C · ONE request asks the cloud to place them all; nothing nested or saved by the page', sent.filter(x => x === 'runCmdPut').length === 1 && !sent.some(x => ['nestStart', 'putSheet', 'poolPut', 'placeOnSheet'].includes(x)), sent);
  ok('C · the answer is the cloud\'s own words (placed on its sheet), and every piece is on a record', out.ok === true && /3 custom pieces placed on /.test(out.text) && out.poolIds.length === 3 && out.poolIds.every(id => sheetsListing(id).length === 1), out);
  // refusals, nothing sent
  sent.length = 0; who = '';
  out = await V.handArt('silver', art, io);
  ok('C · nobody signed in: refused in words, nothing sent', !out.ok && /sign in/.test(out.error) && !sent.some(x => x === 'customPiecePut' || x === 'runCmdPut'), { out, sent });
  who = 'Michelle';
  const stateWas = g.CharmNestRunFollow.state; g.CharmNestRunFollow.state = () => ({ runId: null });
  out = await V.handArt('silver', art, io);
  ok('C · no run open in the cloud: refused in words, nothing sent', !out.ok && /no run is open/.test(out.error) && !sent.some(x => x === 'customPiecePut'), out);
  g.CharmNestRunFollow.state = stateWas;
  out = await V.handArt('silver', art, { fileOf: io.fileOf, upload: async () => { throw new Error('storage refused'); } });
  ok('C · its file not saved: refused in words, nothing recorded or asked', !out.ok && /could not be saved in the cloud \(storage refused\)/.test(out.error) && /Nothing was sent/.test(out.error) && !sent.some(x => x === 'customPiecePut' || x === 'runCmdPut'), out);
  // charmPress: the record's hand piece, as a card shows it
  const P1 = rows[0].poolId, recS = sheetsListing(P1)[0];
  { const x = F.doc('Charm_Nest_Sheets/' + recS.id), ch = x.charms.find(z => z.poolId === P1); x.placements = (x.placements || []).concat([{ id: ch.id, cxPt: 40, cyPt: 40, angle: 0, wPt: 30, hPt: 25, scale: 1 }]); x.dirty = false; F.seed('Charm_Nest_Sheets/' + recS.id, x); }
  const sh = { sheetId: recS.id, fileBase: 'SS Draft 1' }, card = { poolId: P1, name: 'moon-01 · by hand · no order', hand: true };
  toasts.length = 0;
  let pr = await V.charmPress(sh, card, { excluded: true });
  let x1 = F.doc('Charm_Nest_Sheets/' + recS.id);
  ok('C · charmPress Exclude: saved on the record, said in words (never before the cloud took it)', pr.ok && (x1.charms.find(z => z.poolId === P1) || {}).excluded === true && /excluded — saved in the cloud/.test((toasts[0] || [])[0] || ''), { pr, toasts });
  toasts.length = 0; pr = await V.charmPress(sh, card, { excluded: false });
  x1 = F.doc('Charm_Nest_Sheets/' + recS.id);
  ok('C · charmPress Include: saved, a nest owed, said in words', pr.ok && (x1.charms.find(z => z.poolId === P1) || {}).excluded === false && x1.nestOwed && /included — saved in the cloud/.test((toasts[0] || [])[0] || ''), toasts);
  toasts.length = 0; pr = await V.charmPress(sh, { poolId: 'NOPE_1', name: 'Ghost' }, { excluded: true });
  ok('C · charmPress refused: said in words, nothing changed', !pr.ok && /Ghost: not excluded — That piece is no longer on/.test((toasts[0] || [])[0] || '') && toasts[0][1] === 'bad', toasts);
  toasts.length = 0; pr = await V.charmPress(sh, card, { metal: 'gold' }, { wait: true, followMs: 1 });
  ok('C · charmPress Move: taken off, the cloud placed it on a Gold Filled sheet, said in its words', pr.ok && pr.follow && pr.follow.ok && sheetsListing(P1).length === 1 && sheetsListing(P1)[0].metal === 'gold'
    && toasts.some(t => /taken off SS Draft 1 — the cloud places it on Gold Filled/.test(t[0])) && toasts.some(t => /placed on /.test(t[0]) && t[1] === 'ok'), { pr: pr.follow, toasts });
  apiDown = true; toasts.length = 0; pr = await V.charmPress(sh, card, { excluded: true });
  ok('C · the cloud not reached: refused in words, never "done"', !pr.ok && /not excluded — offline/.test((toasts[0] || [])[0] || ''), toasts);
  apiDown = false;

  /* ═══ D · charm-nest-1.html glue ═══ */
  const html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  const cut = (from, to) => { const i = html.indexOf(from), j = html.indexOf(to, i + 1); return i >= 0 && j > i ? html.slice(i, j) : ''; };
  const flushSrc = cut('function flushManualIntake(metal) {', '/* S3 (Paul 21 Nov 2026: "staff keep FULL use'), handSrc = cut('function handToCloud(metal,sources){', '/** Start only after all files'), autoSrc = cut('function autoNest(metal) {', 'function assignCharm(');
  ok('D · the three pieces of glue are in the page', !!flushSrc && !!handSrc && !!autoSrc);
  // run flushManualIntake + handToCloud with stubs: a viewer, two files of Gold Filled artwork ready
  const run = (viewerOn, answer) => {
    const calls = [], boom = n => () => { calls.push(n); throw new Error(n + ' must not run on a viewer'); };
    const src = { id: 'S1', name: 'stars.ai', state: 'ready', metal: 'gold', charms: [{ id: 'S1:0', hash: 'aa', sourceName: 'stars.ai', pendingMetal: 'gold' }, { id: 'S1:1', hash: 'bb', sourceName: 'stars.ai', pendingMetal: 'gold', open: true, excluded: true }] };
    const Sx = { sources: [src], unassigned: [src.charms[0]], settings: { autoNest: 'on' } };
    const env = { window: { B: { run: null }, CNViewer: { on: () => viewerOn, handArt: async (m, list, o) => { calls.push(['handArt', m, list.map(c => c.id), typeof o.fileOf, typeof o.upload]); return answer; } } },
      S: Sx, METALS: [{ key: 'gold' }, { key: 'silver' }], activePage: boom('activePage'), pagesOf: boom('pagesOf'), startNest: boom('startNest'), addPage: boom('addPage'), renderCard: boom('renderCard'), manualSheetClosed: boom('manualSheetClosed'),
      renderRail: () => calls.push('renderRail'), updateTopSub: () => {}, labelOf: m => m === 'gold' ? 'Gold Filled' : m, today: () => '2026-11-21', toast: (t, tone) => calls.push(['toast', t, tone]), agent: (s, k, t) => ({ t }), agentUpdate: (ev, p) => calls.push(['ev', p.text]),
      sourceOf: () => null, CharmNestPDF: {}, uploadBytes: async () => ({}) };
    env.CNViewer = env.window.CNViewer;
    const fn = new Function(...Object.keys(env), `${flushSrc}\n${handSrc}\nreturn { flushManualIntake, handToCloud };`)(...Object.values(env));
    fn.flushManualIntake();
    return { calls, src, Sx };
  };
  let t = run(true, { ok: true, text: '1 custom piece placed on GF Draft 2 (the cloud nests it there)', poolIds: ['p1'] });
  await new Promise(res => setTimeout(res, 5));
  const hcall = t.calls.find(x => Array.isArray(x) && x[0] === 'handArt');
  ok('D · a viewer: flushManualIntake sends the artwork to the cloud (handArt with its file maker and its upload) and touches no sheet, nests nothing', hcall && hcall[1] === 'gold' && hcall[2].join() === 'S1:0' && hcall[3] === 'function' && hcall[4] === 'function'
    && !t.calls.some(x => typeof x === 'string' && /activePage|pagesOf|startNest|addPage|renderCard|manualSheetClosed/.test(x)), t.calls);
  ok('D · the piece is the cloud\'s now (not pending, not in the rail), the open outline stays out, and the cloud\'s words are said', !t.src.charms[0].pendingMetal && t.src.charms[0].cloudSend.state === 'placed' && !t.Sx.unassigned.length && !t.src.charms[1].cloudSend
    && t.calls.some(x => Array.isArray(x) && x[0] === 'toast' && /stars\.ai: 1 custom piece placed on GF Draft 2/.test(x[1]) && x[2] === 'ok'), t.calls.filter(x => Array.isArray(x)));
  t = run(true, { ok: false, error: 'no run is open in the cloud: start the run, then add the artwork again. Nothing was sent', poolIds: [] });
  await new Promise(res => setTimeout(res, 5));
  ok('D · refused: said in words, the piece goes back to the rail to be sent again', t.Sx.unassigned.includes(t.src.charms[0]) && !t.src.charms[0].cloudSend && t.calls.some(x => Array.isArray(x) && x[0] === 'toast' && /not sent — no run is open in the cloud/.test(x[1]) && x[2] === 'bad'), t.calls.filter(x => Array.isArray(x)));
  let threw = null; try { run(false, null); } catch (e) { threw = e.message; }
  ok('D · mode off: the page\'s own intake as before (it goes on to the newest sheet)', /activePage must not run|pagesOf must not run/.test(threw || ''), threw);
  ok('D · autoNest nests nothing on a viewer', /flushManualIntake\(metal\);\s*if\(window\.CNViewer&&typeof CNViewer\.on==='function'&&CNViewer\.on\(\)\)return;/.test(autoSrc));
  ok('D · handToCloud never puts a piece on a sheet or nests', !/startNest|sh\.charms\.push|addPage|Session/.test(handSrc));
  const oc = cut('function openCharm(sh, c, placement) {', 'async function pinInCloud(');
  ok('D · the charm window: a hand piece keeps Exclude / Include and Move on a viewer, each a press saved in the cloud; mode off as before', /const line = !!c\.poolId && !hand;/.test(oc) && /if \(ex && viewerCharm\) ex\.onclick = \(\) => \{[^}]*CNViewer\.charmPress\(sh, c, \{ excluded: want \}\)/.test(oc)
    && /if \(viewerCharm\) \$\$\("\[data-move\]"[^\n]*CNViewer\.charmPress\(sh, c, \{ metal \}\)/.test(oc) && /else if \(ex\) ex\.onclick = \(\) => \{ const lift/.test(oc) && /else \$\$\("\[data-move\]"[\s\S]{0,400}assignCharm\(c, metal\)/.test(oc), oc.length);
  const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
  ok('D · every computer\'s card shows a hand piece left out (its record), so Include can be pressed there', /x\.excluded === true && x\.hand === true && x\.poolId/.test(bridge) && /if \(rc\.hand === true\) charm\.hand = true;/.test(bridge));
  ok('D · charm-nest-1.html loads the changed files under new tokens', /charm-nest-viewer\.js\?v=[^"]*-s3(?:-\w+)*"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-s3(?:-\w+)*"/.test(html));
  ok('rule · Etsy was never called', F.etsy.calls === 0);
  log0(failed ? `FAIL viewer-s3 (${failed})` : 'PASS viewer-s3');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL crashed: ' + (e && e.stack || e)); process.exit(1); });
