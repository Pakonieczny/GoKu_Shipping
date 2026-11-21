// ITEM 10 (Paul, 21 Nov 2026): the Charm Editor's Custom-portion "Done" (piece kind), and its Undo, refused on every viewer in mode "on" ("The custom piece was not completed · The cloud
// carries every run now: poolUpdate…"). Now one press, customDone (_charmNestCustomDone.js), done in the cloud at once: the piece off every sheet that lists it, in one transaction with its rows.
//   S  the library in mode "on" (offline, _cloudFakes.cjs): Done takes the piece off its sheet (charms, placements, poolIds, revs, filesOwed) and marks its row covered, the timeline says
//      "removed"; a retry writes nothing; Undo makes it live again; a non-custom piece, a cut sheet, a busy sheet (two sheets: nothing half done), an Undo of a piece not covered are
//      refused whole; the page's own poolUpdate / putSheet stay refused (nothing else newly allowed); an old page gets the reload words; mode off: customDone refused, poolUpdate as before
//   P  charm-nest-custom-done.js on a viewer (node, the page faked): Done sends ONE customDone, no Pool.update, no takeOffGone, then the Etsy custom line is released; a refusal releases
//      nothing; Undo sends customDone undo; not a viewer (mode off): exactly the old path
//   G7 (21 Nov 2026): Undo puts the piece back where it was (same sheet, same place) when the sheet can take it and the place is free; else the cloud places it (cloudPlace + runCmdPut
//      placewaiting, lane on), or it is live on no sheet with the reason; the page says where, and follows the cloud's placing until it answers
// Offline only. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-item10.cjs
'use strict';
const path = require('path');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-item10', RID = '4225890898', CP = `${RID}_202611210001_1`, CP2 = `${RID}_202611210001_2`, LP = `${RID}_5282523907_1`, SH = 'gold-s41-item10', SH2 = 'gold-s42-item10';
    const cover = { poolId: LP, lineKey: `${RID}_5282523907`, name: 'DOG 9667', designId: 'pc-x', version: 4, at: T0 };
    const seed = (mode, o = {}) => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: o.steps || ['takeoff', 'files', 'nest'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
      F.seed('Charm_Pool/' + CP, { poolId: CP, runId: R, orderId: RID, custom: true, state: 'nested', sheetId: SH, material: 'gold' });
      F.seed('Charm_Pool/' + LP, { poolId: LP, runId: R, orderId: RID, state: 'nested', sheetId: SH, material: 'gold' });
      F.seed('Charm_Nest_Sheets/' + SH, Object.assign({ id: SH, runId: R, metal: 'gold', status: 'complete', poolIds: [CP, LP], orders: [RID], charms: [{ id: 'c1', poolId: CP, custom: true, widthPt: 20, heightPt: 20, areaPt2: 400, cloud: { aiPath: 'charmnest/custom/designs/pc-x/v4-abc.pdf' } }, { id: 'c2', poolId: LP, widthPt: 20, heightPt: 20, areaPt2: 400 }], placements: [{ id: 'c1', cxPt: 10, cyPt: 10 }, { id: 'c2', cxPt: 40, cyPt: 40 }], layoutRev: 3, contentRev: 3, placedCount: 2, charmCount: 2 }, o.sheet || {}));
      if (o.two) {
        F.seed('Charm_Pool/' + CP2, { poolId: CP2, runId: R, orderId: RID, custom: true, state: 'nested', sheetId: SH2, material: 'gold' });
        F.seed('Charm_Nest_Sheets/' + SH2, Object.assign({ id: SH2, runId: R, metal: 'gold', status: 'complete', poolIds: [CP2], orders: [RID], charms: [{ id: 'd1', poolId: CP2 }], placements: [{ id: 'd1', cxPt: 5, cyPt: 5 }], layoutRev: 7, contentRev: 7 }, o.sheet2 || {}));
      }
    };
    const carries = r => r.http === 409 && r.cloudCarries === true && /The cloud carries every run now/.test(r.error || '');
    const donePatch = { state: 'abandoned', sheetId: null, setId: null, removedBy: 'Michelle', removedReason: 'completed in the Charm Editor', removedAt: T0, coveredBy: Object.assign({}, cover, { by: 'Michelle' }) };

    seed('on');
    let r = await P({ op: 'poolUpdate', poolIds: [CP], patch: donePatch, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S1 mode on: the page\'s own poolUpdate of the Done patch is still refused (nothing newly allowed), nothing written', carries(r) && F.doc('Charm_Pool/' + CP).state === 'nested', r);
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    let row = F.doc('Charm_Pool/' + CP), sh = F.doc('Charm_Nest_Sheets/' + SH);
    ok('S2 Done from a viewer: the piece is off its sheet (poolIds, charms, placements; the other piece keeps its pose), the sheet owes its files and is a revision on, the row is abandoned and covered by the saved piece, who, when',
      r.http === 200 && r.ok && row.state === 'abandoned' && row.sheetId === null && row.removedReason === 'completed in the Charm Editor' && row.coveredBy && row.coveredBy.name === 'DOG 9667' && row.coveredBy.by === 'Michelle' && row.coveredBy.at === T0
        && JSON.stringify(sh.poolIds) === JSON.stringify([LP]) && sh.charms.length === 1 && sh.charms[0].id === 'c2' && sh.placements.length === 1 && sh.placements[0].cxPt === 40 && sh.layoutRev === 4 && sh.contentRev === 4 && sh.filesOwed === T0 && F.doc('Charm_Pool/' + LP).state === 'nested',
      { r, row, sh });
    const tl = await P({ op: 'timelineGet', orderId: RID });
    ok('S3 ... the order\'s timeline says the piece was removed, completed in the Charm Editor, by the person', (tl.events || []).some(e => e.type === 'removed' && e.by === 'Michelle' && /completed in the Charm Editor/.test(e.text || '')), (tl.events || []).map(e => [e.type, e.by, e.text]));
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S4 ... pressed again: answered done already, nothing written again', r.http === 200 && r.ok && r.already && F.doc('Charm_Nest_Sheets/' + SH).layoutRev === 4, r);
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP], by: 'Michelle', dev: PAGE, cv: CV });
    row = F.doc('Charm_Pool/' + CP);
    sh = F.doc('Charm_Nest_Sheets/' + SH);
    ok('S5 Undo from a viewer (G7: nothing moved on its sheet since): the piece is BACK on its sheet at its place (poolIds, its charm, its placement, the revisions on, files owed), its row on that sheet as it was, its cover says undone, by whom; said in words',
      r.http === 200 && r.ok && row.state === 'nested' && row.sheetId === SH && !row.customDoneFrom && row.coveredBy.undoneAt > 0 && row.coveredBy.undoneBy === 'Michelle' && row.coveredBy.name === 'DOG 9667'
        && sh.poolIds.includes(CP) && sh.charms.some(c => c.id === 'c1' && c.poolId === CP && c.widthPt === 20) && sh.placements.some(p => p.id === 'c1' && p.cxPt === 10 && p.cyPt === 10) && sh.layoutRev === 5 && sh.contentRev === 5 && sh.filesOwed === T0 && sh.dirty === true
        && r.back.length === 1 && r.back[0].sheetId === SH && r.back[0].placed === true && !r.placing.length && !r.loose.length && /back on .+, where it was/.test(r.said.join(' ')), { r, row, sh });
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP], by: 'Michelle', dev: PAGE, cv: CV });
    ok('S6 ... Undo again: answered undone already, nothing written', r.http === 200 && r.ok && r.already, r);

    seed('on');
    r = await P({ op: 'customDone', orderId: RID, poolIds: [LP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S7 a piece that is not a custom piece: refused, nothing written', !r.ok && /not a custom piece/.test(r.error || '') && F.doc('Charm_Pool/' + LP).state === 'nested' && F.doc('Charm_Nest_Sheets/' + SH).layoutRev === 3, r);
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP], by: 'Michelle', dev: PAGE, cv: CV });
    ok('S8 an Undo of a piece not completed: refused, nothing written', !r.ok && /not completed in the Charm Editor/.test(r.error || '') && F.doc('Charm_Pool/' + CP).state === 'nested', r);
    seed('on', { sheet: { laserDoneAt: T0 - 60000 } });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S9 the piece\'s sheet is cut: refused with the reason, nothing written', !r.ok && /already cut/.test(r.error || '') && /Nothing was changed/.test(r.error || '') && F.doc('Charm_Pool/' + CP).state === 'nested' && F.doc('Charm_Nest_Sheets/' + SH).charms.length === 2, r);
    seed('on', { two: true, sheet2: { status: 'nesting' } });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP, CP2], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S10 two copies on two sheets, one of them nesting: refused whole, NOTHING half done (the free sheet and both rows untouched)', !r.ok && /try again/.test(r.error || '') && F.doc('Charm_Pool/' + CP).state === 'nested' && F.doc('Charm_Pool/' + CP2).state === 'nested' && F.doc('Charm_Nest_Sheets/' + SH).charms.length === 2 && F.doc('Charm_Nest_Sheets/' + SH).layoutRev === 3, r);
    seed('on', { two: true });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP, CP2], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S11 ... both free: both come off, in one press', r.ok && F.doc('Charm_Nest_Sheets/' + SH).charms.length === 1 && F.doc('Charm_Nest_Sheets/' + SH2).charms.length === 0 && F.doc('Charm_Pool/' + CP2).state === 'abandoned', r);
    seed('on');
    r = await P({ op: 'putSheet', sheet: { id: SH, runId: R, metal: 'gold', charms: [] }, dev: PAGE, cv: CV });
    ok('S12 mode on: a page\'s putSheet of a sheet\'s pieces stays refused', carries(r), r);
    r = await P({ op: 'poolUpdate', poolIds: [CP], patch: { state: 'ready', sheetId: null, setId: null, repooledAt: T0 }, dev: PAGE, cv: CV });
    ok('S13 mode on: the page\'s own Undo poolUpdate stays refused (it goes through customDone)', carries(r), r);
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE });
    ok('S14 an old page (no cv): the reload words', r.http === 409 && r.reload === true && F.doc('Charm_Pool/' + CP).state === 'nested', r);
    // G7: the place taken on the sheet meanwhile: the cloud places the piece as a run piece (its rows say so, the run is asked: placewaiting), and says so
    const takeSpot = () => { const cur = F.doc('Charm_Nest_Sheets/' + SH); F.seed('Charm_Nest_Sheets/' + SH, Object.assign({}, cur, { charms: cur.charms.concat([{ id: 'c9', poolId: `${RID}_5282523999_1`, widthPt: 20, heightPt: 20, areaPt2: 400 }]), placements: cur.placements.concat([{ id: 'c9', cxPt: 12, cyPt: 11, angle: 0 }]), poolIds: cur.poolIds.concat([`${RID}_5282523999_1`]), layoutRev: cur.layoutRev + 1 })); };
    seed('on', { steps: ['takeoff', 'files', 'nest', 'cmd:placewaiting'] });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S18 G7 Done keeps where the piece was on its row (the sheet, its charm, its placement)', r.ok && F.doc('Charm_Pool/' + CP).customDoneFrom && F.doc('Charm_Pool/' + CP).customDoneFrom.sheetId === SH && F.doc('Charm_Pool/' + CP).customDoneFrom.placements[0].cxPt === 10, F.doc('Charm_Pool/' + CP).customDoneFrom);
    takeSpot();
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP], by: 'Michelle', dev: PAGE, cv: CV });
    row = F.doc('Charm_Pool/' + CP); sh = F.doc('Charm_Nest_Sheets/' + SH);
    let cmds = Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {});
    ok('S19 G7 Undo with its place taken: not put over the other piece; the row is live, says the cloud places it (cloudPlace, the run, its size and design file from the charm it was), and the run is asked ONCE (placewaiting, that piece); said: why, and that the cloud places it',
      r.ok && !sh.poolIds.includes(CP) && !sh.charms.some(c => c.id === 'c1') && row.state === 'ready' && row.sheetId === null && row.cloudPlace === true && row.runId === R && row.widthPt === 20 && row.areaPt2 === 400 && /pc-x/.test(row.aiPath || '') && row.coveredBy.undoneAt > 0
        && cmds.length === 1 && cmds[0].type === 'placewaiting' && JSON.stringify(cmds[0].args.ids) === JSON.stringify([CP]) && cmds[0].by === 'Michelle' && cmds[0].state === 'waiting'
        && r.placing.length === 1 && r.placing[0].cmdId === cmds[0].id && /place there is taken/.test(r.said.join(' ')) && /the cloud places it on a sheet now/.test(r.said.join(' ')), { r, row, cmds });
    seed('on', { steps: ['takeoff', 'files', 'nest'] });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    takeSpot();
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP], by: 'Michelle', dev: PAGE, cv: CV });
    row = F.doc('Charm_Pool/' + CP); cmds = Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {});
    ok('S20 G7 ... the cloud\'s placement not on (no cmd:placewaiting lane): live again on no sheet, no request, and said why (never said placed)', r.ok && row.state === 'ready' && row.sheetId === null && !row.cloudPlace && !cmds.length && r.loose.length === 1 && /cmd:placewaiting/.test(r.said.join(' ')) && /send it to a sheet from its order/.test(r.said.join(' ')) && !/back on/.test(r.said.join(' ')), { r, row });
    seed('on', { steps: ['takeoff', 'files', 'nest', 'cmd:placewaiting'] });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    { const cur = F.doc('Charm_Nest_Sheets/' + SH); F.seed('Charm_Nest_Sheets/' + SH, Object.assign({}, cur, { laserDoneAt: T0 + 1000 })); }
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP], by: 'Michelle', dev: PAGE, cv: CV });
    row = F.doc('Charm_Pool/' + CP); sh = F.doc('Charm_Nest_Sheets/' + SH); cmds = Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {});
    ok('S21 G7 ... its sheet was cut meanwhile: never put back on it; the cloud places it (asked), said why', r.ok && !sh.poolIds.includes(CP) && row.cloudPlace === true && cmds.length === 1 && /already cut/.test(r.said.join(' ')), { r, row });
    seed('on', { steps: ['takeoff', 'files', 'nest', 'cmd:placewaiting'], two: true });
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP, CP2], cover, by: 'Michelle', dev: PAGE, cv: CV });
    takeSpot();
    r = await P({ op: 'customDone', undo: true, orderId: RID, poolIds: [CP, CP2], by: 'Michelle', dev: PAGE, cv: CV });
    cmds = Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {});
    ok('S22 G7 two copies on two sheets, one place taken: the free one goes back on its sheet at its place, the other is placed by the cloud; one answer says both',
      r.ok && F.doc('Charm_Nest_Sheets/' + SH2).poolIds.includes(CP2) && F.doc('Charm_Nest_Sheets/' + SH2).placements.some(p => p.id === 'd1' && p.cxPt === 5) && F.doc('Charm_Pool/' + CP2).sheetId === SH2 && F.doc('Charm_Pool/' + CP).cloudPlace === true
        && cmds.length === 1 && JSON.stringify(cmds[0].args.ids) === JSON.stringify([CP]) && r.back.length === 1 && r.placing.length === 1, { r, cmds });
    const C = require(path.join(ROOT, 'netlify/functions/_charmNestOpClass.js'));
    ok('S15 customDone is a person\'s press (PRESS), never on the cloud\'s list', C.classOf('customDone') === 'PRESS' && !C.isCloudOp('customDone'));
    seed('off');
    r = await P({ op: 'customDone', orderId: RID, poolIds: [CP], cover, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S16 mode off: customDone is refused in words and writes nothing (the page does it itself)', !r.ok && /does not carry the runs/.test(r.error || '') && F.doc('Charm_Pool/' + CP).state === 'nested', r);
    r = await P({ op: 'poolUpdate', poolIds: [CP], patch: donePatch, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S17 mode off: the page\'s own poolUpdate take-off works as before', r.http === 200 && F.doc('Charm_Pool/' + CP).state === 'abandoned' && F.doc('Charm_Pool/' + CP).coveredBy.name === 'DOG 9667', r);
  } finally { console.log = quiet; console.warn = qw; }
}

async function partP() {
  const g = globalThis, RID = '4225890898', PID = `${RID}_202611210001_1`;
  const calls = { api: [], update: [], takeOff: [], complete: [], reopen: [] };
  let viewerOn = true, answer = () => ({ ok: true, cover: { at: 1790000000000 } });
  const reset = () => { for (const k of Object.keys(calls)) calls[k] = []; g.B = { pool: { rows: new Map([[PID, { poolId: PID, custom: true, state: 'nested', sheetId: 'gf-1' }]]) } }; };
  const sheet = { sheetId: 'gf-1', name: 'GF Draft 1', metal: 'gold', status: 'complete', charms: [{ id: 'c1', poolId: PID }], placements: [{ id: 'c1' }] };
  g.__cdSheets = () => [sheet]; g.__cdToast = () => null;
  g.CNViewer = { on: () => viewerOn };
  g.CN = { api: async (fn, body) => { calls.api.push(body); return answer(body); } };
  g.Orders = { rows: () => [], takeOffGone: async (rows, opt) => { calls.takeOff.push(rows); await g.Pool.update(rows[0].poolIds, Object.assign({ state: 'abandoned' }, opt.patch)); return [{ sh: sheet, poolIds: rows[0].poolIds }]; } };
  g.Pool = { update: async (ids, patch) => { calls.update.push({ ids, patch }); return {}; } };
  g.CustomPrint = { complete: async it => { calls.complete.push(it.key); return true; }, reopen: it => { calls.reopen.push(it.key); } };
  g.CustomOrders = { rowsOf: () => [], rows: () => [], note: () => {} };
  delete require.cache[require.resolve(path.join(ROOT, 'charm-nest-custom-done.js'))];
  const CD = require(path.join(ROOT, 'charm-nest-custom-done.js'));
  const piece = { id: 'piece:x', kind: 'piece', rid: RID, key: `${RID}_202611210001`, name: 'Cat portrait', poolIds: [PID], sheets: [], why: '', metal: 'gold' };
  const line = { id: 'line:y', kind: 'line', rid: RID, key: `${RID}_5282523908`, name: 'Custom portrait', poolIds: [], sheets: [], why: '', it: { key: 'cu:y' }, row: {} };
  const cover = { poolId: `${RID}_5282523907_1`, lineKey: `${RID}_5282523907`, name: 'DOG 9667', version: 4 };
  reset();
  const r = await CD.complete({ rid: RID, items: [piece, line], cover, by: 'Michelle' });
  const a = calls.api[0] || {};
  ok('P1 viewer, Done: ONE customDone (the order, the piece, the cover, who), no Pool.update, no take-off here; then the Etsy custom line released; the token offered',
    r.ok && calls.api.length === 1 && a.op === 'customDone' && a.orderId === RID && JSON.stringify(a.poolIds) === JSON.stringify([PID]) && a.by === 'Michelle' && a.cover.name === 'DOG 9667' && !a.undo
      && !calls.update.length && !calls.takeOff.length && calls.complete.length === 1 && r.done.length === 2 && !!r.token && g.B.pool.rows.get(PID).state === 'abandoned', { r, calls });
  const u = await CD.undo(r.token);
  ok('P2 viewer, Undo: customDone undo for the piece, no Pool.update; the line reopened; said "live again"', u.ok && calls.api.length === 2 && calls.api[1].op === 'customDone' && calls.api[1].undo === true && !calls.update.length && calls.reopen.length === 1 && /live again/.test(u.said.join(' ')), { u, calls });
  reset(); answer = () => ({ error: 'GF Draft 1: it is being nested or saved right now: try again in a moment. Nothing was changed' });
  const r3 = await CD.complete({ rid: RID, items: [piece, line], cover, by: 'Michelle' });
  ok('P3 viewer, refused by the cloud: said with the reason, nothing released (the Etsy line untouched), no token', !r3.ok && /stays: GF Draft 1: it is being nested/.test(r3.why) && /Nothing was changed\.$/.test(r3.why) && !calls.complete.length && !r3.token, r3);
  answer = () => ({ ok: true }); reset();
  const r4 = await CD.complete({ rid: RID, items: [piece], cover, by: 'Michelle' }); answer = () => ({ error: 'the Custom piece is not completed in the Charm Editor any more' });
  const u4 = await CD.undo(r4.token), u4b = (answer = () => ({ ok: true }), await CD.undo(r4.token));
  ok('P4 viewer, Undo refused: said, and the Undo stays offered (pressed again, it goes through)', !u4.ok && /Nothing was changed/.test(u4.why) && u4b.ok, { u4, u4b });
  viewerOn = false; reset();
  const r5 = await CD.complete({ rid: RID, items: [piece], cover, by: 'Paul_K' });
  ok('P5 not a viewer (mode off): exactly the old path (take-off here, Pool.update), no customDone', r5.ok && !calls.api.length && calls.takeOff.length === 1 && calls.update.length === 1 && calls.update[0].patch.coveredBy.name === 'DOG 9667', calls);
  const html = require('fs').readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P6 charm-nest-1.html loads the changed file under a new token', /charm-nest-custom-done\.js\?v=20261121-item10-g78"/.test(html));
  // G7 on the page: the cloud's answer said per piece, the piece shown on its sheet at once, the cloud's placing followed until it answers
  viewerOn = true; g.__cdFollowGapMs = 20; const toasts = []; g.__cdToast = (t, k) => { toasts.push([String(t), k || '']); return null; };
  g.CharmNestRunFollow = { state: () => ({ runId: 'run-2026-11-21-item10' }) };
  const job = async ans => { reset(); answer = () => ({ ok: true }); const d = await CD.complete({ rid: RID, items: [piece], cover, by: 'Michelle' }); answer = body => (body.op === 'runCmdList' ? { ok: true, cmds: [{ id: 'cq1', state: 'done', text: '1 custom piece placed on GF Sheet 3 (the cloud nests it there)' }] } : ans); return CD.undo(d.token); };
  const u7 = await job({ ok: true, at: 1790000000500, undone: [PID], back: [{ ids: [PID], sheetId: 'gf-1', label: 'GF Draft 1', placed: true }], placing: [], loose: [] });
  const sent = calls.api.find(b => b.undo === true) || {};
  ok('P7 G7 viewer Undo, put back by the cloud: the run followed is named, said "back on GF Draft 1, where it was", the piece shown on that sheet at once',
    u7.ok && sent.runId === 'run-2026-11-21-item10' && /“Cat portrait” is back on GF Draft 1, where it was/.test(u7.said.join(' ')) && g.B.pool.rows.get(PID).sheetId === 'gf-1' && g.B.pool.rows.get(PID).state === 'nested', { u7, sent, row: g.B.pool.rows.get(PID) });
  toasts.length = 0;
  const u8 = await job({ ok: true, undone: [PID], back: [], placing: [{ ids: [PID], runId: 'run-2026-11-21-item10', cmdId: 'cq1', was: 'GF Draft 1: its place there is taken now' }], loose: [] });
  await new Promise(r => setTimeout(r, 300));
  ok('P8 G7 ... placed by the cloud: said why not where it was and that the cloud places it; the request followed and its answer said (the sheet it went on)',
    u8.ok && /cannot go back where it was \(GF Draft 1: its place there is taken now\): the cloud places it on a sheet now/.test(u8.said.join(' ')) && toasts.some(t => /“Cat portrait”: 1 custom piece placed on GF Sheet 3/.test(t[0])), { u8, toasts });
  const u9 = await job({ ok: true, undone: [PID], back: [], placing: [], loose: [{ id: PID, why: 'no run is open in the cloud' }] });
  ok('P9 G7 ... the cloud cannot place it: live again on no sheet, and why; never said placed', u9.ok && /live again on no sheet \(no run is open in the cloud\): send it to a sheet from its order/.test(u9.said.join(' ')) && !/back on|places it/.test(u9.said.join(' ')), u9);
}

(async () => {
  await partS().catch(e => ok('S run', false, String(e && e.stack || e)));
  await partP().catch(e => ok('P run', false, String(e && e.stack || e)));
  log0(failed ? `FAIL viewer-item10 (${failed})` : 'PASS viewer-item10');
  process.exit(failed ? 1 : 0);
})();
