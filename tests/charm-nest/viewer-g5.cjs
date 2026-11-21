// GAP G5 (Paul, 21 Nov 2026, the cloud carrying every run since 20 Nov 23:41): a staff computer's QR label / set files request (the sheet window's Make QR label, a Library set move,
// Approve's QR label step) is a labelsAsk the cloud's labels unit answers. It had two limits: (a) it answered only inside a pass with a run open (no open run: the ask waited forever),
// (b) it could not draw the label of a sheet whose run is not its set's run (a draft of one run moved in the Library into a set of another: the set's view held only its own run's sheets).
// Now: the labels pass item is "runless" (the driver runs it in a pass with no open run, and the cron dispatches a pass for a waiting ask), and the unit draws such a sheet from its own
// run's record, in its set (_charmNestLabels.js withOwnRuns / askRunId).
//   S  OFFLINE, the REAL library (_cloudFakes.cjs, mode "on", lane "labels"), the REAL driver (tick, charmNestDrive-background pass, _charmNestSteps.js pass items) and labels unit:
//      A  no run open: a Make QR label (complete) of a draft, a relabel of a sheet of run B moved into committed Set-1 of run A, and Set-1's files (setFiles): the cron dispatches, the
//         pass answers all three "done", the labels are made (the moved sheet's titled Set-1, in Set-1's list), and set.json lists the moved sheet; the page reads back "done"
//      B  a run open (the cloud carries it) with an OPEN set listing a sheet of another run: the labels pass item draws that sheet's label in its set, and its Make QR label ask is done
//      C  drive "off": nothing dispatched, the ask left as it is, nothing drawn (the page makes its own label as before)
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> node tests/charm-nest/viewer-g5.cjs
'use strict';
const path = require('path');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle7';
const RA = 'run-2026-11-18-g5a', RB = 'run-2026-11-19-g5b', RC = 'run-2026-11-21-g5c', DA = '2026-11-18', DB = '2026-11-19', DC = '2026-11-21';
const O1 = '4261000001', O2 = '4261000002', O3 = '4261000003', O4 = '4261000004', O5 = '4261000005';
const pid = (rid, n) => `${rid}_${rid}${n}_1`;
/** a sheet's record: its placed pieces, files and check */
function sheetRec(id, o) {
  const name = o.fileBase, dir = o.dir || `charmnest/sheets/${o.day}/${name}`;
  const charms = o.orders.map((rid, i) => ({ id: `${id}-c${i}`, poolId: pid(rid, i + 1), order: rid, name: `${rid} · charm`, sku: 'LEAF_CHARM', areaPt2: 300, widthPt: 28.35, heightPt: 28.35 }));
  return Object.assign({ id, metal: o.metal || 'gold', metalLabel: 'GF', day: o.day, runId: o.runId, page: 1, draft: !o.setId, setId: o.setId || null, setSeq: o.setSeq || null, sheetIndex: o.sheetIndex || null,
    fileBase: name, folder: name, charms, placements: charms.map((c, i) => ({ n: i + 1, id: c.id, cxPt: 20 + i * 32, cyPt: 20, angle: 0, scale: 0.971, wPt: 28.35, hPt: 28.35 })), poolIds: charms.map(c => c.poolId),
    orders: o.orders.slice(), placedCount: charms.length, charmCount: charms.length, density: 0.4, releaseFull: true, status: 'complete', endedBy: 'complete', verification: { ok: true }, persistedDone: true,
    stock: { wIn: 182 / 72, hIn: 62 / 72, wPt: 182, hPt: 62 }, outputs: { ai: { path: `${dir}/${name}.ai`, url: 'https://storage.test.invalid/' + id + '.ai' }, pdf: { path: `${dir}/${name}.pdf`, url: 'https://storage.test.invalid/' + id + '.pdf' } },
    backPool: [], engraving: {}, listings: [], names: '', archived: false }, o.extra || {});
}
const orderLines = (rid, sheetId, sheet) => ({ held: null, lines: [{ transactionId: rid + '1', sku: 'LEAF_CHARM', copies: [{ copy: 1, sheetId, sheet, poolId: pid(rid, 1) }] }] });

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 19, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn, qe = console.error; console.log = () => {}; console.warn = () => {}; console.error = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), QrLabel = F.load('_charmNestQrLabel.js'), O = require(path.join(ROOT, 'charm-nest-orders.js'));
    // (the QR drawing, @resvg/resvg-js, may not be installed here: a fixed 1x1 PNG then stands for each drawn label; every other part is the real unit)
    let how = 'resvg';
    try { QrLabel.renderPng({ payload: 'x', label: 'x', notes: [] }); } catch (_) { how = 'stand-in'; const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==', 'base64'); QrLabel.renderPng = () => ({ png, ecc: 'M' }); }
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const ask = b => P(Object.assign({ op: 'labelsAsk', by: 'Michelle', dev: PAGE, cv: CV }, b));
    const askDoc = key => F.doc('Charm_Nest_Label_Asks/' + key) || {};
    const rec = id => F.doc('Charm_Nest_Sheets/' + id) || {};
    const setDoc = id => F.doc('Charm_Nest_Sets/' + id) || {};
    const fresh = () => { F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: Date.now() - 1000 }); F.seed('Charm_World/summary', { at: Date.now(), live: 0, mirrorBefore: Date.now() - 500 }); };
    const base = (mode, steps) => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: steps, flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + RA, { runId: RA, status: 'complete', step: 'complete', day: DA, releasePolicy: 2, setIds: ['set-g5-a'], errors: [], orders: [O1, O3], lines: {} });
      F.seed('Charm_Nest_Runs/' + RB, { runId: RB, status: 'complete', step: 'complete', day: DB, releasePolicy: 2, errors: [], orders: [O2, O4], lines: {} });
      F.seed('Charm_World/summary', { at: T0 - 60000, live: 0, mirrorBefore: T0 - 60000 });
      F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: T0 - 60000 });
    };
    /** the cron and the passes it starts, until every ask is answered (or n turns) */
    const drive = async n => { const ticks = []; let passes = 0;
      for (let i = 0; i < n && (i === 0 || F.list('Charm_Nest_Label_Asks').some(x => x.state !== 'done' && x.state !== 'refused')); i++) { const t = await Drive.tick({ db: F.db, kick: Drive.kick }); ticks.push({ why: t.why, dispatched: t.dispatched, off: t.off }); passes += await F.drain(4); F.clock.advance(61000); fresh(); }
      return { ticks, passes }; };

    /* ═══ A · no run open ═══ */
    base('on', ['labels', 'files', 'takeoff', 'engrave']);
    const SA = 'gf-g5-a1', MOVED = 'gf-g5-moved', OWN = 'gf-g5-own', SET1 = 'set-g5-a', folder1 = `charmnest/sets/${DA}/Set-1`;
    const sa = sheetRec(SA, { day: DA, runId: RA, setId: SET1, setSeq: 1, sheetIndex: 1, fileBase: 'GF_Nov.18.26_Set-1_Sheet-1', dir: `${folder1}/GF_Nov.18.26_Set-1_Sheet-1`, orders: [O1] });
    const saPayload = O.encodeOrderList([O1], O.CARD_TO_METAL.gold || 'gold'), saPath = `${folder1}/GF_Nov.18.26_Set-1_Sheet-1/GF_Nov.18.26_Set-1_Sheet-1_label.png`;
    F.bucket.file(saPath).save(QrLabel.renderPng({ payload: saPayload, label: 'GF · Set-1 · Sheet 1 · 1 order', notes: [] }).png, { contentType: 'image/png' });
    const saLabel = { path: saPath, url: 'https://storage.test.invalid/' + encodeURIComponent(saPath), sheet: sa.fileBase, part: 1, parts: 1, orders: [O1], payload: saPayload, ecc: 'M', label: 'GF · Set-1 · Sheet 1 · 1 order' };
    sa.label = { files: [saLabel], orders: [O1], by: 'cloud', at: T0 - 86400e3 };
    // (the moved sheet: a draft of run B, put into committed Set-1 of run A in the Library: its record names the set, it keeps its run)
    const moved = sheetRec(MOVED, { day: DB, runId: RB, setId: SET1, setSeq: 1, sheetIndex: 2, fileBase: 'GF_Nov.19.26_Draft-3', orders: [O2] });
    F.seed('Charm_Nest_Sheets/' + SA, sa); F.seed('Charm_Nest_Sheets/' + MOVED, moved);
    F.seed('Charm_Nest_Sheets/' + OWN, sheetRec(OWN, { day: DA, runId: RA, fileBase: 'GF_Nov.18.26_Draft-5', orders: [O3, O5] }));
    F.seed('Charm_Nest_Sets/' + SET1, { setId: SET1, runId: RA, day: DA, seq: 1, name: 'Set-1', folder: folder1, group: 'dispatch', status: 'complete', committedAt: T0 - 86400e3, committed: [O1], sheetIds: [SA, MOVED], materials: ['gold'],
      orders: { [O1]: orderLines(O1, SA, sa.fileBase), [O2]: orderLines(O2, MOVED, moved.fileBase) }, labelFiles: [Object.assign({ sheetId: SA, metal: 'gold' }, saLabel)], labels: null, setEdit: { at: T0 - 5000, by: 'Michelle' }, sheetNos: { gold: 2 } });   // (as the Library's move leaves a committed set: setEdit, its files owed)
    ok('A0 no run is open (Charm_Nest_Open/run absent; both runs complete)', !F.doc('Charm_Nest_Open/run'));
    // (in the order a person's presses make them: the Library move asks the sheet's label, then the set's files)
    const a1 = await ask({ kind: 'complete', sheetId: OWN }); F.clock.advance(1000);
    const a2 = await ask({ kind: 'relabel', sheetId: MOVED }); F.clock.advance(1000);
    const a3 = await ask({ kind: 'setFiles', setId: SET1 });
    ok('A1 the three presses are taken as asks (Make QR label, the moved sheet\'s QR label, Set-1\'s files), each "asked"', [a1, a2, a3].every(r => r.http === 200 && r.ok) && askDoc('sheet-' + OWN).state === 'asked' && askDoc('sheet-' + MOVED).state === 'asked' && askDoc('set-' + SET1).state === 'asked', { a1, a2, a3 });
    F.clock.advance(30000); fresh();
    const dA = await drive(6);
    ok('A2 with no run open, the cron sees them ("a label ask waits") and passes run (the press\'s own kick, or the cron\'s)', dA.ticks[0] && (dA.ticks[0].why || []).includes('a label ask waits') && dA.passes > 0, dA);
    const asksA = F.list('Charm_Nest_Label_Asks').map(x => ({ key: x.key, kind: x.kind, state: x.state, why: x.why }));
    ok('A3 the pass with no open run answers all three: done', asksA.length === 3 && asksA.every(x => x.state === 'done'), { asksA, ticks: dA.ticks });
    const lo = rec(OWN).label || {}, lo0 = (lo.files || [])[0] || {};
    ok(`A4 Make QR label of a draft (no run open): its own label beside its files, its code the orders on it (QR: ${how})`, lo.own === true && (lo.orders || []).join() === [O3, O5].join() && lo0.payload === O.encodeOrderList([O3, O5], O.CARD_TO_METAL.gold || 'gold') && F.blobs.has(lo0.path) && /GF_Nov\.18\.26_Draft-5_label\.png$/.test(lo0.path), lo);
    const lm = rec(MOVED).label || {}, lm0 = (lm.files || [])[0] || {}, s1 = setDoc(SET1);
    ok('A5 the sheet of run B in Set-1 of run A: its QR label drawn IN its set (title Set-1 Sheet 2, its order, by the cloud), its record says so', lm.by === 'cloud' && (lm.files || []).length === 1 && /Set-1/.test(lm0.label || '') && /Sheet 2/.test(lm0.label || '') && lm0.payload === O.encodeOrderList([O2], O.CARD_TO_METAL.gold || 'gold') && F.blobs.has(lm0.path) && rec(MOVED).runId === RB, lm);
    ok('A6 Set-1\'s label list carries both sheets (the moved one\'s new label, the first one\'s kept)', (s1.labelFiles || []).some(f => f.sheetId === MOVED && f.path === lm0.path) && (s1.labelFiles || []).some(f => f.sheetId === SA && f.path === saPath), s1.labelFiles);
    const json = (() => { try { return JSON.parse(F.blobs.get(`${folder1}/set.json`).buf.toString('utf8')); } catch (_) { return null; } })();
    ok('A7 Set-1\'s files made again WITH the moved sheet: labels PDF and manifest in the bucket, set.json lists the moved sheet and its label', !!(s1.labels && s1.labels.pdf && F.blobs.has(s1.labels.pdf.path) && s1.labels.manifest && F.blobs.has(s1.labels.manifest.path)) && !!json && json.sheets.some(s => s.sheetId === MOVED) && json.labels.some(l => l.path === lm0.path) && !!s1.committedAt,
      { labels: s1.labels, json: json && { sheets: json.sheets.map(s => s.sheetId), labels: json.labels.map(l => l.path) } });
    const got = await P({ op: 'labelsAskGet', sheetId: MOVED, setId: SET1, dev: PAGE, cv: CV });
    ok('A8 what the page reads back (labelsAskGet, the item 5/6 follow) says done for the sheet and the set', got.http === 200 && (got.asks || []).length === 2 && got.asks.every(a => a.state === 'done'), got);
    ok('A9 still no run open: the cloud answered without opening one', !F.doc('Charm_Nest_Open/run') && !F.list('Charm_Nest_Runs').some(r => r.runId && ![RA, RB].includes(r.runId)), F.list('Charm_Nest_Runs').map(r => r.runId));

    // (an ask no press kicked, the lease idle: the cron alone dispatches a pass for it, with no run open)
    F.seed('Charm_Nest_Label_Asks/sheet-' + OWN, Object.assign({}, askDoc('sheet-' + OWN), { at: Date.now(), seq: 2, state: 'asked', why: null, doneAt: null }));
    F.net.kicks.length = 0;
    const tA = await Drive.tick({ db: F.db, kick: Drive.kick }), nA = await F.drain(4);
    ok('A10 an ask nobody kicked, no run open: the cron dispatches a pass ("a label ask waits") and it is answered done', tA.dispatched === true && (tA.why || []).includes('a label ask waits') && nA > 0 && askDoc('sheet-' + OWN).state === 'done' && askDoc('sheet-' + OWN).seq === 2, { tA, nA, ask: askDoc('sheet-' + OWN) });

    /* ═══ B · a run open, its open set listing a sheet of another run ═══ */
    base('on', ['labels']);
    const M2 = 'gf-g5-moved2', SET4 = 'set-g5-c', folder4 = `charmnest/sets/${DC}/Set-4`;
    F.seed('Charm_Nest_Open/run', { runId: RC, at: T0 - 600e3 });
    F.seed('Charm_Nest_Runs/' + RC, { runId: RC, status: 'running', step: 'nest', mode: 'manual', day: DC, releasePolicy: 2, setIds: [SET4], errors: [], orders: [], lines: {} });
    F.seed('Charm_Nest_Run_Carrier/' + RC, { runId: RC, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    F.seed('Charm_Nest_Sheets/' + M2, sheetRec(M2, { day: DB, runId: RB, setId: SET4, setSeq: 4, sheetIndex: 1, fileBase: 'GF_Nov.19.26_Draft-4', orders: [O4] }));
    F.seed('Charm_Nest_Sets/' + SET4, { setId: SET4, runId: RC, day: DC, seq: 4, name: 'Set-4', folder: folder4, group: 'dispatch', status: 'nesting', sheetIds: [M2], materials: ['gold'], orders: { [O4]: orderLines(O4, M2, 'GF_Nov.19.26_Draft-4') }, labelFiles: [], labels: null, sheetNos: { gold: 1 } });
    const b1 = await ask({ kind: 'complete', sheetId: M2 });
    const dB = await drive(4);
    const l2 = rec(M2).label || {}, l20 = (l2.files || [])[0] || {}, s4 = setDoc(SET4);
    ok('B1 a run open (the cloud carries it), its open Set-4 listing a sheet of run B: that sheet\'s QR label drawn in Set-4 from its own run\'s record, the set\'s list says so',
      b1.http === 200 && l2.by === 'cloud' && /Set-4/.test(l20.label || '') && l20.payload === O.encodeOrderList([O4], O.CARD_TO_METAL.gold || 'gold') && F.blobs.has(l20.path) && (s4.labelFiles || []).some(f => f.sheetId === M2 && f.path === l20.path), { b1, l2, labelFiles: s4.labelFiles, ticks: dB.ticks });
    ok('B2 ... and its Make QR label ask is answered done (never refused "in no set of the run")', askDoc('sheet-' + M2).state === 'done', askDoc('sheet-' + M2));

    /* ═══ C · drive "off": unchanged ═══ */
    base('off', []);
    F.seed('Charm_Nest_Sheets/' + OWN, sheetRec(OWN, { day: DA, runId: RA, fileBase: 'GF_Nov.18.26_Draft-5', orders: [O3, O5] }));
    F.seed('Charm_Nest_Label_Asks/sheet-' + OWN, { key: 'sheet-' + OWN, kind: 'complete', sheetId: OWN, setId: null, runId: RA, by: 'Michelle', dev: PAGE, at: T0 - 1000, seq: 1, state: 'asked', why: null, doneAt: null });
    const t = await Drive.tick({ db: F.db, kick: Drive.kick }), n = await F.drain(4);
    ok('C1 drive off: the cron reads its switch and nothing else (no pass, the ask left as it is, no label drawn)', t.off === true && !t.dispatched && n === 0 && askDoc('sheet-' + OWN).state === 'asked' && !rec(OWN).label, { t, n });
    const r = await P({ op: 'putSheet', sheet: { id: OWN, label: { files: [{ path: 'x.png' }], own: true } }, dev: PAGE, cv: CV });
    ok('C2 drive off: the page\'s own label is saved as before (nothing refused)', r.http === 200 && !!rec(OWN).label, r);
    ok('C3 Etsy never called, nothing else fetched', F.etsy.calls === 0 && F.net.tripwire.length === 0, { etsy: F.etsy.calls, trip: F.net.tripwire });
  } finally { console.log = quiet; console.warn = qw; console.error = qe; }
}

partS().then(() => { log0(failed ? `FAIL viewer-g5 (${failed})` : 'PASS viewer-g5'); process.exit(failed ? 1 : 0); })
  .catch(e => { log0('FAIL viewer-g5: ' + (e && e.stack || e)); process.exit(1); });
