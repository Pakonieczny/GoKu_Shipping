/* NODUPDROP (Paul, 28 Nov 2026: "The user added an .ai file to a custom order that was already nested. This duplicated the pieces of this order"; order 4249694385, a necklace of 12 charms cut
   from its own design, read "THIS ORDER · 25 PIECES"). A design dropped for an order line made pieces BESIDE the pieces the line already had: a send puts every design of its card on the line
   (charm-nest-bridge.js planLines), copy n being pool id <line>_<n>, so the first design again (copies 1-12, kept where they were) and the new one after it (13-25); nothing compared a send with
   what the line already held. Offline, over _cloudFakes.cjs (the real library handler; the real cloud: its driver pass and its customsend request, the page's charm-nest-pdf.js tracing a real
   .ai from the fake bucket; no network, no Firebase, no Etsy):
     R  the one rule (charm-nest-pool-pieces.js dropRule): pieces of a design dropped before, one on a sheet: refused in words; none on a sheet: replaced; a matched charm's pieces: as before
     A  a first drop makes N pieces: the send recorded (customSheetPut), the cloud makes and places them (customsend): N custom pool rows <line>_1..N, on a sheet
     B  a second drop once they are on a sheet: refused in words that say what to do (its sheet, "Take off the sheet…"), and nothing is written (no record, no claim, no pool row, no sheet);
        the old design with the new one: refused; from another card: refused; asked of the cloud again: still N
     D  the duplication itself: a line whose custom pieces are on a sheet with no send recorded for them (made while a page's cloud was offline) and a send of that design again with a new one
        (before NODUPDROP it was recorded, N + M pieces for the one line, what the order window counts): refused, nothing written; a record of exactly those pieces is still taken
     C  a second drop BEFORE the pieces are on a sheet (they wait): the new send names the send it replaces and takes its place: still N pieces, the same pool ids, the new design; with fewer
        pieces the copy past its count stays superseded, never placed; a send that names none (a retry) is refused as before
     P  the page (charm-nest-bridge.js): the drop and the send ask the rule (its block run as it stands, over stubs), a replacement takes the earlier design off the card and names the send
        it replaces, a refusal from the server is not retried for ever; the changed files load under new tokens
   node tests/charm-nest/rt1128-nodupdrop.cjs */
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 28, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); if (!process.env.ND_LOUD) { console.log = () => {}; console.warn = () => {}; console.error = () => {}; }
let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const part = async (name, fn) => { try { await fn(); } catch (e) { ok(name + ' ran to its end', false, String((e && e.stack) || e).slice(0, 900)); } };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js');
const PP = require(path.join(ROOT, 'charm-nest-pool-pieces.js'));
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const CV = 'cloud14-20261120', DEV = 'd0staff0003', R = 'run-2026-11-28-nd', RID = '4249600001', SA = 'gold-a-nd', OTHER = '4249600009_42496000091_1';
const KEY = `${RID}_42496000011`, KEYD = `${RID}_42496000012`, KEYC = `${RID}_42496000013`, KEYF = `${RID}_42496000014`;
const PDF1 = `charmnest/custom/${RID}/aaa111aaa111-DONUT1.pdf`, PDF2 = `charmnest/custom/${RID}/bbb222bbb222-DONUT2.pdf`;
const N = 3;   // (the first design's pieces: a one-charm file cut 3 times; the order's 12 charms stand in as 3)
const LINES = [KEY, KEYD, KEYC, KEYF];

async function seed() {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'off', runMode: 'manual', cloudSteps: [], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-28', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [] });
  const line = k => ({ state: 'pulled', poolIds: [], orderId: RID, material: 'gold', arrivedAt: T0 - 86400e3 });
  const put = await P({ op: 'runPut', merge: true, run: { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-28', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [], processingComplete: true,
    lines: Object.assign(Object.fromEntries(LINES.map(k => [k, line(k)])), { '4249600009_42496000091': { state: 'pooled', poolIds: [OTHER], orderId: '4249600009', material: 'gold', arrivedAt: T0 - 86400e3 } }) } });
  if (put.http !== 200) throw new Error('run not seeded: ' + JSON.stringify(put));
  F.seed('Charm_Nest_Sheets/' + SA, { id: SA, runId: R, metal: 'gold', page: 1, draft: true, status: 'complete', layoutRev: 3, contentRev: 3, fileBase: 'GF Draft 1',
    charms: [{ id: 'oth1', poolId: OTHER, order: '4249600009', name: 'oth1', sku: 'OLDSKU', metal: 'gold', areaPt2: 300, widthPt: 20, heightPt: 20, lineKey: '4249600009_42496000091' }], poolIds: [OTHER], placements: [{ id: 'oth1', cxPt: 30, cyPt: 30, angle: 0 }], orders: ['4249600009'] });
  F.seed('Charm_Pool/' + OTHER, { poolId: OTHER, runId: R, orderId: '4249600009', lineKey: '4249600009_42496000091', sku: 'OLDSKU', material: 'gold', state: 'nested', sheetId: SA, copy: 1, quantity: 1 });
  for (const k of LINES) F.seed('Charm_World_Lines/' + k, { at: T0 - 60e3, data: JSON.stringify({ order: { receiptId: RID, createTs: T0 - 86400e3 }, line: { transactionId: k.split('_')[1], sku: 'DONUT', quantity: 1 }, spec: { designSku: 'DONUT', material: 'gold', quantity: 1, form: 'charm' }, problems: [] }) });
  const ai = fs.readFileSync(path.join(__dirname, 'fixtures/COMET_9427.ai'));
  for (const p of [PDF1, PDF2]) await F.bucket.file(p).save(ai, { contentType: 'application/pdf' });
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['nest', 'pool', 'cmd:customsend'], flippedAt: T0 - 3600e3 }); lib._driveForget();
}
async function pass() {
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
const cmdOf = id => Object.values((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {}).find(c => c.id === id) || {};
async function untilAnswered(id, k = 6) { let c = {}; for (let i = 0; i < k; i++) { await pass(); c = cmdOf(id); if (c.state === 'done' || c.state === 'failed') break; F.clock.advance(60e3); } return c; }
const file = (id, p, hash, qty) => ({ id, name: path.basename(p).replace(/^\w+-/, '').replace(/\.pdf$/, '.ai'), kind: 'ai', size: 1000, hash, cloud: { path: p, url: '' }, metal: 'gold', qty, pieces: 1, wMm: 20, hMm: 20, maxPt: 60, minPt: 40, maxAreaPt2: 2000, state: 'ready' });
const F1 = q => file('f1', PDF1, 'aaa111aaa111', q), F2 = q => file('f2', PDF2, 'bbb222bbb222', q);
const copies = (f, q) => Array.from({ length: q }, () => ({ f, i: 0 }));
let clockAt = T0 - 9000;
function record(ck, files, lines, o) { const at = (o && o.at) || (clockAt += 1000); const sent = Object.assign({ id: `custom-sheet:${ck}:${at}`, at, by: 'Ana_M', lines }, o && o.replaces ? { replaces: o.replaces } : {}); return { ck, rid: RID, at, phase: (o && o.phase) || 'sent', files, sent }; }
const sendPut = rec => P({ op: 'customSheetPut', record: rec, dev: DEV, cv: CV });
const customsend = ck => P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Ana_M', cmd: { type: 'customsend', ck, ids: [], label: `Place the designs sent for order ${RID}` }, cv: CV });
const rowsOf = k => F.list('Charm_Pool').filter(r => r._id.startsWith(k + '_')).map(r => Object.assign({}, r, { poolId: r._id }));
const liveOf = k => rowsOf(k).filter(r => !['abandoned', 'superseded'].includes(r.state));
const listed = id => F.list('Charm_Nest_Sheets').filter(s => !s.archived && (s.poolIds || []).includes(id)).map(s => s.fileBase || s._id);
const watched = p => /^(Charm_Custom_Sheet|Charm_Pool|Charm_Nest_Sheets)\//.test(p);
/** Custom pieces of a line put on SA directly (made with no send recorded, or a send's pieces), copy 1..q, design path p. */
function seedOnSheet(k, q, p) {
  const sa = F.doc('Charm_Nest_Sheets/' + SA), ids = Array.from({ length: q }, (_, i) => `${k}_${i + 1}`);
  for (const [i, id] of ids.entries()) F.seed('Charm_Pool/' + id, { poolId: id, runId: R, orderId: RID, transactionId: k.split('_')[1], lineKey: k, sku: 'DONUT', material: 'gold', state: 'nested', sheetId: SA, sheetName: 'GF Draft 1', copy: i + 1, quantity: q, custom: true, customIndex: 0, aiPath: p, customFile: 'DONUT1.ai' });
  F.seed('Charm_Nest_Sheets/' + SA, Object.assign({}, sa, { poolIds: sa.poolIds.concat(ids), charms: sa.charms.concat(ids.map((id, i) => ({ id: 'd' + id, poolId: id, order: RID, name: `${RID} · Custom · DONUT1`, sku: 'DONUT', metal: 'gold', areaPt2: 300, widthPt: 20, heightPt: 20, lineKey: k, custom: true }))), placements: sa.placements.concat(ids.map((id, i) => ({ id: 'd' + id, cxPt: 70 + 25 * i, cyPt: 80, angle: 0 }))) }));
  return ids;
}

(async () => {
  await part('R', async () => {
    ok('R0 charm-nest-pool-pieces.js has the one rule (dropRule)', typeof PP.dropRule === 'function');
    if (typeof PP.dropRule !== 'function') return;
    const pc = (i, o) => Object.assign({ poolId: `${KEY}_${i}`, custom: true, sheet: '', cut: false }, o || {});
    const a = PP.dropRule({ rid: RID, decided: true, pieces: [pc(1, { sheet: 'GF Sheet 2' }), pc(2)] });
    ok('R1 pieces of a design dropped before, one on a sheet: refused, in words that name the sheet and say what to do', a.ok === false && /GF Sheet 2/.test(a.words) && /never added beside/.test(a.words) && /Take off the sheet/.test(a.words) && /Nothing was changed/.test(a.words), a);
    const b = PP.dropRule({ rid: RID, decided: true, pieces: [pc(1), pc(2)] });
    ok('R2 none on a sheet: the new design replaces them, under the same pool ids', b.ok === true && b.replace === true && JSON.stringify(b.ids) === JSON.stringify([`${KEY}_1`, `${KEY}_2`]), b);
    const c = PP.dropRule({ rid: RID, decided: false, pieces: [pc(1, { custom: false, sheet: 'GF Sheet 1' })] });
    ok('R3 a matched charm\'s pieces (none custom, no send recorded), even on a sheet: replaced in place as before (REVDROP)', c.ok === true && c.replace === false, c);
    const d = PP.dropRule({ rid: RID, decided: true, pieces: [pc(1, { sheet: 'GF Sheet 3', cut: true })] });
    ok('R4 on a sheet already cut: refused, and the words say it can no longer change', d.ok === false && /already cut/.test(d.words), d);
    const e = PP.dropRule({ rid: RID, decided: true, pieces: [] });
    ok('R5 a recorded send whose pieces were all taken off: the new design replaces it', e.ok === true && e.replace === true && !e.ids.length, e);
  });

  await seed();
  const CK = 'custom:' + KEY, R1 = record(CK, [F1(N)], { [KEY]: copies('f1', N) });
  await part('A', async () => {
    const s = await sendPut(R1), c = await customsend(CK);
    ok('A1 the first drop\'s send is recorded and asked of the cloud', s.http === 200 && s.ok && c.http === 200 && !!c.id, { s, c });
    const ans = await untilAnswered(c.id), live = liveOf(KEY);
    ok('A2 the cloud made N custom pieces of the first design, <line>_1..N', ans.state === 'done' && live.length === N && live.every(r => r.custom === true && r.aiPath === PDF1) && JSON.stringify(live.map(r => r.poolId).sort()) === JSON.stringify(Array.from({ length: N }, (_, i) => `${KEY}_${i + 1}`)), { ans: ans.text, live: live.map(r => [r.poolId, r.state, r.aiPath]) });
    ok('A3 and they are on a sheet ("already nested")', live.every(r => listed(r.poolId).length === 1), live.map(r => [r.poolId, listed(r.poolId)]));
  });

  await part('B', async () => {
    const mark = F.count(), names = [...new Set(liveOf(KEY).flatMap(r => listed(r.poolId)))];
    const r2 = await sendPut(record(CK, [F2(N + 1)], { [KEY]: copies('f2', N + 1) }, { replaces: R1.sent.id }));
    ok('B1 a second drop once the pieces are on a sheet is refused (409), in words that name the sheet and say what to do', r2.http === 409 && r2.refused === 'nodupdrop' && names.every(x => r2.error.includes(x)) && /Take off the sheet/.test(r2.error) && /never added beside/.test(r2.error), r2);
    const r3 = await sendPut(record(CK, [F2(N + 1)], { [KEY]: copies('f2', N + 1) }));
    ok('B2 the same from an older page (it names no send it replaces): refused in the same words', r3.http === 409 && r3.refused === 'nodupdrop' && /Take off the sheet/.test(r3.error), r3);
    const r4 = await sendPut(record(CK, [F1(N), F2(N + 1)], { [KEY]: copies('f1', N).concat(copies('f2', N + 1)) }, { replaces: R1.sent.id }));
    ok('B3 the first design again with the new one (the card that kept both): refused', r4.http === 409 && r4.refused === 'nodupdrop', r4);
    const r5 = await sendPut(record('custom:' + RID + ':OTHERCARD', [F2(N)], { [KEY]: copies('f2', N) }));
    ok('B4 from another card of the order: refused, in words', r5.http === 409 && /another card/.test(r5.error || ''), r5);
    const wrote = F.since(mark).writePaths.filter(watched);
    ok('B5 a refused drop writes nothing: no record, no claim, no pool row, no sheet', !wrote.length, wrote);
    const rec = (((await P({ op: 'customSheetGet', keys: [CK] })).records) || {})[CK];
    ok('B6 the recorded send is the first one, unchanged', rec && rec.sent.id === R1.sent.id && rec.sent.lines[KEY].length === N, rec && rec.sent);
    const c = await customsend(CK), ans = await untilAnswered(c.id), live = liveOf(KEY);
    ok('B7 asked of the cloud again: still N pieces, the first design, where they were', ans.state === 'done' && live.length === N && live.every(r => r.aiPath === PDF1 && listed(r.poolId).length === 1), { ans: ans.text, live: live.map(r => [r.poolId, r.aiPath]) });
  });

  await part('D', async () => {
    seedOnSheet(KEYD, N, PDF1);
    const mark = F.count(), CKD = 'custom:' + KEYD;
    const dup = await sendPut(record(CKD, [F1(N), F2(N + 1)], { [KEYD]: copies('f1', N).concat(copies('f2', N + 1)) }));
    const rec = (((await P({ op: 'customSheetGet', keys: [CKD] })).records) || {})[CKD];
    ok('D1 a line whose custom pieces are on a sheet with no send recorded, and a send of that design again with a new one: refused (before NODUPDROP it was recorded: N + M pieces for one line)',
      dup.http === 409 && dup.refused === 'nodupdrop' && !rec, { dup, recorded: rec ? rec.sent.lines[KEYD].length : 0 });
    ok('D2 and nothing was written', !F.since(mark).writePaths.filter(watched).length, F.since(mark).writePaths.filter(watched));
    const late = await sendPut(record(CKD, [F1(N)], { [KEYD]: copies('f1', N) }));
    ok('D3 a record of exactly the pieces already made (the same file and charm, copy for copy) is still taken', late.http === 200 && late.ok && liveOf(KEYD).length === N, late);
  });

  await part('C', async () => {
    const CKC = 'custom:' + KEYC, R1c = record(CKC, [F1(N)], { [KEYC]: copies('f1', N) });
    ok('C0 the first drop of another line is recorded', (await sendPut(R1c)).http === 200);
    // its pieces made and waiting (on no sheet yet)
    for (let i = 1; i <= N; i++) F.seed('Charm_Pool/' + `${KEYC}_${i}`, { poolId: `${KEYC}_${i}`, runId: R, orderId: RID, transactionId: KEYC.split('_')[1], lineKey: KEYC, sku: 'DONUT', material: 'gold', state: 'ready', sheetId: null, copy: i, quantity: N, custom: true, customIndex: 0, aiPath: PDF1, customFile: 'DONUT1.ai' });
    const retry = await sendPut(record(CKC, [F2(N)], { [KEYC]: copies('f2', N) }));
    ok('C1 a changed send that names no send it replaces (a retry) is refused as before: a recorded send never changes', retry.http === 409 && /different decision/.test(retry.error || ''), retry);
    const R2c = record(CKC, [F2(N)], { [KEYC]: copies('f2', N) }, { replaces: R1c.sent.id }), r2 = await sendPut(R2c);
    const rec = (((await P({ op: 'customSheetGet', keys: [CKC] })).records) || {})[CKC];
    ok('C2 a second drop before the pieces are on a sheet: the new send takes the first one\'s place (the record and its line are the new send\'s, the first kept in its history)',
      r2.http === 200 && r2.ok && rec && rec.sent.id === R2c.sent.id && (rec.history || []).some(h => h.phase === 'replaced' && h.id === R1c.sent.id), { r2, rec: rec && { id: rec.sent.id, history: rec.history } });
    ok('C3 the first design\'s waiting pieces are superseded at once, so none of them is ever placed', rowsOf(KEYC).length === N && rowsOf(KEYC).every(r => r.state === 'superseded' && !r.sheetId), rowsOf(KEYC).map(r => [r.poolId, r.state]));
    const c = await customsend(CKC), ans = await untilAnswered(c.id), live = liveOf(KEYC);
    ok('C4 made again by the cloud: still N pieces, the SAME pool ids, the new design', ans.state === 'done' && live.length === N && live.every(r => r.aiPath === PDF2 && r.custom === true) && JSON.stringify(live.map(r => r.poolId).sort()) === JSON.stringify(Array.from({ length: N }, (_, i) => `${KEYC}_${i + 1}`)), { ans: ans.text, live: live.map(r => [r.poolId, r.state, r.aiPath]) });
    // fewer pieces than before: the copy past the new count is never placed
    const CKF = 'custom:' + KEYF, R1f = record(CKF, [F1(N)], { [KEYF]: copies('f1', N) });
    await sendPut(R1f);
    for (let i = 1; i <= N; i++) F.seed('Charm_Pool/' + `${KEYF}_${i}`, { poolId: `${KEYF}_${i}`, runId: R, orderId: RID, transactionId: KEYF.split('_')[1], lineKey: KEYF, sku: 'DONUT', material: 'gold', state: 'ready', sheetId: null, copy: i, quantity: N, custom: true, customIndex: 0, aiPath: PDF1, customFile: 'DONUT1.ai' });
    const r3 = await sendPut(record(CKF, [F2(N - 1)], { [KEYF]: copies('f2', N - 1) }, { replaces: R1f.sent.id }));
    const c2 = await customsend(CKF), a2 = await untilAnswered(c2.id), live2 = liveOf(KEYF), past = F.doc('Charm_Pool/' + `${KEYF}_${N}`) || {};
    ok('C5 a new design with fewer pieces: those made again under their pool ids, the copy past its count superseded and on no sheet', r3.http === 200 && a2.state === 'done' && live2.length === N - 1 && live2.every(r => r.aiPath === PDF2) && past.state === 'superseded' && !listed(`${KEYF}_${N}`).length, { r3, a2: a2.text, live2: live2.map(r => [r.poolId, r.aiPath]), past: past.state });
  });

  await part('P', async () => {
    const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
    const a = src.indexOf('  /* NODUPDROP (Paul, 28 Nov 2026: "The user added'), b = src.indexOf('  /* NODUPDROP end */', a);
    ok('P0 the page has its NODUPDROP block (CustomSheet)', a > 0 && b > a);
    if (!(a > 0 && b > a)) return;
    // the block run as it stands, over the page's own lists as stubs
    const sheets = [{ fileBase: 'GF Sheet 4', charms: [{ poolId: `${KEY}_1`, custom: true }, { poolId: `${KEY}_2`, custom: true }, { poolId: `${KEYC}_1`, custom: false, sku: 'FEATHER' }] }], rows = new Map(), decided = new Set();
    const ctx = vm.createContext({ Math, Object, String, Array, JSON, Set, Map, Number, window: { CharmNestPoolPieces: PP }, B: { pool: { rows } }, allSheets: () => sheets, Pool: { charmOf: () => null }, replaceLaserWhy: () => '', sheetName: s => s.fileBase,
      decisionOf: r => (decided.has(r.key) ? { id: 'd1' } : null), linesOf: it => it.rows, pickable: () => false, pickRow: it => it.rows[0] });
    vm.runInContext(src.slice(a, b) + '\nthis.X = { nodupRule, nodupCard };', ctx);
    const X = ctx.X, row = (key, ids, spec) => ({ key, poolIds: ids, order: { receiptId: RID }, spec: spec || { designSku: 'DONUT' } }), it = r => ({ rows: [r] });
    decided.add(KEY);
    const on = X.nodupCard(it(row(KEY, [`${KEY}_1`, `${KEY}_2`])), {});
    ok('P1 the drop on a card whose sent pieces are on a sheet: refused, in the rule\'s words', !!(on && on.refuse && /GF Sheet 4/.test(on.refuse) && /Take off the sheet/.test(on.refuse)), on);
    const off = X.nodupCard(it(row(KEY, [`${KEY}_5`, `${KEY}_6`])), {});
    ok('P2 the same card with its pieces on no sheet: the drop replaces them', !!(off && off.replace && off.replace.length === 1), off);
    const stale = X.nodupCard(it(row(KEYD, [`${KEY}_1`], { designSku: 'DONUT', special: { kind: 'custom' } })), {});
    ok('P3 a page that never read the send (a custom listing\'s line with pieces on a sheet): refused too', !!(stale && stale.refuse), stale);
    const matched = X.nodupCard(it(row(KEYC, [`${KEYC}_1`])), {});
    ok('P4 a matched charm\'s pieces (no send, not custom): replaced in place as before (REVDROP)', matched === null, matched);
    const at = s => src.indexOf(s);
    const add0 = at('  function add(it, files, opts = {}) {'), addEnd = at('  /** The one design dropped on an earring-pair card'), addSrc = src.slice(add0, addEnd);
    ok('P5 add() asks the rule before anything is added, and a refusal returns at once', addSrc.indexOf('const nd = nodupCard(it, opts);') > 0 && addSrc.indexOf('if (nd && nd.refuse) { toast(nd.refuse') > addSrc.indexOf('const nd = nodupCard') && addSrc.indexOf('if (nd && nd.refuse)') < addSrc.indexOf('e.files.push(...fresh)') && /if \(c\.sent && !nd\)/.test(addSrc), addSrc.slice(0, 200));
    ok('P6 a replacement takes the earlier designs of those lines off the card (never joins them) and reopens a recorded send, named as the one it replaces', /e\.files = e\.files\.filter\(F => !own\(F\)\)/.test(addSrc) && /e\.replaces = \{ id:/.test(addSrc) && /replaces:e\.replaces\.id/.test(src));
    const send0 = at('  async function sendOnce(it, how, ck) {'), sendSrc = src.slice(send0, at('  /* REMOTESEND · askCarrier'));
    ok('P7 the send asks the rule of every line before a new send is planned', /if\(!e\.sendIntent\)\{for\(const r of rows\)\{const a=nodupRule\(r\);if\(!a\.ok\)/.test(sendSrc) && sendSrc.indexOf('nodupRule(r)') < sendSrc.indexOf('planLines(files,rows'));
    ok('P8 a refusal from the server drops the unrecorded send (no retry for ever) and says its words', /err\.data\.refused==="nodupdrop"[^\n]*delete e\.sendIntent/.test(sendSrc));
    ok('P9 a card held by the rule takes no drop (dropOk) and says why; the replaced send is never put back over the new design', /dropOk: !sent && !locked && open && !cut && !held/.test(src) && /if \(c && c\.held\) return c\.held;/.test(src) && /had\.replaces\.id===rec\.sent\.id\)continue;/.test(src));
    const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
    ok('P10 charm-nest-1.html loads the changed page files under new tokens', /charm-nest-bridge\.js\?v=[^"]*-nd1"/.test(html) && /charm-nest-pool-pieces\.js\?v=[^"]*-nd1"/.test(html));
  });

  ok('Z no Etsy call, no network', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length, { etsy: F.etsy.calls, trip: F.net.tripwire, other: F.net.other });
  log0(`rt1128-nodupdrop: ${n - failed} passed, ${failed} failed`); process.exit(failed ? 1 : 0);
})();
