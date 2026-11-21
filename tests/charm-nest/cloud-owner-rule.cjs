// THE OWNER'S RULE, offline (Paul, 20 Nov 2026: "no production work runs on any computer"; cloud plan v2 §5 and build step 8): with NO browser and NO computer carrying the
// run, and Charm_Nest_Open/drive in standby, new orders written where the mail system copies Etsy's receipts (EtsyMail_Receipts, whole, with a real master design from
// tests/charm-nest/fixtures in Storage) are built into the cloud world by its own cron, pulled into a run the cloud opens and carries, made into pool pieces, placed on sheet
// records (SETSHUT and PAIR / PAIR2 kept), nested by the solver's slots and written by the one sheet writer (.ai, labelled proof, report, preview, the record saved): all of it
// by the crons and background functions alone, run in-process against the in-memory cloud of _cloudFakes.cjs with a clock the test moves (one tick = the world's cron, the
// driver's cron, every kick they queued drained, then one minute).
//   Part A: one order (a Gold Filled and a Sterling Silver line) arrives with no run open: the cloud opens the run, and the order ends placed, nested and saved.
//   Part B: a computer's assembly made a set of part A's two sheets; two more orders arrive while the run waits for a computer at its checkpoint (parked): they are placed
//           too, never on a sheet of that set (SETSHUT), one Gold Filled and one Sterling Silver sheet filled at a time (ONEFILL), each sharing orders with at most two sheets
//           of the other metal (PAIR2), and nested and saved.
// Also: every order taken in carries the run's gold dot on the Design Station (the claim step, and the intake for orders joining a run under way), the sheet choice reads the
// shop's shared settings as values, and five more ticks after it settles change nothing.
// Asserted throughout: Etsy is never called (its stub counts every call, and every Etsy address is a tripwire), no other address is fetched, the Etsy switch is never written,
// every listing SKU table read is cacheOnly, no browser global exists and no page module is loaded.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-owner-rule.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), { performance } = require('perf_hooks');
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
// the clock: the test's (moved a minute a tick) plus the real time spent, so a solver slot's, a unit's and a writer's own time budgets run out as they do in the cloud
const p0 = performance.now();
Date.now = () => F.clock.t + Math.floor(performance.now() - p0);
const VERBOSE = !!process.env.CN_VERBOSE, log0 = console.log.bind(console), warn0 = console.warn.bind(console), err0 = console.error.bind(console);
const said = [];
console.log = (...a) => { said.push(a.join(' ')); if (VERBOSE) log0(...a); };
console.warn = (...a) => { said.push('WARN ' + a.join(' ')); if (VERBOSE) warn0(...a); };
console.error = (...a) => { said.push('ERROR ' + a.join(' ')); if (VERBOSE) err0(...a); };
const out = (...a) => log0(...a);
let failed = 0;
const ok = (name, cond, detail) => { out((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
process.on('unhandledRejection', e => { out('FAIL an unhandled rejection: ' + ((e && e.stack) || e)); failed++; });

const FIX = path.join(__dirname, 'fixtures');
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib);
const Standby = F.load('_charmNestStandby.js'), EtsyStop = F.load('_etsyStop.js');
const worldCron = F.load('charmNestWorldCron.js'), driveCron = F.load('charmNestDriveCron.js');

/* ── the shop (made-up data) ── */
const SKU = 'COMET_9427', AI = 'charmnest/master/' + SKU + '.ai';
const GF = '14k Gold Filled', SS = 'Sterling Silver';
const shared = (name, fields) => F.seed('Charm_Nest_Shared/' + name, { name, f: Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, { j: JSON.stringify(v), at: T0 - 3600e3, dev: 'd1aaaa', by: 'Ann' }])), at: T0 - 3600e3, n: 1 });
/** A receipt as Etsy's v3 list gives it (the mail system keeps it whole, items included), in the _autorunWorld.cjs wave() shape: one transaction per [metal]. */
function receipt(rid, metals) {
  const now = Math.floor(Date.now() / 1000);
  return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', status: 'Paid', is_paid: true, is_shipped: false, is_gift: false, gift_message: '',
    create_timestamp: now - 3600, created_timestamp: now - 3600, update_timestamp: now, updated_timestamp: now,
    transactions: metals.map((metal, i) => ({ transaction_id: Number(`${rid}${i + 1}`), listing_id: 1718099, receipt_id: rid, sku: SKU, title: `${SKU} charm`, quantity: 1, expected_ship_date: now + 3 * 86400, is_personalized: false,
      variations: [{ formatted_name: 'Metal', formatted_value: metal, property_id: 200, value_id: 9000 + i }] })) };
}
/** The mail system's copy of a receipt (etsyMailReceiptsMirrorCron), and its sync stamped after it. */
function arrive(r) {
  F.seed('EtsyMail_Receipts/' + r.receipt_id, { receipt_id: String(r.receipt_id), status: r.status, is_paid: true, is_shipped: false, updated_timestamp: r.updated_timestamp, raw: r });
  F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now()) });
}

/* ── one tick: the world's cron, the driver's cron, every function they queued (and those queued in turn), then one minute ── */
let ticks = 0;
async function tick() {
  ticks++;
  if (ticks % 5 === 0) F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now()) });   // (the mail system's own sync, every few minutes)
  const w = await worldCron.handler(), d = await driveCron.handler();
  if (w.statusCode >= 500 || d.statusCode >= 500) out(`  tick ${ticks}: a cron failed: ${w.body} ${d.body}`);
  for (let i = 0; i < 20 && F.net.kicks.length; i++) await F.drain(100);
  F.clock.advance(60000);
}
const runIdNow = () => (F.doc('Charm_Nest_Open/run') || {}).runId || null;
const poolOf = rid => F.list('Charm_Pool').filter(p => String(p.poolId || p._id).startsWith(rid + '_') && !['abandoned', 'superseded'].includes(p.state));
const sheetsNow = () => F.list('Charm_Nest_Sheets').filter(s => !s.archived);
const sheetOf = id => sheetsNow().find(s => s._id === id) || null;
const saved = s => !!(s && s.outputs && s.outputs.ai && s.outputs.ai.path && s.verification && s.verification.ok && !s.filesOwed && !s.saving && !s.dirty);
/** Whether every piece of these orders is on a sheet whose files are written and whose record is saved. */
function settled(rids) {
  const rows = rids.flatMap(poolOf);
  if (!rows.length || rows.length < rids.length) return false;
  return rows.every(p => p.sheetId && (sheetOf(p.sheetId) || {}).poolIds && sheetOf(p.sheetId).poolIds.includes(p.poolId || p._id) && saved(sheetOf(p.sheetId)));
}
function view() {
  const id = runIdNow(), r = id ? F.doc('Charm_Nest_Runs/' + id) || {} : null, st = F.doc('Charm_Nest_Drive/state') || {};
  return `${id ? `${r.status}/${r.step}${r.stoppedBy ? ' [' + String(r.stoppedBy).slice(0, 90) + ']' : ''}${r.note ? ' note: ' + String(r.note).slice(0, 120) : ''}` : 'no run'} | wait ${st.wait ? st.wait.kind : '-'} | pool ${F.list('Charm_Pool').length} | sheets ${sheetsNow().map(s => `${s.metal}:${(s.poolIds || []).length}/${(s.placements || []).length}${s.outputs && s.outputs.ai ? ':files' : ''}${s.filesOwed ? ':owed' : ''}${s.nestJob ? ':nest-' + (s.nestJob.status || s.nestJob.state) : ''}`).join(' ') || '-'} | jobs ${F.list('Charm_Nest_Jobs').map(j => j.status).join(',') || '-'}`;
}
async function until(what, done, max) {
  let last = '';
  for (let k = 0; k < max; k++) {
    await tick();
    const v = view(); if (v !== last) { out(`  tick ${ticks}: ${v}`); last = v; }
    if (done()) return true;
  }
  out(`  ${what}: not settled after ${max} ticks; the last things the functions said:\n    ` + said.slice(-40).join('\n    '));
  return false;
}
/** Each piece of the sheet listed once, with its charm and exactly one placement. */
function placedWell(s) {
  const charms = (s.charms || []).filter(Boolean), byPool = new Map(charms.map(c => [String(c.poolId), c])), pl = s.placements || [];
  return (s.poolIds || []).every(id => { const c = byPool.get(String(id)); return c && pl.filter(p => p && p.id === c.id).length === 1; }) && new Set(s.poolIds || []).size === (s.poolIds || []).length;
}
const blobOk = p => !!p && F.blobs.has(p) && F.blobs.get(p).buf.length > 0;
const ordersOf = s => new Set((s.poolIds || []).map(id => String(id).split('_')[0]));

(async () => {
  const t0 = performance.now();
  /* the seed: the master in Storage and the library's index, the shared settings and plate, the drive in standby, Etsy's switch not stopped, no run, no carrier */
  const entry = JSON.parse(fs.readFileSync(path.join(FIX, 'comet-master-entries.json'), 'utf8')).entries.find(e => e.sku === SKU);
  await F.bucket.file(AI).save(fs.readFileSync(path.join(FIX, SKU + '.ai')), { contentType: 'application/postscript' });
  F.seed('Charm_Master_Index/' + SKU, Object.assign({}, entry, { aiPath: AI, thumbPath: null, masterPath: null }));
  shared('sorter-settings', { insetPt: 1.5, clearancePt: -0.5, maxFill: 0.8, budgetS: 3, pullMode: 'all', autoCommit: 'on' });
  shared('sorter-stock', { preset: 'A' });
  F.seed(EtsyStop.DOC, { stopped: false });
  F.seed('Charm_Nest_Open/drive', { mode: 'standby', runMode: 'auto', cloudSteps: Standby.STANDBY_STEPS.slice(), at: T0 - 3600e3 });
  lib._driveForget();
  ok('setup · no run is open and no computer carries one', !runIdNow() && !F.list('Charm_Nest_Run_Carrier').length);

  /* ═══ A · one order arrives with every computer closed ═══ */
  const A = '4300000001';
  arrive(receipt(A, [GF, SS]));
  out('A · order ' + A + ' (Gold Filled + Sterling Silver) arrives');
  const settledA = await until('A', () => settled([A]), 40);
  const runId = runIdNow(), run = F.doc('Charm_Nest_Runs/' + (runId || '-')) || {};
  ok('A · the world build (its own cron) holds the order\'s lines', F.list('Charm_World_Lines').filter(l => String(l._id).startsWith(A + '_') && !l.gone).length === 2, F.list('Charm_World_Lines').map(l => l._id));
  ok('A · the cloud opened the run and carries it', !!runId && ((F.doc('Charm_Nest_Run_Carrier/' + runId) || {}).carrier || {}).dev === 'cloud', { runId, carrier: F.doc('Charm_Nest_Run_Carrier/' + (runId || '-')) });
  const runLines = async () => { const g = runId ? await lib.call({ op: 'runGet', runId }, {}) : null; return (g && g.run && g.run.lines) || {}; };
  ok('A · the run holds the order\'s two lines', Object.keys(await runLines()).filter(k => k.startsWith(A + '_')).length === 2, Object.keys(await runLines()));
  ok('A · settled: every piece of the order is on a sheet whose files are written and whose record is saved', settledA, view());
  const rowsA = poolOf(A);
  ok('A · two pool pieces (one a line), each naming the sheet that lists it', rowsA.length === 2 && rowsA.every(p => p.sheetId && (sheetOf(p.sheetId).poolIds || []).includes(p.poolId || p._id)), rowsA.map(p => [p._id, p.state, p.sheetId]));
  const gfA = rowsA.find(p => (p.material || p.metal) === 'gold'), ssA = rowsA.find(p => (p.material || p.metal) === 'silver');
  ok('A · the Gold Filled piece is on a Gold Filled sheet, the Sterling Silver piece on a Sterling Silver sheet', !!gfA && !!ssA && sheetOf(gfA.sheetId).metal === 'gold' && sheetOf(ssA.sheetId).metal === 'silver', rowsA.map(p => [p._id, p.material, p.sheetId && sheetOf(p.sheetId).metal]));

  // (C5: once its sheets are nested and written, the run parked at its checkpoint takes new orders in: the intake's gate reads no sheet as being nested or written)
  const Intake = F.load('_charmNestIntake.js'), busy = sheetsNow().filter(s => Intake.busySheet(s));
  ok('A · the intake\'s gate is open: no sheet reads as being nested or written once its nest and files are done', !busy.length, busy.map(s => [s._id, s.status, s.nestJob, s.filesJob]));

  /* ═══ B · a computer's assembly made a set of part A's sheets; two more orders arrive while the run waits for a computer at its checkpoint ═══ */
  const setSheets = gfA && ssA ? [gfA.sheetId, ssA.sheetId] : [];
  const before = new Map(setSheets.map(id => [id, (sheetOf(id).poolIds || []).slice()]));
  F.seed('Charm_Nest_Sets/set-1', { setId: 'set-1', runId, sheetIds: setSheets, status: 'awaiting review', seq: 1, day: '2026-11-20', name: 'Set-1', createdAt: Date.now() });
  for (const id of setSheets) F.seed('Charm_Nest_Sheets/' + id, Object.assign({}, F.doc('Charm_Nest_Sheets/' + id), { setId: 'set-1', setSeq: 1, draft: false }));
  const B = '4300000002', C = '4300000003';
  arrive(receipt(B, [GF, SS])); arrive(receipt(C, [GF, SS]));
  out('B · set-1 holds part A\'s sheets; orders ' + B + ' and ' + C + ' arrive');
  const settledB = await until('B', () => settled([B, C]), 40);
  ok('B · settled: every piece of both orders is on a sheet whose files are written and whose record is saved', settledB, view());
  const rowsB = poolOf(B).concat(poolOf(C));
  ok('B · four pool pieces, each naming the sheet that lists it', rowsB.length === 4 && rowsB.every(p => p.sheetId && (sheetOf(p.sheetId) || {}).poolIds && sheetOf(p.sheetId).poolIds.includes(p.poolId || p._id)), rowsB.map(p => [p._id, p.state, p.sheetId]));
  ok('B · SETSHUT: no new order went on a sheet of set-1 (its sheets list what they listed)', rowsB.every(p => !setSheets.includes(p.sheetId)) && setSheets.every(id => JSON.stringify(sheetOf(id).poolIds || []) === JSON.stringify(before.get(id))), { rowsB: rowsB.map(p => p.sheetId), set: setSheets });
  const gfB = [...new Set(rowsB.filter(p => (p.material || p.metal) === 'gold').map(p => p.sheetId))], ssB = [...new Set(rowsB.filter(p => (p.material || p.metal) === 'silver').map(p => p.sheetId))];
  ok('B · one Gold Filled and one Sterling Silver sheet filled at a time (ONEFILL): both orders share one new sheet of each metal', gfB.length === 1 && ssB.length === 1, { gfB, ssB });

  /* ═══ the sheets: placed, nested and saved ═══ */
  const sheets = sheetsNow().filter(s => s.runId === runId && (s.poolIds || []).length);
  for (const s of sheets) {
    const o = s.outputs || {};
    ok(`sheet ${s.fileBase || s._id} (${s.metal}) · each piece listed once, with its charm and one placement`, placedWell(s), { poolIds: s.poolIds, charms: (s.charms || []).map(c => [c.id, c.poolId]), placements: (s.placements || []).map(p => p.id) });
    ok(`sheet ${s.fileBase || s._id} · its .ai, labelled proof, report and preview are in Storage, the record says verification ok and owes no files`,
      blobOk(o.ai && o.ai.path) && blobOk(o.labelled && o.labelled.path) && blobOk(o.report && o.report.path) && blobOk(o.preview && o.preview.path) && saved(s),
      { outputs: Object.fromEntries(Object.entries(o).map(([k, v]) => [k, v && v.path ? [v.path, blobOk(v.path)] : v])), verification: s.verification, filesOwed: s.filesOwed, saving: s.saving, dirty: s.dirty });
  }
  // PAIR / PAIR2: a Gold Filled or Sterling Silver sheet shares its orders with at most two sheets of the other metal; each order's GF and SS pieces sit on paired sheets
  const fast = sheets.filter(s => ['gold', 'silver'].includes(s.metal));
  const partners = s => fast.filter(t => t.metal !== s.metal && [...ordersOf(t)].some(r => ordersOf(s).has(r))).map(t => t._id);
  ok('PAIR2 · every Gold Filled / Sterling Silver sheet shares orders with at most two sheets of the other metal', fast.every(s => partners(s).length <= 2), fast.map(s => [s._id, s.metal, partners(s)]));
  ok('PAIR · each order\'s Gold Filled sheet shares that order with its Sterling Silver sheet', [A, B, C].every(rid => { const rs = poolOf(rid), g = rs.find(p => p.material === 'gold'), s = rs.find(p => p.material === 'silver'); return g && s && partners(sheetOf(g.sheetId)).includes(s.sheetId); }));
  // (the sheet choice read the shop's own settings and plate, as every page reads them: the shared store's values, not its stored field records)
  const world = await F.load('_charmNestPlaceUnit.js').readWorld({ db: F.db, runId, read: b => lib.call(b, {}) });
  ok('placement · the sheet choice read the shop\'s shared settings and plate as values (insetPt 1.5, clearancePt -0.5, maxFill 0.8, preset A)', world.settings.insetPt === 1.5 && world.settings.clearancePt === -0.5 && world.settings.maxFill === 0.8 && world.preset === 'A', { settings: world.settings, preset: world.preset });
  // (the gold dot: every order the run took in is claimed by it on the Design Station, as the page's claim step and its intake claim them)
  const dots = [A, B, C].map(rid => [rid, F.doc('Design_RealTime_Selected_Orders/' + rid)]);
  ok('claim · every order the run took in carries the gold dot of this run (claimed by the sorter)', dots.every(([, d]) => d && d.claimed === true && d.claimRun === runId && d.claimedBy === 'sorter'), dots);

  if (VERBOSE) out('  states:', JSON.stringify({ pool: F.list('Charm_Pool').map(p => [p._id, p.state, p.sheetId]), lines: Object.entries(await runLines()).map(([k, l]) => [k, l.state]), world: F.list('Charm_World_Lines').map(l => [l._id, l.state]) }));
  /* ═══ settled stays settled: five more ticks place, nest and write nothing again ═══ */
  const snapOf = () => JSON.stringify({ pool: F.list('Charm_Pool').map(p => [p._id, p.state, p.sheetId]).sort(), sheets: sheetsNow().map(s => [s._id, s.poolIds, (s.placements || []).map(p => [p.id, p.cxPt, p.cyPt, p.angle]), s.outputs && s.outputs.ai && s.outputs.ai.path, s.layoutRev, s.contentRev, s.filesOwed || null]).sort(), jobs: F.list('Charm_Nest_Jobs').length, blobs: [...F.blobs.keys()].filter(k => !k.startsWith('charmnest/_work/')).sort() });
  const was = snapOf();
  for (let k = 0; k < 5; k++) await tick();
  ok('again · five more ticks: no piece, sheet, placement, nest job or file changed', snapOf() === was, view());

  /* ═══ the owner's rule ═══ */
  ok('rule · the cloud carried the run at the end (Charm_Nest_Run_Carrier says Cloud)', ((F.doc('Charm_Nest_Run_Carrier/' + runId) || {}).carrier || {}).dev === 'cloud');
  ok('rule · Etsy was never called (its stub counted 0 calls, 0 inventory reads)', F.etsy.calls === 0 && F.etsy.inventory.length === 0, F.etsy.log);
  ok('rule · no Etsy address and no other address was fetched (only the named background functions, in-process)', !F.net.tripwire.length && !F.net.other.length, { tripwire: F.net.tripwire, other: F.net.other });
  ok('rule · the Etsy switch was never written', !F.stopWrites.length, F.stopWrites);
  ok('rule · every listing SKU table read was cacheOnly', spy.listingSkus.every(b => b.cacheOnly === true), spy.listingSkus);
  // (the one exception: OffscreenCanvas when it is the cloud's own node canvas, _charmNestCanvas.js NodeCanvas, which _charmNestPdf.js withCanvas() puts in place for the piece maker)
  const nodeCanvas = require.cache[path.join(F.fnDir, '_charmNestCanvas.js')] ? require(path.join(F.fnDir, '_charmNestCanvas.js')).NodeCanvas : null;
  const globals = F.browserGlobals().concat(typeof globalThis.Worker !== 'undefined' ? ['Worker'] : []).filter(k => !(k === 'OffscreenCanvas' && nodeCanvas && globalThis.OffscreenCanvas === nodeCanvas));
  ok('rule · no browser global exists (the node canvas the cloud draws on aside)', !globals.length, globals);
  const pages = Object.keys(require.cache).filter(k => /charm-nest-bridge\.js$|charm-nest-1\.html$|charm-nest-sheetwin\.js$|[\\/](playwright|playwright-core|jsdom|puppeteer)[\\/]/.test(k));
  ok('rule · no page module and no browser library was loaded', !pages.length, pages);
  ok(`rule · fast: ${ticks} ticks in ${Math.round((performance.now() - t0) / 1000)} s`, performance.now() - t0 < 240000);
  out(failed ? `FAIL cloud-owner-rule (${failed})` : 'PASS cloud-owner-rule');
  process.exit(failed ? 1 : 0);
})().catch(e => { out('FAIL the test crashed: ' + ((e && e.stack) || e)); out('    ' + said.slice(-30).join('\n    ')); process.exit(1); });
