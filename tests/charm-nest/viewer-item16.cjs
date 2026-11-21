// ITEM 16 (Paul, 21 Nov 2026): Change SKU on an order line, on a staff computer (a viewer) in cloud mode "on". The page told the person "the computer that carries the run makes the
// pieces again", but no computer carries a run any more: nothing was asked of the cloud, the line kept its old pieces.
//   C  the cloud, offline (the real crons, driver, intake, placement, nest and sheet writer over _cloudFakes.cjs, mode "on" with the live lanes, run mode Auto), with NO computer:
//      C1 Change SKU of a listing (the saved link, aliasPut) followed by the page's request remakeline: each line of the listing ends with pieces of the NEW design, the old ones off
//         the sheet, the new ones placed, nested, and the sheet's files written again
//   R  the page: the real CNRelayAsk (mode on: always put, words name the cloud; mode off: as before) and Change SKU's mode-on branch (remakeline for the listing's open lines)
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item16.cjs
'use strict';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';
const LIVE_STEPS = ['pull', 'claim', 'pool', 'plan', 'nest', 'checkpoint', 'engrave', 'revalidate', 'labels', 'commit', 'complete', 'files', 'takeoff', 'rosecut', 'sweeps',
  'cmd:reorder', 'cmd:consolidate', 'cmd:empty', 'cmd:delete', 'cmd:repicture', 'cmd:seat', 'cmd:renest', 'cmd:remake', 'cmd:unkeep', 'cmd:placewaiting', 'cmd:customsend', 'cmd:setrebuild',
  'cmd:pairnow', 'cmd:leaveset', 'cmd:sendhome', 'cmd:remakeline', 'cmd:keepsync', 'cmd:release', 'cmd:skuonce'];

/* ═══ C · the cloud alone ═══ */
async function partC() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const { performance } = require('perf_hooks');
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const p0 = performance.now();
  Date.now = () => F.clock.t + Math.floor(performance.now() - p0);
  const said = [], VERBOSE = !!process.env.CN_VERBOSE;
  console.log = (...a) => { said.push(a.join(' ')); if (VERBOSE) log0(...a); };
  console.warn = (...a) => { said.push('WARN ' + a.join(' ')); if (VERBOSE) log0('WARN', ...a); };
  console.error = (...a) => { said.push('ERROR ' + a.join(' ')); if (VERBOSE) log0('ERROR', ...a); };
  const FIX = path.join(__dirname, 'fixtures');
  const lib = F.load('charmNestLibrary.js');
  const EtsyStop = F.load('_etsyStop.js'), worldCron = F.load('charmNestWorldCron.js'), driveCron = F.load('charmNestDriveCron.js');
  const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
  const OLD = 'COMET_9427', NEW = 'CELESTIAL3-COMET', GF = '14k Gold Filled', SS = 'Sterling Silver';
  const entries = JSON.parse(fs.readFileSync(path.join(FIX, 'comet-master-entries.json'), 'utf8')).entries;
  for (const sku of [OLD, NEW]) {
    const ai = 'charmnest/master/' + sku + '.ai';
    await F.bucket.file(ai).save(fs.readFileSync(path.join(FIX, sku + '.ai')), { contentType: 'application/postscript' });
    F.seed('Charm_Master_Index/' + sku, Object.assign({}, entries.find(e => e.sku === sku), { aiPath: ai, thumbPath: null, masterPath: null }));
  }
  const hashOf = sku => entries.find(e => e.sku === sku).charmHash;
  const shared = (name, fields) => F.seed('Charm_Nest_Shared/' + name, { name, f: Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, { j: JSON.stringify(v), at: T0 - 3600e3, dev: 'd1aaaa', by: 'Ann' }])), at: T0 - 3600e3, n: 1 });
  shared('sorter-settings', { insetPt: 1.5, clearancePt: -0.5, maxFill: 0.8, budgetS: 3, pullMode: 'all', autoCommit: 'off' });
  shared('sorter-stock', { preset: 'A' });
  F.seed(EtsyStop.DOC, { stopped: false });
  const setDrive = runMode => { F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode, cloudSteps: LIVE_STEPS.slice(), at: T0 - 3600e3, flippedAt: T0 - 3600e3 }); lib._driveForget(); };
  setDrive('auto');
  let seq = 0;
  function receipt(rid, items) {
    const now = Math.floor(Date.now() / 1000) - 3600 + (seq++);
    return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', status: 'Paid', is_paid: true, is_shipped: false, is_gift: false, gift_message: '',
      create_timestamp: now, created_timestamp: now, update_timestamp: now, updated_timestamp: now,
      transactions: items.map(([lid, sku, metal], i) => ({ transaction_id: Number(`${rid}${i + 1}`), listing_id: lid, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: now + 3 * 86400, is_personalized: false,
        variations: [{ formatted_name: 'Metal', formatted_value: metal, property_id: 200, value_id: 9000 + i }] })) };
  }
  function arrive(r) {
    F.seed('EtsyMail_Receipts/' + r.receipt_id, { receipt_id: String(r.receipt_id), status: r.status, is_paid: true, is_shipped: false, updated_timestamp: r.updated_timestamp, raw: r });
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now()) });
  }
  let ticks = 0;
  async function tick() {
    ticks++;
    if (ticks % 5 === 0) F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now()) });
    await worldCron.handler(); await driveCron.handler();
    for (let i = 0; i < 20 && F.net.kicks.length; i++) await F.drain(100);
    F.clock.advance(60000);
  }
  const runIdNow = () => (F.doc('Charm_Nest_Open/run') || {}).runId || null;
  const live = p => !['abandoned', 'superseded'].includes(p.state);
  const poolOf = rid => F.list('Charm_Pool').filter(p => String(p.poolId || p._id).startsWith(rid + '_') && live(p));
  const sheetsNow = () => F.list('Charm_Nest_Sheets').filter(s => !s.archived);
  const sheetOf = id => sheetsNow().find(s => s._id === id) || null;
  const savedSheet = s => !!(s && s.outputs && s.outputs.ai && s.outputs.ai.path && s.verification && s.verification.ok && !s.filesOwed && !s.saving && !s.dirty);
  /** Where a piece is: its sheet lists it, holds its charm and seats it once. */
  const seat = id => { const s = sheetsNow().find(x => (x.poolIds || []).includes(id)); if (!s) return null; const c = (s.charms || []).find(z => z && String(z.poolId) === id), n = c ? (s.placements || []).filter(p => p && p.id === c.id).length : 0; return { s, c, n }; };
  const settled = rids => { const rows = rids.flatMap(poolOf); return rows.length >= rids.length && rows.every(p => { const w = seat(p.poolId || p._id); return w && w.n === 1 && p.sheetId === w.s._id && savedSheet(w.s); }); };
  const cmdOf = id => { const d = F.doc('Charm_Nest_Run_Cmds/' + runIdNow()) || {}; return (d.cmds || {})[id] || null; };
  const view = () => { const id = runIdNow(), r = id ? F.doc('Charm_Nest_Runs/' + id) || {} : {}; return `${id ? `${r.status}/${r.step}` : 'no run'} | pool ${F.list('Charm_Pool').filter(live).map(p => `${String(p._id).slice(6, 12)}:${p.sku}:${p.state}`).join(' ')} | sheets ${sheetsNow().map(s => `${s.metal}:${(s.poolIds || []).length}/${(s.placements || []).length}${s.outputs && s.outputs.ai ? ':files' : ''}${s.filesOwed ? ':owed' : ''}${s.nestOwed ? ':nestOwed' : ''}${s.nestJob ? ':nest-' + (s.nestJob.status || s.nestJob.state) : ''}`).join(' ')}`; };
  async function until(what, done, max) {
    let last = '';
    for (let k = 0; k < max; k++) { await tick(); const v = view(); if (v !== last) { if (VERBOSE) log0(`  tick ${ticks}: ${v}`); last = v; } if (done()) return true; }
    log0(`  ${what}: not settled after ${max} ticks: ${view()}\n    ` + said.slice(-30).join('\n    '));
    return false;
  }
  const ask = async (cmd, by) => { const r = await P({ op: 'runCmdPut', runId: runIdNow(), dev: PAGE, by: by || 'Michelle', cmd, cv: CV }); return r.id || (r.cmd && r.cmd.id) || null; };
  const ended = id => () => { const c = cmdOf(id); return !!c && ['done', 'failed', 'cancelled'].includes(c.state); };
  const linesNow = async () => { const g = await lib.call({ op: 'runGet', runId: runIdNow() }, {}); return (g && g.run && g.run.lines) || {}; };
  const keysOf = async rid => Object.keys(await linesNow()).filter(k => k.startsWith(rid + '_')).sort();
  /** Every live piece of these orders: its SKU, the charm its sheet holds for it (that charm's SKU or drawing), seated once on a saved sheet. */
  const pieces = rids => rids.flatMap(poolOf).map(p => { const id = p.poolId || p._id, w = seat(id); return { id, sku: p.sku, hash: p.charmHash || null, sheet: w && w.s._id, onSheetSku: w && w.c && (w.c.sku || (w.c.orderInfo && w.c.orderInfo.sku)), onSheetHash: w && w.c && (w.c.charmHash || w.c.hash || null), seated: w ? w.n : 0, saved: !!(w && savedSheet(w.s)), layoutRev: w ? +w.s.layoutRev || 0 : 0 }; });

  // ── the shop: order A (two lines of listing 1718099, COMET_9427) arrives; the cloud opens the run, makes, places, nests and writes it
  const LA = 1718099, LC = 1718101, A = '4310000001', B = '4310000002', C = '4310000003';
  arrive(receipt(A, [[LA, OLD, GF], [LA, OLD, SS]]));
  const firstOk = await until('A', () => settled([A]), 45);
  ok('C0 mode on, no computer: order A is made, placed, nested and its sheets written by the cloud alone', firstOk && pieces([A]).every(p => p.sku === OLD), { view: view(), pieces: pieces([A]) });
  const runId = runIdNow();
  ok('C0 the cloud carries the run', ((F.doc('Charm_Nest_Run_Carrier/' + runId) || {}).carrier || {}).dev === 'cloud');
  if (!firstOk) return;

  /* C1 · Change SKU for listing LA (the page's aliasPut), then the page's request: remakeline for the listing's open lines */
  {
    const before = pieces([A]);
    const al = await P({ op: 'aliasPut', listingId: String(LA), sku: NEW, fromSku: OLD, by: 'Michelle', dev: PAGE, cv: CV });
    ok('C1 the saved link is taken from a viewer in mode on (a person\'s own input)', al.http === 200 && al.verified === true, al);
    const kA = await keysOf(A), id = await ask({ type: 'remakeline', ids: kA, label: `Change SKU: ${OLD} → ${NEW}` });
    ok('C1 the request is put in the cloud, naming both lines of the listing', !!id && kA.length === 2, { id, kA });
    const done = await until('C1', () => ended(id)() && settled([A]), 60);
    const c = cmdOf(id) || {}, after = pieces([A]);
    ok('C1 the cloud answered the request done, in words', done && c.state === 'done' && /made again/.test(c.text || ''), c);
    ok('C1 every line of the listing now has pieces of the NEW design (the pool row and the charm its sheet holds)', after.length === before.length && after.every(p => p.sku === NEW && (p.onSheetSku == null || p.onSheetSku === NEW)), after);
    ok('C1 placed once each, on sheets nested and written again (files saved, verified, nothing owed)', after.every(p => p.seated === 1 && p.saved), after);
    ok('C1 the sheets were laid out again (their layout moved on)', after.every(p => p.layoutRev > (before.find(b => b.sheet === p.sheet) || { layoutRev: 0 }).layoutRev || before.every(b => b.sheet !== p.sheet)), { before, after });
    const L = await linesNow();
    ok('C1 the run\'s lines read the new charm, nothing held, none left "vacated"', kA.every(k => L[k] && ['pooled', 'written'].includes(L[k].state) && !L[k].vacated && !L[k].hold), kA.map(k => L[k] && [L[k].state, L[k].sku, L[k].vacated]));
  }
  log0(`  (C took ${ticks} ticks)`);
  if (VERBOSE) log0(said.filter(s => /\[drive\]|request|remake|vacate|inplace/i.test(s)).slice(-60).join('\n'));
  void B; void C; void LC; void hashOf;
}

/* ═══ R · the page's relay (window.CNRelayAsk, the real source from charm-nest-bridge.js, in a bare context) and the Change SKU branch for mode "on" ═══ */
async function partR() {
  const vm = require('vm'), src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const i = src.indexOf('window.CNRelayAsk = async function'), j = src.indexOf('\n};\n', i), relay = src.slice(i, j + 3);
  async function asked(o) {
    const calls = [], cmds = {};
    const w = { console, Promise, Date, JSON, setTimeout: f => setTimeout(f, 0) };
    w.window = w; w.B = { run: null }; w.employeeName = () => 'Michelle';
    w.CharmNestRunFollow = { state: () => ({ runId: 'run-i16' }), dev: () => PAGE };
    w.CNViewer = { on: () => !!o.on };
    w.api = async (fn, body) => {
      calls.push(body);
      if (body.op === 'runCarrier') return { ok: true, mine: false, alive: !!o.alive, carrier: o.on ? { dev: 'cloud', by: 'Cloud', at: 0 } : { dev: 'd0paulpc01', by: 'Paul', at: 0 } };
      if (body.op === 'runCmdPut') { cmds.c1 = { id: 'c1', state: 'waiting' }; return { ok: true, id: 'c1' }; }
      if (body.op === 'runCmdList') { if (o.answer && cmds.c1) Object.assign(cmds.c1, o.answer); return { cmds: Object.values(cmds) }; }
      return { ok: true };
    };
    vm.runInContext(relay, vm.createContext(w));
    const steps = [], ans = await w.CNRelayAsk({ type: 'remakeline', ids: ['4310000001_43100000011'] }, t => steps.push(t), 60000, o.always ? { always: true } : undefined);
    return { ans, steps, put: calls.find(c => c.op === 'runCmdPut') };
  }
  let r = await asked({ on: true, alive: false, answer: { state: 'done', text: '1 order line(s) made again: 1 piece(s) on GF Draft 1' } });
  ok('R1 mode on, the cloud\'s carrier beat old: the request is still put in the cloud and its answer shown, the words name the cloud', !!r.put && r.put.cmd.type === 'remakeline' && r.ans.ok === true && /made again/.test(r.ans.text) && r.steps.every(t => /the cloud/.test(t) && !/computer/.test(t)), r);
  r = await asked({ on: false, alive: false });
  ok('R2 mode off, no carrier open: nothing is put (as before)', !r.put && r.ans.none === true, r);
  const b = src.slice(src.indexOf('async function editSku(row, sku, opt)'), src.indexOf('async function editSkuOnce('));
  ok('R3 Change SKU on a viewer in mode on asks the cloud: remakeline with the listing\'s open lines, always put, and the panel follows the cloud\'s answer', /CNViewer\.on\(\) && typeof window\.CNRelayAsk === "function"[\s\S]{0,1500}type: "remakeline", ids: keys/.test(b) && /\{ always: true \}\)\s*\n\s*\.then\(ans =>/.test(b) && /later\(flow, asked, true\)/.test(b));
  ok('R4 charm-nest-1.html loads the changed bridge under a new token', /charm-nest-bridge\.js\?v=[^"]*-i16(?:-\w+)*"/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')));
}

(async () => {
  if (process.env.I16_PART === 'cloud') { await partC(); }
  else {
    await partR();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { I16_PART: 'cloud' }), encoding: 'utf8', timeout: 540000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) |^  /.test(l)) log0(l);
    if (r.status !== 0) failed++;
  }
  if (process.env.I16_PART) { log0(failed ? `FAIL part (${failed})` : 'PASS part'); process.exit(failed ? 1 : 0); }
  log0(failed ? `FAIL viewer-item16 (${failed})` : 'PASS viewer-item16');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item16: ' + (e && e.stack || e)); process.exit(1); });
