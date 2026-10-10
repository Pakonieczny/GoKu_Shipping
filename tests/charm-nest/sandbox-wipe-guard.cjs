// GUARD TEST for the one sandbox wipe (Paul, 10 Oct 2026: "completely wipe ALL sandbox data and order info; the only thing that
// remains is the employee efficiency and the Charm repo"). Offline: in-memory Firestore (refuses nested arrays, logs every write)
// and Storage (logs every file) against the REAL charmNestLibrary, firebaseOrders, designArchive, charmNestOutput and
// _etsyMailOrderLink code. No network, no paid call, no live record.
//
// How it cannot be fooled by a hand list:
//   1. THE SAME session runs on both sides: once as production (no sandbox flag) and once as the sandbox (every call tagged),
//      so every sandbox key has a production twin under the same real order number, as in the live sorter.
//   2. The session drives EVERY op the library exports (lib.ops), the station doors, the design archive, the file door and the
//      customer-mail door. An op that is not driven must be named in READ_ONLY / REPO / NOT_SANDBOX with its reason, and its
//      source is scanned for a write: a NEW writing op fails here until the session drives it.
//   3. Every write the sandbox session made (the write log of the fake) is classified by the registry
//      (charm-nest-sandbox-families.js ownerOf): a sandbox write that lands anywhere the registry does not name FAILS, and so
//      does a registry family the session never wrote (the session would not be testing it).
//   4. After the wipe (the real sandboxReset op, called until it says it is done) the whole database is compared with its
//      state before the sandbox session: no sandbox document or file is left; every production document, every protected one
//      and every file the sandbox did not write is byte-identical; only the registry's keep list (employee efficiency,
//      the pull budget, the Charm repo, config…) may differ, and only by what the sandbox added to it.
//   5. sandboxStatus (read-only) counts every family of the registry: non-zero before, zero after, and says what is kept.
//   6. Purge all run history ends in the same wipe.
//   node tests/charm-nest/sandbox-wipe-guard.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
process.env.CHARM_NEST_DELETE_CODE = 'guard-test-delete-code';
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs, writes, fileWrites, TS, canon, clone } = Fk;
const fnDir = path.join(__dirname, '../../netlify/functions');
const Families = require('../../charm-nest-sandbox-families.js');
const warnings = [], realWarn = console.warn, realError = console.error; console.warn = (...a) => { warnings.push(a.map(String).join(' ')); }; console.error = (...a) => { warnings.push('ERROR ' + a.map(String).join(' ')); };   // (the handler logs every refusal; they are named in the output instead)
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), stations = require(path.join(fnDir, 'firebaseOrders.js')), archive = require(path.join(fnDir, 'designArchive.js')),
  output = require(path.join(fnDir, 'charmNestOutput.js')), mail = require(path.join(fnDir, '_etsyMailOrderLink.js'));

/* ── a request, each on its own tick of the clock (instant in-memory calls must not collide on a timeline key) ── */
let last = 0;
async function tick(fn) { const base = Date.now, t = (last = Math.max(base(), last + 1)); Date.now = () => t; try { return await fn(); } finally { Date.now = base; } }
const post = b => tick(async () => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; });
const live = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };   // (the clock moves inside the call: a wipe works against it)
const driven = new Set();   // ops of lib.ops the sandbox session called and the library answered 200
const issues = [];          // what the session could not do (named in the output: nothing is hidden)
async function must(b, sb) {
  const r = await post(Object.assign({}, b, sb ? { sandbox: true } : {}));
  assert.strictEqual(r.status, 200, `${sb ? 'sandbox ' : ''}${b.op}: ${JSON.stringify(r.body).slice(0, 300)}`);
  if (sb) driven.add(b.op);
  return r.body;
}
async function may(b, sb, why) {   // an op whose arguments need fixtures this offline session does not have: tried, its answer named
  const n = writes.length + fileWrites.length, r = await post(Object.assign({}, b, sb ? { sandbox: true } : {}));
  if (r.status === 200 || writes.length + fileWrites.length > n) { if (sb) driven.add(b.op); }   // (a refusal that wrote first still counts: its writes are logged and classified)
  if (r.status !== 200) issues.push(`${b.op}: ${r.status} ${JSON.stringify(r.body).slice(0, 110)} (${why})`);
  return r;
}
const station = (sb, body) => tick(async () => { const r = await stations.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sb ? { sandbox: '1' } : {}, body: JSON.stringify(body) }); assert.strictEqual(r.statusCode, 200, JSON.stringify(body).slice(0, 100) + ' → ' + r.body); return JSON.parse(r.body); });
const putArchive = (sb, rid) => tick(async () => { const r = await archive.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sb ? { sandbox: '1' } : {}, body: JSON.stringify({ op: 'put', orders: [{ receiptId: rid, completedAtMs: Date.now(), completedDay: '2026-10-09', items: [{ transactionId: '1', imageUrl: 'https://i.etsystatic.com/il/x.jpg' }] }] }) }); assert.strictEqual(r.statusCode, 200, r.body); });
const fileDoor = (sb, op, b) => tick(async () => { const r = await output.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({ op }, b, sb ? { sandbox: true } : {})) }); assert.strictEqual(r.statusCode, 200, op + ' ' + r.body); return JSON.parse(r.body); });

/* ── the orders the sandbox plays under their real numbers ── */
const A = '4173162973', B = '4170408845', C = '4170000555', D = '4170000777';
const DAY = '2026-10-09', RUN = 'run-20261009-1', RUN2 = 'run-20261009-2', SHEET = 'gold-gf-guard-1', SHEET2 = 'silver-ss-guard-2', ROSE = 'sheet-rose-guard', SESSION = 'sorter-session-guard', COMPUTER = 'computer-guard1';
const NOW = Fk.realNow();
const SET = `set-${DAY}-1`;

/** One whole session of the sorter. sb true = a sandbox page (every call carries the flag), false = the same work on production. */
async function session(sb) {
  const L = b => must(b, sb), P = sb ? 'Sandbox_' : '';
  // ── Review: custom orders (QR label printed twice, Complete Order, reopened), a custom design sent to a sheet, a decision ──
  const label = { w: 40, h: 20, qr: 'x'.repeat(40), lines: [A, 'CHAIN_8941'] };
  await L({ op: 'customPut', key: `${A}_10010`, by: 'paul', label, receiptId: A, transactionId: '10010', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Chain only', kind: 'chainOnly' });
  await L({ op: 'customPut', key: `${A}_10010`, by: 'paul', label, receiptId: A });
  await L({ op: 'customPut', key: `${B}_20020`, by: 'ann', how: 'button', receiptId: B, transactionId: '20020', sku: 'CUSTOM-N-001', title: 'Custom necklace', kind: 'custom' });
  await L({ op: 'customReopen', key: `${B}_20020`, by: 'ann', how: 'reopen' });
  await L({ op: 'customDelete', key: `${B}_20020`, by: 'ann', how: 'reopen' });   // (the same door under its second name)
  const at = NOW - 3 * 86400000, ck = `custom:${B}:CUSTOM-N-001`;
  await L({ op: 'customSheetPut', record: { ck, rid: B, at: at - 1000, phase: 'sent',
    files: [{ id: 'file-custom-1', name: 'Customer design.ai', kind: 'ai', size: 2400, hash: 'abcde01234567890123456789', cloud: { path: `charmnest/custom/${B}/original.pdf`, url: 'https://saved.example/original.pdf' }, metal: 'silver', qty: 1, pieces: 1, wMm: 14, hMm: 18, maxPt: 52, minPt: 40, maxAreaPt2: 2000, state: 'ready' }],
    sent: { id: `custom-sheet:${ck}:${at}`, at, by: 'paul', lines: { [`${B}_20020`]: [{ f: 'file-custom-1', i: 0 }] } } } });
  await L({ op: 'customDecide', key: `${A}_10010`, kind: 'chainOnly', by: 'paul' });
  await L({ op: 'customDecide', keys: [`${D}_40040`], kind: 'rework', by: 'paul' });
  if (sb) await L({ op: 'customDecide', keys: ['4170000888_50050'], kind: 'rework', by: 'paul' });   // (a line no one has read and production never decided: the sandbox's decision is the only thing on its record)
  await L({ op: 'customGet', key: `${A}_10010` }); await L({ op: 'customSheetGet', keys: [ck] });
  // ── the pool, sets and counters, runs and their archive, releases, the bridge log, arrivals ──
  await L({ op: 'poolPut', pools: [`${A}_10010_1`, `${B}_20020_1`, `${C}_30030_1`].map((poolId, i) => ({ poolId, orderId: poolId.split('_')[0], transactionId: poolId.split('_')[1], lineKey: poolId.split('_').slice(0, 2).join('_'), runId: RUN, state: 'pooled', material: ['gold', 'silver', 'gold'][i], copy: 1, quantity: 1, custom: false })) });
  await L({ op: 'poolUpdate', poolIds: [`${A}_10010_1`], patch: { state: 'written', sheetId: SHEET } });
  await L({ op: 'setAllocate', day: DAY, runId: RUN });
  await L({ op: 'setAllocate', day: DAY, runId: RUN2, group: 'silver' });
  await L({ op: 'setUpdate', setId: SET, patch: { orders: { [A]: { held: null, lines: [1] } }, labels: { a: 1 } } });
  await L({ op: 'runPut', run: { runId: RUN, status: 'complete', step: 'complete', day: DAY, lines: { [`${A}_10010`]: { orderId: A, key: `${A}_10010` } } } });
  await L({ op: 'runArchive', runId: RUN, parts: [{ json: JSON.stringify({ [`${C}_30030`]: { orderId: C, key: `${C}_30030` } }) }] });
  await L({ op: 'releasePut', released: { gf: DAY }, lastReleased: { gf: DAY } });
  await L({ op: 'bridgeLog', session: SESSION, meta: { by: 'paul' }, rows: [{ t: Date.now(), dir: 'cmd', type: 'ping' }, { t: Date.now(), dir: 'evt', type: 'pong' }] });
  await L({ op: 'arrivalRecord', orders: [{ id: A, createTs: 1 }, { id: C, createTs: 1 }] });
  // ── sheets: saved, cut, hold/release, back records, pdf, an empty one archived, one deleted and the sandbox's restore of it ──
  await L({ op: 'putSheet', sheet: { id: SHEET, metal: 'gold', metalLabel: 'GF 14/20', day: DAY, status: 'complete', charmCount: 3, placedCount: 3, density: 0.71, runId: RUN, setId: SET, orders: [A, B], poolIds: [`${A}_10010_1`], outputs: { preview: { url: 'https://saved.example/p.png', path: `charmnest/${sb ? 'sandbox/' : ''}sheets/${DAY}/${SHEET}/${SHEET}.png` } }, names: 'anna' } });
  await L({ op: 'putSheet', sheet: { id: SHEET2, metal: 'silver', metalLabel: 'SS', day: DAY, status: 'partial', charmCount: 2, placedCount: 0, runId: RUN2, setId: SET, orders: [C] } });
  await L({ op: 'flowApply', by: 'Paul', steps: [{ type: 'hold', sheetIds: [SHEET2], note: 'check the back' }] });
  await L({ op: 'flowApply', by: 'Paul', steps: [{ type: 'release', sheetIds: [SHEET2] }] });
  // a laser-ready set (every line, file and QR present: the same fixture server-stamps.cjs uses): cut a sheet, undo it, cut the set, seal a sheet
  const LSET = 'set-2026-10-09-9', LRUN = 'guard-laser-fixture', lineRows = {};
  store.set(`${P}Charm_Nest_Sets/${LSET}`, { setId: LSET, seq: 9, runId: LRUN, sheetIds: ['gl-1', 'gl-2'] });
  for (const [sheetId, index, orders] of [['gl-1', 2, ['4170000002', '4170000003']], ['gl-2', 3, ['4170000004']]]) {
    const poolIds = orders.map((order, i) => order + '_' + (sheetId === 'gl-1' ? 60001 + i : 60003 + i) + '_1');
    for (const pool of poolIds) lineRows[pool.slice(0, pool.lastIndexOf('_'))] = { orderId: pool.split('_')[0], state: 'written', quantity: 1, poolIds: [pool], engraveCandidate: false };
    store.set(`${P}Charm_Nest_Sheets/${sheetId}`, { id: sheetId, metal: 'gold', setId: LSET, runId: LRUN, sheetIndex: index, fileBase: 'GF_Oct.09.26_Set-9_Sheet-' + index, orders, poolIds, placements: [], placedCount: poolIds.length, status: 'complete', verification: { ok: true },
      outputs: { ai: { path: sheetId + '.ai', url: 'https://example.test/' + sheetId + '.ai' }, preview: { path: sheetId + '.png', url: 'https://example.test/' + sheetId + '.png' } }, label: { files: [{ path: sheetId + '-qr.png', url: 'https://example.test/' + sheetId + '-qr.png', payload: orders.join(','), orders }] } });
  }
  store.set(`${P}Charm_Nest_Runs/${LRUN}`, { runId: LRUN, lines: lineRows });
  await L({ op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'gl-1', by: 'Cara' });
  await L({ op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'gl-1', done: false });
  await L({ op: 'laserDone', stage: 'laser', kind: 'set', id: LSET, by: 'Dan' });
  await L({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'sheet', id: 'gl-2' }] });
  await L({ op: 'laserDoneList' }); await L({ op: 'laserStatus' }); await L({ op: 'laserSheetLast', by: 'Cara' });
  await L({ op: 'backPut', back: { poolId: `${A}_10010_1`, sheetId: SHEET, setId: SET, order: A, transactionId: '10010', sku: 'BR-1', copy: 1, text: 'Love you, Mom', approvedAt: NOW - 5000, approvedBy: 'Eve' } });
  await L({ op: 'backInvalidate', poolIds: [`${A}_10010_1`] });
  await L({ op: 'runPut', run: { runId: RUN2, status: 'running', step: 'nesting', day: DAY, lines: {} } });   // (an open run: only a sheet of an open run can be repacked)
  await L({ op: 'archiveEmptySheet', id: SHEET2, runId: RUN2 });
  await L({ op: 'sheetPdf', ids: [SHEET, 'no-such-sheet'] });
  await L({ op: 'putSheet', sheet: { id: 'gold-gf-guard-3', metal: 'gold', metalLabel: 'GF 14/20', day: DAY, status: 'partial', charmCount: 1, placedCount: 1, runId: RUN, setId: SET, orders: [C] } });
  await L({ op: 'deleteSheet', id: 'gold-gf-guard-3', code: process.env.CHARM_NEST_DELETE_CODE });
  // ── Rose Gold: claim, plan, record the cut (its leftover), take off, release; the rehearsal ──
  const create = require(path.join(fnDir, '_charmNestRoseStock.js')), shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });
  store.set(`${P}Charm_Nest_Runs/run-rose`, { lines: { a: { orderId: D, state: 'written', poolIds: ['pool-1'], spec: { quantity: 1, engraveCandidate: false } } } });
  const rose = { id: ROSE, metal: 'rose', sheetIndex: 1, fileBase: 'RG_Oct.09.26_Set-1_Sheet-1', verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, setId: SET, runId: 'run-rose', poolIds: ['pool-1'], placedCount: 1,
    placements: [{ id: 'pool-1', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'x', orders: [D] }] }, orders: [D] };
  const claim = await L({ op: 'roseClaim', sheetId: rose.id, wPt: 100, hPt: 50 });
  store.set(`${P}Charm_Nest_Sheets/${rose.id}`, rose);   // (what the nester's own save writes: the stock calls above and below read it)
  const plan = await L({ op: 'rosePlan', sheetId: rose.id, stockId: claim.stock.id, revision: 0, fingerprint: create.fingerprint(rose), shapesJson: JSON.stringify([shape('pool-1', 2, 2, 10, 30)]), allowanceMm: 0.2, cut: true });
  await L({ op: 'roseRecordCut', sheetId: rose.id, stockId: claim.stock.id, revision: 0, planHash: plan.planHash, by: 'Kim' });
  if (process.env.DBG && sb) console.log(JSON.stringify([...store].filter(([k]) => /Remnants|Rose_Stock/.test(k)), null, 0).slice(0, 2500));
  await L({ op: 'roseList' }); await L({ op: 'roseGet', stockId: claim.stock.id });
  const rose2 = { id: 'sheet-rose-guard-2', metal: 'rose', sheetIndex: 2, fileBase: 'RG_Oct.09.26_Set-1_Sheet-2', status: 'draft', dirty: true, runId: 'run-rose', poolIds: ['pool-7'], placedCount: 1, placements: [{ id: 'pool-7', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], orders: [D] };
  const claim2 = await L({ op: 'roseClaim', sheetId: rose2.id, wPt: 100, hPt: 50 });
  store.set(`${P}Charm_Nest_Sheets/${rose2.id}`, rose2);
  await L({ op: 'roseTakeOff', sheetId: rose2.id, ids: ['pool-7'], by: 'Kim' });
  await L({ op: 'roseRelease', stockId: claim2.stock.id, sheetId: rose2.id, by: 'Kim' });
  await L({ op: 'partialPolicySet', metal: 'rose', mode: 'new', by: 'Kim' });
  const remId = `${claim.stock.id}-1`;
  await L({ op: 'remnantMark', id: remId, status: 'discarded', by: 'Kim' }); await L({ op: 'remnantMark', id: remId, status: 'available', by: 'Kim' });
  await L({ op: 'partialClaim', metal: 'rose', id: remId, sheetId: 'sheet-rose-guard-3', by: 'Kim' });
  await L({ op: 'partialRelease', sheetId: 'sheet-rose-guard-3', by: 'Kim' });
  await L({ op: 'partialUse', id: remId, sheetId: SHEET, by: 'Kim' });
  await L({ op: 'remnantBackfill' }); await may({ op: 'partialBackfill' }, sb, 'the same backfill under its second name');
  const made = await L({ op: 'sheetMake', metal: 'rose', wMm: 300, hMm: 200, by: 'Kim' });
  await L({ op: 'sheetDelete', id: made.item.id, reason: 'made by mistake', by: 'Kim' });
  await L({ op: 'sheetMake', metal: 'gold14k', wMm: 300, hMm: 200, by: 'Kim' });
  if (sb) { await L({ op: 'roseDemo', demoId: 'rgdemo-guard1', action: 'start' }); }
  // ── cancel records ("kept for good" in production), the history a restore keeps, the order's timeline ──
  await L({ op: 'cancelPut', orderId: C, by: 'Etsy', why: 'buyer cancelled', record: { buyer: 'Buyer C', lines: [{ transactionId: '30030', title: 'Charm' }] } });
  await L({ op: 'cancelPut', orderId: A, by: 'paul', why: 'duplicate' });
  await L({ op: 'cancelRestore', orderId: A, by: 'paul' });
  if (sb) await L({ op: 'sandboxCancel', orderId: B, by: 'paul' }); else await L({ op: 'cancelPut', orderId: B, by: 'paul', why: 'sandbox twin' });
  await L({ op: 'timelineAdd', events: [{ orderId: A, type: 'qrLabel', id: 'q1', text: 'QR label for GF Sheet 1', by: 'paul' }, { orderId: B, type: 'designSent', id: 'ds1', text: 'sent', by: 'paul' }, { orderId: C, type: 'note', id: 'n1', text: 'note', by: 'paul' }] });
  // ── the learned maps ──
  const design = sb ? 'LION_SANDBOX_9' : 'TRICERATOPS_PROD_1';
  await L({ op: 'aliasPut', listingId: '1718000', sku: design, fromSku: 'CUTE_TRICERATOPS_4170', title: 'CUTE TRICERATOPS W/ HEARTS', by: 'paul' });
  await L({ op: 'aliasPut', listingId: '1718000', sku: design, title: 'CUTE TRICERATOPS W/ HEARTS', by: 'paul' });
  // a person's word for a whole listing (ADDONCUSTOM: custom / rework / regular …) lives on the SAME alias document as the SKU answer: production writes the shared one, the sandbox only its own copy (Sandbox_Charm_Sku_Aliases), which the wipe deletes
  await L({ op: 'listingKindPut', listingId: '1718000', kind: 'custom', title: sb ? 'Add a Lion Charm' : 'Add a Triceratops Charm', note: 'an add-on listing', by: 'paul' });
  await L({ op: 'listingKindPut', listingId: '1718001', kind: 'rework', by: 'paul' }); await L({ op: 'listingKindPut', listingId: '1718001', kind: 'regular', by: 'paul' }); await L({ op: 'listingKindPut', listingId: '1718001', kind: '' });
  await L({ op: 'optionMapPut', listingId: '1718000', optionName: 'Size', optionValue: 'Small', map: { field: 'design', value: design }, by: 'paul' });
  const row = await L({ op: 'noDesignPut', sku: sb ? 'LION_NODESIGN' : 'PROD_NODESIGN_1', note: 'a SKU with no design', by: 'paul' });
  await L({ op: 'noDesignPut', pattern: sb ? '^LION_ONLY' : '^PROD_ONLY', note: 'a pattern', by: 'paul' });
  const pat = await L({ op: 'noDesignPut', pattern: sb ? '^LION_TWO' : '^PROD_TWO', note: 'a second pattern', by: 'paul' });
  await L({ op: 'noDesignDelete', id: pat.id || row.id }); await L({ op: 'noDesignGet' }); await L({ op: 'aliasGet' }); await L({ op: 'optionMapGet' });
  // ── shape guidance, the engraving job (its kick fails offline: the job records that), nesting jobs ──
  const H1 = 'a'.repeat(64), H2 = 'b'.repeat(64), shapeKey = JSON.stringify([H1, 40, 40, 2, 900]), otherKey = JSON.stringify([H2, 20, 20, 2, 100]);
  const put = await L({ op: 'putShapeGuidance', profiles: [{ key: shapeKey, profile: { adaptability: 40, interlock: 50, edgeAffinity: 90, priority: 70, family: 'elongated', edgeRole: 'long-edge', mates: ['compact'], angles: [0, 90], partners: { [otherKey]: 85 } } }] });
  assert.strictEqual(put.count, 1, 'the shape guidance is stored');
  await may({ op: 'startAgent', mode: 'grouping', payload: { sourceName: 't', overview: 'data:image/jpeg;base64,/9j/', charms: [{ index: 0, thumb: 'data:image/png;base64,iVBORw0KGgo=' }] } }, sb, 'the background job cannot be kicked offline: the job record and its parked payload are written first, as in a real failure');
  const job = await L({ op: 'startJob', sheetId: SHEET, job: { pieces: [{ id: 'p1' }] } });
  await L({ op: 'stopJob', id: job.id });
  // ── what the library reads must not write: the status, lists, the stream ──
  await L({ op: 'cancelList' }); await L({ op: 'cancelList', idsOnly: true }); await L({ op: 'cancelFates', orderId: C });
  await L({ op: 'findSheets', q: '1718000' }); await L({ op: 'findSheets', q: A });   // (a search that finds nothing by order fills the listings of the sheets it finds through their runs: a write to the sheet records)
  await L({ op: 'sandboxStatus', light: true }); await L({ op: 'poolList', runId: RUN }); await L({ op: 'listSheets' });
  // ── the stations (firebaseOrders): locks and claims, finished orders, messages, notes, sign-in, activity, timeline, live; the design archive ──
  const F = body => station(sb, body);
  await F({ rtLockIds: [A, B], clientId: 'client-1', page: 'design' });
  await F({ rtClaimIds: [B], claimedBy: 'sorter', claimRun: RUN });
  await F({ completedIds: [A] }); await F({ completedIds: [C] }); await F({ uncompleteIds: [C] });
  await F({ newMessage: 'hello team', orderNumber: A, employeeName: 'Paul' });
  await F({ newMessage: 'DESIGNED :)', orderNumber: B, employeeName: 'Paul', designSetId: 'set-a' });
  await F({ orderNumber: B, staffNote: 'engrave the back' });
  await F({ session: { id: 'session-guard-0001', event: 'start', station: 'sorting', computerId: COMPUTER, person: 'Paul', computerLabel: 'Bench 1' } });
  await F({ activity: [{ id: 'activity-guard-0001', station: 'sorting', action: 'scan', person: 'Paul', orderId: A, parts: 2, at: NOW - 60000, computer: COMPUTER, session: 'session-guard-0001' }, { id: 'activity-guard-0002', station: 'sorting', action: 'complete', person: 'Paul', orderId: A, parts: 2, orders: 1, at: NOW - 30000, computer: COMPUTER, session: 'session-guard-0001' }] });
  await F({ timeline: [{ orderId: A, type: 'scan', id: 'scan-guard-1', station: 'sorting', by: 'Paul', text: 'scanned' }] });
  await F({ live: { v: 1, event: 'work', station: 'sorting', device: 'sorting-1', computer: COMPUTER, session: 'session-guard-0001', person: 'Paul', startAt: NOW - 20000, order: { rid: A, orderNumber: A, customer: 'Buyer A', scannedAt: NOW - 20000 } } });
  await F({ session: { id: 'session-guard-0001', event: 'end', station: 'sorting', computerId: COMPUTER, person: 'Paul' } });
  await putArchive(sb, A);
  // ── the file door: a sheet picture, a QR label, a custom design file (every path a sandbox page writes is moved under charmnest/sandbox/) ──
  const png = Buffer.from([137, 80, 78, 71]).toString('base64');
  await fileDoor(sb, 'put', { path: `charmnest/sheets/${DAY}/${SHEET}/${SHEET}.png`, contentType: 'image/png', base64: png });
  await fileDoor(sb, 'put', { path: `charmnest/labels/${A}/qr.png`, contentType: 'image/png', base64: png });
  await fileDoor(sb, 'put', { path: `charmnest/custom/${B}/original.pdf`, contentType: 'application/pdf', base64: png });
  // ── the customer messages the sorter sends through the inbox (sandbox: olsb_…, sent at once, never to Etsy) ──
  if (sb) await tick(() => mail.ask({ name: 'Paul', username: 'paul' }, { receiptId: A, scope: 'order', text: 'Which chain length would you like?', orderNumber: A, buyerName: 'Buyer A', sandbox: true }));
  else store.set(`EtsyMail_OrderLinks/ol_${A}_o_prod9`, { id: `ol_${A}_o_prod9`, receiptId: A, scope: 'order', sandbox: false, status: 'open', outbox: [{ id: 'x1', text: 'which chain length?', status: 'queued' }], sim: [], createdAtMs: NOW });   // (production's engagement waits for a thread: a real conversation lookup the offline session has no Etsy for)
  // ── a file an older build mirrored (design-archive/sandbox/…: the sandbox no longer mirrors, but earlier runs left such files) ──
  if (sb) { blobs.set('design-archive/sandbox/listing/0123456789abcdef0123456789abcdef01234567.jpg', { buf: Buffer.from('x') }); fileWrites.push({ kind: 'save', path: 'design-archive/sandbox/listing/0123456789abcdef0123456789abcdef01234567.jpg' }); }
  // ── what the paid or background jobs write and this offline session cannot call (a Claude reading, the engraving job's result): seeded as they store it ──
  store.set(`${P}Charm_Nest_Agent_Cache/agentc-${'a'.repeat(40)}`, { text: 'ANNA', cacheKey: 'a'.repeat(40), createdAt: new TS(NOW) });
  if (sb) {
    // the stream and the orders it plays, as sandboxPullOrders writes them (its own test drives that op against a fake Etsy)
    store.set('Charm_Sandbox/current', { source: 'etsy-pull', path: 'charmnest/sandbox/orders-pull/1760000000000-ab12.json', count: 3, at: NOW, takenBy: 'guard' });
    blobs.set('charmnest/sandbox/orders-pull/1760000000000-ab12.json', { buf: Buffer.from(JSON.stringify({ receipts: [{ receipt_id: 1, status: 'Paid' }, { receipt_id: 2, status: 'Paid' }, { receipt_id: 3, status: 'Paid' }] })) });
    fileWrites.push({ kind: 'save', path: 'charmnest/sandbox/orders-pull/1760000000000-ab12.json' });
    await L({ op: 'sandboxStream', action: 'ensure', speed: 50, seed: 7 }); await L({ op: 'sandboxStream', action: 'tick', speed: 50 }); await L({ op: 'sandboxStream', action: 'get' });
  }
}

/** Records a long-running sandbox holds in bulk: pages and pages of them (the wipe deletes 300 at a time and answers on the clock). */
function bulk(sb, n) {
  const P = sb ? 'Sandbox_' : '';
  for (let i = 0; i < n; i++) {
    const rid = String(4170100000 + i), pad = String(i).padStart(5, '0'), line = `${rid}_${100 + (i % 900)}`;
    store.set(`${P}Order_Timeline/${rid}~note~bulk${pad}`, { orderId: rid, type: 'note', at: NOW - i, by: 'System', source: 'sorter', station: 'sorter', text: 'x', createdAt: new TS(NOW) });
    store.set(`${P}Charm_Custom_Orders/${line}`, { key: line, receiptId: rid, state: 'completed', updatedAtMs: NOW - i, stamps: [{ how: 'print', at: NOW - i, by: 'paul' }] });
    store.set(`${P}Charm_Pool/${line}_1`, { poolId: `${line}_1`, orderId: rid, state: 'pooled' });
    store.set(`${P}Charm_Nest_Run_Lines/run~${pad}`, { runId: 'run', json: '{}' });
    store.set(`${P}Brites_Orders/${rid}/messages/m${pad}`, { text: 'x' });   // a message under an order that was never written
  }
}

/* ── what is where ── */
const dbState = () => new Map([...store.entries()].map(([k, v]) => [k, canon(v)]));
const fileState = () => new Map([...blobs.entries()].map(([k, v]) => [k, v.buf.toString('base64') + '|' + (v.contentType || '')]));
const own = (kind, p) => Families.ownerOf(kind, p);
const isFamily = (kind, p) => { const o = own(kind, p); return !!o && o.kind === 'family'; };
// a shared document is the sandbox's only by its marks: the registry's idPrefix / flag / field; the rest of the document is production's
const sandboxDoc = (k, v) => { const o = own('firestore', k); if (!o || o.kind !== 'family') return false; const f = Families.server().find(x => x.key === o.key); if (f.store === 'shared' && f.field) return (v || {})[f.field] !== undefined; if (f.store === 'shared' && f.flag && !f.idPrefix) return Object.entries(f.flag).every(([a, b]) => (v || {})[a] === b); if (f.store === 'shared' && f.idPrefix) return Object.entries(f.flag || {}).every(([a, b]) => (v || {})[a] === b); return true; };

(async () => {
  /* ═══ 0 · the registry itself ═══ */
  const fam = Families.families(), keys = fam.map(f => f.key);
  assert.strictEqual(new Set(keys).size, keys.length, 'every family key is unique');
  for (const f of fam) assert(f.key && f.label && ['firestore', 'shared', 'doc', 'storage', 'browser'].includes(f.store), 'a family names key, label and a known store: ' + JSON.stringify(f));
  for (const k of ['Charm_Master_Index', 'Charm_Master_Files', 'Station_Activity', 'Efficiency_Daily', 'Station_Sessions']) assert(Families.protected().some(p => p.key === k), 'the keep list holds ' + k);
  for (const k of ['Charm_Master_Index', 'Charm_Master_Files', 'Station_Activity', 'Efficiency_Daily', 'Station_Sessions', 'Station_Live', 'Laser_Sheet_Times', 'config']) assert(!Families.server().some(f => f.key === k), k + ' is never a family the wipe clears');
  assert(Families.server().filter(f => f.store === 'firestore').every(f => !/^Sandbox_/.test(f.key)), 'a family key is the name without the prefix');

  /* ═══ 1 · production first: its fixture, then the whole session as production ═══ */
  const seedProd = {
    'Charm_Master_Index/BR-TST-01': { sku: 'BR-TST-01', name: 'Compass', version: 3 }, 'Charm_Master_Index/BR-TST-02': { sku: 'BR-TST-02', name: 'Rose' },
    'Charm_Master_Files/BR-TST-01~a': { sku: 'BR-TST-01', path: 'charmnest/master/BR-TST-01/a.ai' },
    'config/stationAdmins': { v: 1, admins: [] }, 'config/charmNestPartials': { v: 1, n: 4, policies: { silver: { mode: 'auto' } } }, 'config/etsyOauth': { access_token: 'x', expires_at: 1 },
    'Station_Activity/activity-prod-1': { id: 'activity-prod-1', person: 'Paul', day: '2026-10-08', station: 'sorting', action: 'scan', at: NOW - 86400000 }, 'Efficiency_Daily/2026-10-08__Paul': { day: '2026-10-08', person: 'Paul', events: 4 },
    'Station_Sessions/session-prod-1': { id: 'session-prod-1', person: 'Paul', station: 'sorting', startAt: NOW - 90000000, endAt: NOW - 86400000 }, 'Station_Live/sorting__sorting-1__Paul': { id: 'x', person: 'Paul', beatAt: NOW }, 'Laser_Sheet_Times/sheet-x__1': { sheetId: 'x', person: 'Cara', ms: 90000 },
    'Station_Rev/employee': { act: 4, ses: 2, live: 1, at: NOW }, 'Charm_Nest_Rev/library': { n: 41, at: NOW }, 'Charm_Nest_Rev/remnants': { n: 3 },
    'EtsyMail_Config/etsyApiCounters': { day: '2026-10-09', total: 3500 }, 'EtsyMail_Listings/1718000': { images: ['https://img.etsystatic.com/a.jpg'] }, 'EtsyMail_OrderLinkMeta/bell': { n: 7, atMs: NOW - 5000 },
    'Charm_Sandbox/pulls': { day: '2026-10-09', pulls: 2, calls: 6, starts: ['s1', 's2'] },
    'Charm_Nest_Jobs/job-prod-1': { id: 'job-prod-1', sheetId: 'prod-sheet', status: 'done' }, [`Charm_Nest_CustomRead/${A}_10010`]: { reads: { h1: { kind: 'chainOnly' } }, latest: 'h1', order: A, updatedAt: new TS(NOW - 9000) },
    'Charm_Nest_Library/c0001': { id: 'c0001', name: 'Compass' }, 'Charm_Nest_Calibration/cal-1': { sheetId: 'old', density: 0.7 },
    'Brites_Orders/9999999999': { 'Staff Note': 'production note' }, 'Charm_Sku_NoDesign/prod-1': { pattern: '^PROD_' }
  };
  for (const [k, v] of Object.entries(seedProd)) store.set(k, v);
  for (const f of ['charmnest/master/BR-TST-01/a.ai', 'charmnest/sandbox/master/BR-TST-02/b.ai', 'charmnest/sheets/2026-09-29/prod.ai', 'charmnest/agent/agent-y.json', 'design-archive/listing/aa.jpg', 'charmnest/custom/4170000001/original.pdf']) blobs.set(f, { buf: Buffer.from('prod:' + f) });
  await session(false); bulk(false, 500);
  const mailBefore = [...store.keys()].filter(k => k.startsWith('EtsyMail_OrderLinks/ol_')).length;
  assert(mailBefore >= 1, 'production holds its own customer message (ol_…)');
  const prodDocs = dbState(), prodFiles = fileState();
  assert(prodDocs.size > 80, 'production holds a real session: ' + prodDocs.size + ' documents');

  /* ═══ 2 · the same session as the sandbox: every write is logged and classified ═══ */
  writes.length = 0; fileWrites.length = 0;
  await session(true); bulk(true, 500);
  const touched = new Set(), unknown = [];
  for (const w of writes) { const o = own('firestore', w.path); if (!o) unknown.push(w.path); else touched.add(o.key); }
  const fileTouched = new Set(), unknownFiles = [];
  for (const w of fileWrites) { const o = own('storage', w.path); if (!o) unknownFiles.push(w.path); else fileTouched.add(o.key); }
  assert.deepStrictEqual([...new Set(unknown)].sort(), [], 'the sandbox wrote a Firestore document the registry does not name (add the family to charm-nest-sandbox-families.js and clear it in the wipe): ' + [...new Set(unknown)].slice(0, 8).join(', '));
  assert.deepStrictEqual([...new Set(unknownFiles)].sort(), [], 'the sandbox wrote a file the registry does not name: ' + [...new Set(unknownFiles)].slice(0, 8).join(', '));
  // every family of the registry is exercised (a family the session never writes is a family the guard does not test)
  const present = k => { const f = Families.server().find(x => x.key === k); return touched.has(k) || fileTouched.has(k) || [...store].some(([p, v]) => own('firestore', p) && own('firestore', p).key === k && sandboxDoc(p, v)) || [...blobs.keys()].some(p => own('storage', p) && own('storage', p).key === k); };
  const unexercised = Families.server().filter(f => !present(f.key)).map(f => f.key);
  assert.deepStrictEqual(unexercised, [], 'the guard session wrote nothing in these registry families, drive them in session(): ' + unexercised.join(', '));
  // the protected stores the sandbox session wrote (employee efficiency, counters): they are kept, and listed in the output
  const keptWritten = [...new Set(writes.map(w => own('firestore', w.path)).filter(o => o && o.kind === 'protected').map(o => o.key))].sort();
  // every op of the library: driven, or named with its reason and scanned for a write
  const OPS = Object.keys(lib.ops);
  const READ_ONLY = new Set(['ping', 'lookupCharms', 'listCharms', 'listSheets', 'getSheet', 'listingPhotos', 'getShapeGuidance', 'laserStatus', 'laserDoneList', 'getCalibration', 'jobList', 'getJob', 'getAgent', 'poolList', 'poolGet', 'backList', 'backPreview', 'setGet', 'setList', 'runGet', 'runList', 'history', 'releaseGet', 'cancelCheck', 'timelineGet', 'listingSkus', 'aliasGet', 'noDesignGet', 'optionMapGet', 'customReadGet',
    'customSheetGet', 'customGet', 'sandboxStatus', 'roseGet', 'roseList', 'remnantList', 'partialList', 'partialPolicyGet', 'partialPlan', 'partialStocks', 'sheetHistory', 'partialSearchList', 'masterGet', 'masterGetMany', 'masterList', 'masterListFiles', 'flowState', 'sharedOrders', 'getOrderPieces', 'laserDoneList', 'sessionsList']);
  const REPO = { masterPutIndex: 'the Charm repo (shared by design, never wiped)', masterPatch: 'the Charm repo', masterPutFile: 'the Charm repo', masterRemoveFile: 'the Charm repo', masterRemoveSku: 'the Charm repo', startMaster: 'the Charm repo indexing job' };
  const NOT_SANDBOX = { putCharms: 'skipped in the sandbox (shared library)', renameCharm: 'skipped in the sandbox', putCalibration: 'skipped in the sandbox', cancelSweep: 'refused in the sandbox (sandboxCancel is its twin)', sandboxPut: 'retired (410)', sandboxPullOrders: 'ETSYPULL: its own test (tests/charm-nest/sandbox-pull.cjs) drives it against a fake Etsy; it writes Charm_Sandbox/current, stream, pulls and orders-pull/*', sandboxReset: 'the wipe itself', purgeHistory: 'ends in the wipe (phase 5)', restoreSheet: 'rebuilds a record from the files of a run (needs a nest report)' };
  const writeToken = /\b(?:tx|t|batch|ref|r|w)\.(?:set|update|create|delete)\(|\.doc\([^)]*\)\.(?:set|update|create|delete)\(|\.add\(\{|\.commit\(|\.save\(|db\.batch\(|stampTimeline|Timeline\.add\(|stamp\(|OrderCancel\.put\(|bump[A-Za-z]*\(/;   // (a Firestore write written out in the op's own code; a Map's set() is not one)
  const sources = ['charmNestLibrary.js', '_charmNestRoseStock.js', '_charmNestRemnants.js'].map(f => fs.readFileSync(path.join(fnDir, f), 'utf8'));
  const body = name => { for (const src of sources) { const m = new RegExp(`(?:async )?function (?:op_)?${name}\\s*\\(|OPS\\.${name}\\s*=\\s*(?:async )?`).exec(src); if (m) { const rest = src.slice(m.index + 1), end = rest.search(/\n(?:  )?(?:async )?function |\n(?:const|let|exports|OPS)[ .]/); return rest.slice(0, end > 0 ? end : 6000); } } return null; };
  const classed = [], unclassified = [], writers = [];
  for (const op of OPS) {
    if (driven.has(op)) continue;
    if (op in REPO || op in NOT_SANDBOX) { classed.push(op); continue; }
    if (!READ_ONLY.has(op)) { unclassified.push(op); continue; }
    const src = body(op); if (src && writeToken.test(src)) { writers.push(op); continue; }
    const r = await post({ op, sandbox: true });   // (a read called bare in the sandbox answers or refuses, and writes nothing: the next check)
    classed.push(op);
  }
  assert.deepStrictEqual(unclassified, [], `new op(s) neither driven by the guard session nor classified: drive each in session() (so the wipe is tested against what it writes) or name it in READ_ONLY / REPO / NOT_SANDBOX with a reason`);
  assert.deepStrictEqual(writers, [], `op(s) classified read-only whose code writes: drive each in session()`);
  const stale = [...READ_ONLY, ...Object.keys(REPO), ...Object.keys(NOT_SANDBOX)].filter(op => !OPS.includes(op) && !['sessionsList', 'flowState'].includes(op));
  assert.deepStrictEqual(stale, [], 'classified ops that no longer exist: ' + stale.join(', '));
  const bare = writes.filter(w => !touched.has((own('firestore', w.path) || {}).key)).length;
  assert.strictEqual(bare, 0, 'a read-only op wrote');

  /* ═══ 3 · sandboxStatus counts every family (read-only) ═══ */
  const nW = writes.length, nF = fileWrites.length;
  const before = await post({ op: 'sandboxStatus', sandbox: true });
  assert(before.status === 200 && before.body.ok, JSON.stringify(before.body).slice(0, 200));
  assert(writes.length === nW && fileWrites.length === nF, 'sandboxStatus is read-only');
  const serverKeys = Families.server().map(f => f.key);
  for (const k of serverKeys) assert(k in before.body.records, 'the status counts the family ' + k);
  const seeded = serverKeys.filter(k => before.body.records[k] > 0 || (k === 'Charm_Sandbox' && before.body.records[k] > 0));
  assert(seeded.length === serverKeys.length, 'every family holds something before the wipe: ' + serverKeys.filter(k => !(before.body.records[k] > 0)).join(', '));
  assert(before.body.kept && before.body.kept.charmRepo === 2 && before.body.kept.efficiencyDays >= 1 && typeof before.body.kept.efficiencySandboxDays === 'number', 'the status says what is kept: ' + JSON.stringify(before.body.kept));
  // the listing words (op listingKindPut, ADDONCUSTOM): the sandbox wrote its OWN copy of the alias document, production's is the shared one; a cleared word leaves no field behind
  { const mine = store.get('Sandbox_Charm_Sku_Aliases/1718000'), shared = store.get('Charm_Sku_Aliases/1718000'), gone = store.get('Sandbox_Charm_Sku_Aliases/1718001');
    assert(mine && mine.listingKind && mine.listingKind.kind === 'custom' && mine.listingKind.title === 'Add a Lion Charm', 'the sandbox keeps its own listing word: ' + JSON.stringify(mine));
    assert(shared && shared.listingKind && shared.listingKind.kind === 'custom' && shared.listingKind.title === 'Add a Triceratops Charm', 'production keeps the shared listing word, not the sandbox\'s: ' + JSON.stringify(shared));
    assert(gone && !('listingKind' in gone), 'a cleared listing word leaves no field: ' + JSON.stringify(gone)); }
  const fullDbBefore = dbState(), fullFilesBefore = fileState();
  const sandboxDocsBefore = [...store].filter(([k, v]) => isFamily('firestore', k) && sandboxDoc(k, v)).length, sandboxFilesBefore = [...blobs.keys()].filter(k => isFamily('storage', k)).length;
  assert(sandboxDocsBefore > 60 && sandboxFilesBefore >= 4, `the sandbox holds a real session: ${sandboxDocsBefore} documents, ${sandboxFilesBefore} files`);

  /* ═══ 4 · THE WIPE: the real op, called until it is done (a commit takes 1.5 s of the test's clock, so one call cannot finish) ═══ */
  Fk.setSlow(1500);
  let r = await live({ op: 'sandboxReset', sandbox: true }), calls = 1; const firstAnswer = r.body;
  assert.strictEqual(r.status, 200, JSON.stringify(r.body));
  while (r.body.more && calls < 300) { r = await live({ op: 'sandboxReset', sandbox: true }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); calls++; }
  Fk.setSlow(0);
  assert(firstAnswer.more === true && calls > 2, 'a wipe that runs out of time says so, for the page to call again: ' + JSON.stringify(firstAnswer) + ' · ' + calls + ' calls');
  assert(r.body.more === false && !r.body.filesError && r.body.ok === true, 'the calls finish the wipe: ' + JSON.stringify(r.body));
  assert.deepStrictEqual(Object.keys(r.body).sort(), ['deleted', 'files', 'filesError', 'more', 'ok'], 'the answer keeps its shape');
  // (a) nothing of the sandbox is left
  const leftDocs = [...store].filter(([k, v]) => isFamily('firestore', k) && sandboxDoc(k, v)).map(([k]) => k);
  assert.deepStrictEqual(leftDocs.slice(0, 10), [], 'sandbox documents left after the wipe (' + leftDocs.length + ')');
  const leftFiles = [...blobs.keys()].filter(k => isFamily('storage', k));
  assert.deepStrictEqual(leftFiles, [], 'sandbox files left after the wipe');
  for (const f of Families.server()) {   // each family of the registry, counted by its own rule
    if (f.store === 'firestore') assert(![...store.keys()].some(k => k.startsWith('Sandbox_' + f.key + '/')), 'nothing left in Sandbox_' + f.key);
  }
  // (b) the whole database is what it was before the sandbox session, but the keep list: production byte-identical, protected untouched or only grown
  const after = dbState(), problems = [];
  for (const [k, v] of prodDocs) {
    const o = own('firestore', k);
    if (!after.has(k)) problems.push('production lost ' + k);
    else if (after.get(k) !== v && !(o && o.kind === 'protected')) problems.push('production changed ' + k);
  }
  for (const [k, v] of after) {
    if (prodDocs.has(k)) continue;
    const o = own('firestore', k);
    if (!(o && o.kind === 'protected')) problems.push('a document the sandbox added and the wipe left: ' + k);
  }
  assert.deepStrictEqual(problems.slice(0, 12), [], 'after the wipe: ' + problems.length + ' difference(s) from the state before the sandbox session');
  // the protected stores: the efficiency the sandbox wrote is whole (never trimmed), production's own is byte-identical, and what the wipe leaves of the shared counters is only counters
  for (const [k, v] of fullDbBefore) { const o = own('firestore', k); if (o && o.kind === 'protected') assert.strictEqual(after.get(k), v, 'the wipe left protected ' + k + ' as it was'); }
  for (const k of ['Charm_Sku_Aliases/1718000', 'Charm_Sku_Aliases/1718001']) assert.strictEqual(after.get(k), fullDbBefore.get(k), 'the wipe left production\'s alias document ' + k + ' (with its listing word) byte-identical');
  assert(after.has('Charm_Sku_Aliases/1718000') && ![...after.keys()].some(k => k.startsWith('Sandbox_Charm_Sku_Aliases/')), 'production\'s listing word is still there, the sandbox\'s own copies are gone');
  for (const k of ['Sandbox_Station_Activity/activity-guard-0001', 'Sandbox_Efficiency_Daily', 'Sandbox_Station_Sessions/session-guard-0001', 'Charm_Sandbox/pulls', 'Charm_Master_Index/BR-TST-01']) assert([...after.keys()].some(x => x.startsWith(k)), 'kept: ' + k);
  const after2 = fileState(), fileProblems = [];
  for (const [k, v] of prodFiles) if (after2.get(k) !== v) fileProblems.push('a file of production/the repo changed or went: ' + k);
  for (const k of after2.keys()) if (!prodFiles.has(k)) fileProblems.push('a file the sandbox added and the wipe left: ' + k);
  assert.deepStrictEqual(fileProblems, [], 'files after the wipe');
  // the shared line reading keeps production's own record whole and the sandbox's decision is gone (a line only the sandbox decided is gone whole)
  const read = store.get(`Charm_Nest_CustomRead/${A}_10010`);
  assert(read && !('decidedSandbox' in read) && read.decided.kind === 'chainOnly' && read.reads.h1.kind === 'chainOnly' && canon(read) === prodDocs.get(`Charm_Nest_CustomRead/${A}_10010`), "the sandbox's decision goes, production's reading stays as it was");
  assert(!store.has('Charm_Nest_CustomRead/4170000888_50050'), 'a reading record only the sandbox made goes whole');
  assert(!store.has('Charm_Sandbox/current') && !store.has('Charm_Sandbox/stream') && store.has('Charm_Sandbox/pulls'), 'the orders it played and the stream go, the pull budget stays');
  // (c) the status after: every family zero; what is kept is counted
  const nW2 = writes.length;
  const status = await post({ op: 'sandboxStatus', sandbox: true });
  assert(writes.length === nW2, 'the status after the wipe wrote nothing');
  for (const k of serverKeys) assert.strictEqual(status.body.records[k], 0, 'the status counts 0 in ' + k + ': ' + status.body.records[k]);
  assert.strictEqual(status.body.snapshot, null, 'no order set is left');
  assert(status.body.kept.charmRepo === 2 && status.body.kept.efficiencyDays >= 1 && status.body.kept.efficiencySandboxDays >= 1, 'the Charm repo and the efficiency are counted as kept: ' + JSON.stringify(status.body.kept));
  // (d) a second press finds nothing; a late write from an old tab is cleared by the next press, and production is as it was
  const again = await post({ op: 'sandboxReset', sandbox: true });
  assert(again.status === 200 && again.body.more === false && again.body.deleted === 0 && again.body.files === 0, 'a second wipe finds nothing: ' + JSON.stringify(again.body));
  await must({ op: 'customPut', key: `${A}_10010`, by: 'paul', receiptId: A }, true); await must({ op: 'cancelPut', orderId: C, by: 'paul' }, true);
  await fileDoor(true, 'put', { path: 'charmnest/sheets/late.png', contentType: 'image/png', base64: 'AAAA' });
  assert(store.has(`Sandbox_Charm_Custom_Orders/${A}_10010`) && blobs.has('charmnest/sandbox/sheets/late.png'), 'a late write lands');
  const late = await post({ op: 'sandboxReset', sandbox: true });
  assert(late.body.more === false && ![...store.keys()].some(k => isFamily('firestore', k) && sandboxDoc(k, store.get(k))) && ![...blobs.keys()].some(k => isFamily('storage', k)), 'pressed again, the wipe clears it');
  assert.strictEqual(dbState().get(`Charm_Custom_Orders/${A}_10010`), prodDocs.get(`Charm_Custom_Orders/${A}_10010`), 'and production is as it was');

  /* ═══ 5 · Purge all run history ends in the same wipe ═══ */
  await session(true);
  store.set('Sandbox_Charm_Nest_Runs/run-open', { runId: 'run-open', status: 'running', updatedAt: new TS(Date.now()) });
  let p = await post({ op: 'purgeHistory', code: 'wrong', sandbox: true });
  assert(p.status === 403 && /passcode/.test(p.body.error), 'a wrong passcode purges nothing');
  const whole = canon([...store.entries()].sort(([a], [b]) => (a < b ? -1 : 1)));
  p = await post({ op: 'purgeHistory', code: process.env.CHARM_NEST_DELETE_CODE, sandbox: true });
  assert(p.status === 409 && /a run is still open/.test(p.body.error) && canon([...store.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) === whole, 'an open run is named first, nothing is purged: ' + JSON.stringify(p.body));
  Fk.setSlow(1500);
  p = await live({ op: 'purgeHistory', code: process.env.CHARM_NEST_DELETE_CODE, force: true, sandbox: true });
  assert(p.status === 503 && p.body.more === true && /press Purge again/.test(p.body.error), 'a purge that runs out of time is a failure the page shows: ' + JSON.stringify(p).slice(0, 200));
  let purges = 1; while (p.body.more && purges < 300) { p = await live({ op: 'purgeHistory', code: process.env.CHARM_NEST_DELETE_CODE, force: true, sandbox: true }); purges++; }
  Fk.setSlow(0);
  assert(p.status === 200 && p.body.ok && !p.body.more, 'forced, the purge runs until it is done: ' + JSON.stringify(p.body).slice(0, 200));
  const leftP = [...store].filter(([k, v]) => isFamily('firestore', k) && sandboxDoc(k, v)).map(([k]) => k);
  assert.deepStrictEqual(leftP.slice(0, 8), [], 'after the purge the sandbox holds nothing');
  assert.deepStrictEqual([...blobs.keys()].filter(k => isFamily('storage', k)), [], 'and no sandbox file');
  for (const k of ['Charm_Sandbox/pulls', 'Charm_Master_Index/BR-TST-01', 'Station_Activity/activity-prod-1', 'Efficiency_Daily/2026-10-08__Paul']) assert(store.has(k), 'the purge kept ' + k);
  assert(store.has(`Charm_Custom_Orders/${A}_10010`) && store.has('Charm_Nest_Cancelled/' + C), "production's seals, custom orders and cancel records are permanent");

  const nested = warnings.filter(w => /Nested arrays/.test(w));
  assert.deepStrictEqual(nested, [], 'no write was refused for a nested array');
  console.warn = realWarn; console.error = realError;
  console.log(`guard OK: ${Families.server().length} server families, ${OPS.length} ops (${driven.size} driven, ${classed.length} classified), ${sandboxDocsBefore} sandbox documents and ${sandboxFilesBefore} files wiped in ${calls} call(s)`);
  console.log(`  protected stores the sandbox session also wrote (kept): ${keptWritten.join(', ')}`);
  if (issues.length) console.log('  tried and answered with a refusal (the arguments need fixtures this offline session does not have):\n   - ' + issues.join('\n   - '));
})().catch(e => { console.warn = realWarn; console.error = realError; console.error(e); process.exit(1); });
