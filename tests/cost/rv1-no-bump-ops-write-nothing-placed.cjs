// RV1 guard for FC3's placement counter (Charm_Nest_Rev/placement): the ops named in NO_GEN_BUMP in charmNestLibrary.js do NOT raise it, so none of
// them may change a field that a getOrderPieces answer is made from (which pieces a sheet lists, its orders, whether it is cut or completed or on hold,
// its set, draft / solid / archived state, its run, status, files and position), nor any pool row. A future edit of a read op (a repair on read, a
// back-fill, a new "record on look" like laserStatus' seals) that writes one of those would show only at the page's once-a-minute full read: this test
// fails then, and the op must be taken off NO_GEN_BUMP (it then raises the counter) or must not write that field.
// What these ops legitimately write (checked here, not forbidden): laserStatus' seals (processSeals, processReady, step stamps, updatedAt), findSheets'
// `listings` back-fill, laserDoneList's keeping of an archived sheet's mark aside, sheetPdf's outputs.pdf.
//   node tests/cost/rv1-no-bump-ops-write-nothing-placed.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs');
const meter = require('./meter.cjs');
const W = path.join(__dirname, '..', '..');
const T = 1.7e12;
// the fields of a sheet that readOrderPieces / the page's reading of it use (PIECE_SHEET_FIELDS without the update stamps), and every pool row field
const SHEET_PLACED = ['sheetId', 'metal', 'metalLabel', 'fileBase', 'folder', 'sheetIndex', 'page', 'setId', 'setSeq', 'poolIds', 'orders', 'archived', 'laserDoneAt', 'roseCutAt', 'draft', 'solidIncluded', 'status', 'runId', 'laserHold', 'createdAt'];
const same = (a, b) => JSON.stringify(a === undefined ? null : a) === JSON.stringify(b === undefined ? null : b);

(async () => {
  const src = fs.readFileSync(path.join(W, 'netlify/functions/charmNestLibrary.js'), 'utf8');
  const listed = (src.match(/const NO_GEN_BUMP = new Set\(\[([\s\S]*?)\]\)/)[1].match(/"(\w+)"/g) || []).map(s => s.replace(/"/g, ''));
  assert.ok(listed.length > 40 && listed.includes('laserStatus') && listed.includes('flowState'), 'the list was read');

  const m = meter.create(); m.install();
  const docs = {};
  // a completed set of two sheets, an archived completed sheet (laserDoneList keeps its mark aside), an open set with a ready-looking sheet, a sheet with no listings,
  // pool rows, a run
  docs['Charm_Nest_Sets/set-1'] = { setId: 'set-1', seq: 1, sheetIds: ['sh-1', 'sh-2'], laserDoneAt: T, status: 'complete', updatedAt: T };
  for (const id of ['sh-1', 'sh-2']) docs['Charm_Nest_Sheets/' + id] = { id, setId: 'set-1', laserDoneAt: T, laserDoneBy: 'Ana', metal: 'gold', archived: false, updatedAt: T, orders: ['4190000001'], poolIds: ['4190000001_41900000011_1'], folder: id, fileBase: id, sheetIndex: 1, listings: [] };
  docs['Charm_Nest_Sheets/sh-arch'] = { id: 'sh-arch', setId: 'set-1', laserDoneAt: T + 5, laserDoneBy: 'Ana', metal: 'gold', archived: true, updatedAt: T, orders: ['4190000002'], poolIds: [] };
  docs['Charm_Nest_Sets/set-2'] = { setId: 'set-2', seq: 2, sheetIds: ['sh-3'], status: 'open', updatedAt: T };
  docs['Charm_Nest_Sheets/sh-3'] = { id: 'sh-3', setId: 'set-2', metal: 'gold', archived: false, updatedAt: T, orders: ['4190000003'], poolIds: ['4190000003_41900000031_1'], folder: 'sh-3', fileBase: 'sh-3', sheetIndex: 1, placements: [{ id: 'a', x: 1, y: 1 }], placedCount: 1, charmCount: 1, outputs: { preview: { url: 'x' } } };
  docs['Charm_Pool/4190000001_41900000011_1'] = { poolId: '4190000001_41900000011_1', orderId: '4190000001', state: 'written', sheetId: 'sh-1', setId: 'set-1', updatedAt: T };
  docs['Charm_Pool/4190000003_41900000031_1'] = { poolId: '4190000003_41900000031_1', orderId: '4190000003', state: 'written', sheetId: 'sh-3', setId: 'set-2', updatedAt: T };
  docs['Charm_Nest_Runs/run-1'] = { runId: 'run-1', status: 'complete', orders: ['4190000001'], updatedAt: T };
  m.db.seed(docs);

  // every change of a document, in the four families the placement answer is made from (and the set / run copies)
  const changes = [];
  const before = new Map();
  const watch = k => /^(Charm_Pool|Charm_Nest_Sheets|Charm_Nest_Sets|Charm_Pool_Back|Charm_Nest_Runs)\//.test(k);
  const rawSet = m.db.docs.set.bind(m.db.docs), rawDel = m.db.docs.delete.bind(m.db.docs);
  let current = null;
  m.db.docs.set = (k, v) => { if (watch(k)) { const old = m.db.docs.get(k); changes.push({ op: current, k, old: old ? JSON.parse(JSON.stringify(old)) : null, now: JSON.parse(JSON.stringify(v)) }); } return rawSet(k, v); };
  m.db.docs.delete = k => { if (watch(k)) changes.push({ op: current, k, old: m.db.docs.get(k) || null, now: null }); return rawDel(k); };

  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const post = async body => { current = body.op + (body.recordSeals ? '+recordSeals' : ''); const r = await fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
  const bodies = {
    laserStatus: [{ sheetIds: ['sh-1', 'sh-2', 'sh-3'], recordSeals: true, by: 'Ana' }, { sheetIds: ['sh-1', 'sh-3'] }],
    flowState: [{}, { sheetIds: ['sh-1', 'sh-3'] }],
    listSheets: [{}, { excludeDone: true }], getSheet: [{ id: 'sh-3' }, { id: 'sh-1' }],
    laserDoneList: [{ kind: 'sheets' }, { kind: 'sets' }, { countOnly: true }, { kind: 'sheets', sort: 'activity' }],
    findSheets: [{ q: '4190000001' }, { orderId: '4190000003' }, { q: 'sh-3' }],
    history: [{}, { sort: 'activity' }], sharedOrders: [{ orderIds: ['4190000001', '4190000003'] }],
    poolList: [{ orderId: '4190000001' }, { sheetId: 'sh-3' }], poolGet: [{ poolIds: ['4190000001_41900000011_1'] }], backList: [{ poolIds: ['4190000001_41900000011_1'] }],
    setGet: [{ setId: 'set-1' }], setList: [{}, { excludeDone: true }], runGet: [{ runId: 'run-1' }], runList: [{}], cancelList: [{}, { idsOnly: true, after: { s: 1, n: 0, id: '1' } }], cancelCheck: [{ orderIds: ['4190000001'] }],
    getOrderPieces: [{ orderIds: ['4190000001', '4190000003'] }], sessionsList: [{}], laserSheetLast: [{ by: 'Ana' }], customGet: [{ items: [] }], customSheetGet: [{ keys: [] }],
    sheetPdf: [{ ids: ['sh-3'] }], backPreview: [{ poolIds: ['4190000001_41900000011_1'] }], ping: [{}], getCalibration: [{}], getJob: [{ id: 'x' }], jobList: [{}], masterList: [{}], masterGet: [{ sku: 'x' }],
    remnantList: [{}, { scope: 'all' }], partialList: [{ metal: 'rose' }, { metal: 'gold10k', inUse: true, used: true }], partialPolicyGet: [{}], partialPolicySet: [{ metal: 'rose', mode: 'auto', by: 'Ana' }], partialPlan: [{ metal: 'rose', pieces: { areaMm2: 500, count: 5 } }], partialBackfill: [{}], sandboxStatus: [{ light: true }], releaseGet: [{}], timelineGet: [{ orderId: '4190000001' }], aliasGet: [{}], noDesignGet: [{}], optionMapGet: [{}], lookupCharms: [{ hashes: [] }], listCharms: [{}]
  };
  const ran = [], skipped = [];
  for (const op of listed) {
    const list = bodies[op];
    if (!list) { skipped.push(op); continue; }
    for (const b of list) { await post(Object.assign({ op }, b)); ran.push(op); }
  }
  // placement-relevant changes: a pool row at all, or a sheet / set / run field the answer is made from
  const bad = [];
  for (const c of changes) {
    const kind = c.k.split('/')[0];
    if (kind === 'Charm_Pool' || kind === 'Charm_Pool_Back') { bad.push(`${c.op} wrote ${c.k}`); continue; }
    if (kind === 'Charm_Nest_Sheets' || kind === 'Charm_Nest_Sets') {
      if (!c.old || !c.now) { bad.push(`${c.op} ${c.old ? 'deleted' : 'created'} ${c.k}`); continue; }
      const fields = kind === 'Charm_Nest_Sheets' ? SHEET_PLACED : ['sheetIds', 'laserDoneAt', 'status', 'orders', 'committedAt'];
      for (const f of fields) if (!same(c.old[f], c.now[f]) && !(c.op === 'laserDoneList' && /^Charm_Nest_Sheets\/sh-arch$/.test(c.k) && f === 'laserDoneAt')) bad.push(`${c.op} changed ${f} of ${c.k}: ${JSON.stringify(c.old[f])} -> ${JSON.stringify(c.now[f])}`);
    }
  }
  assert.deepStrictEqual(bad, [], 'ops that do not raise the placement counter changed what a placement answer is made from:\n  ' + bad.join('\n  '));
  const wrote = [...new Set(changes.map(c => `${c.op} -> ${c.k}`))];
  process.stdout.write(`ok   ${[...new Set(ran)].length} of the ${listed.length} no-bump ops ran on the fixture (${skipped.length} need other state: ${skipped.join(', ')}); none changed a placed field or a pool row\n`);
  process.stdout.write(`     what they did write (not placement): ${wrote.length ? wrote.join('; ') : 'nothing'}\n`);
  assert.ok(!m.db.docs.has('Charm_Nest_Rev/placement'), 'and none raised the counter (they are not supposed to)');
  process.stdout.write('rv1-no-bump-ops-write-nothing-placed: passed\n');
})().catch(e => { process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n'); process.exit(1); });
