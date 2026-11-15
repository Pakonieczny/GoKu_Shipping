/* ARTEVERYWHERE (Paul, 15 Nov 2026): a whole drawing replaced (outline removed, another design's cut line added) is kept as the outline's change; artPut names p:<order> and s:<sheet> in the feed.
   node tests/charm-nest/art-everywhere.cjs */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path');
const { start } = require('./bridge-server.cjs');
const ROOT = path.join(__dirname, '..', '..');
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js')), G = global.CharmNestGeom;
const sq = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];
(async () => {
  const cal = await require('./_artCalendar.cjs')(ROOT), base = cal.charm, ids = AE.memberIds(base);
  const front = G.backView(base, { face: 'front', upAngle: 60, res: 6 }), fr = AE.frame(front);
  const toItem = subs => subs.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(fr.toItem)))));
  const piece = Object.assign({}, base, { id: 's:4170000001_1001_1', poolId: '4170000001_1001_1' });
  const b = base.bbox, cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2, w = (b[2] - b[0]) * 0.8, h = (b[3] - b[1]) * 0.8;
  const changes = { removed: ids.slice(), changed: {}, added: [
    { id: 'newcut', kind: 'path', role: 'cut', stroke: '#000000', subpaths: toItem(sq(cx - w / 2, cy - h / 2, w, h)) },
    { id: 'newart', kind: 'path', role: 'hatch', fill: '#000000', subpaths: toItem(sq(cx - 2, cy - 2, 4, 4)) }] };
  const rec = AE.encode(changes, { charm: piece, view: front, face: 'front', placed: false });
  assert.ok(rec.cut && rec.changed[ids[base.members.indexOf(base.outline)]], 'replacement: the new cut line is the outline changed');
  assert.ok(rec.removed.length === ids.length - 1 && !rec.removed.includes(ids[base.members.indexOf(base.outline)]), 'every other part removed');
  assert.deepEqual(rec.added.map(x => x.id), ['newart'], 'the other new part added');
  const out = AE.apply(piece, Object.assign({ at: 5, by: 'Ana_M', face: 'front' }, rec), 'front');
  assert.ok(out.hash !== base.hash && out.artEdit.front && !out.artEdit.front.refused.length && out.artTrace === true, 'a new charm (own hash), nothing refused, shape traced again');
  // no cut line added: the outline stays refused
  assert.throws(() => AE.encode({ removed: ids.slice() }, { charm: piece, view: front, face: 'front' }), /outline is the piece itself/);
  // the feed
  const srv = await start({ receipts: [], strictArrays: true }), st = srv.st;
  try {
    const call = async body => { const o = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: o.statusCode, body: JSON.parse(o.body || '{}') }; };
    const ID = '4170000001_10010_1', SHEET = 'gold-art-1';
    st.put('Charm_Pool', ID, { poolId: ID, orderId: '4170000001', sku: 'CALENDAR', state: 'written', sheetId: SHEET });
    st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', poolIds: [ID] });
    let n0 = (await call({ op: 'changes' })).body.n;
    const r = await call({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M', art: { baseKey: 'k1a2b3', removed: ['1-abc'], changed: {}, added: [] } });
    assert.equal(r.status, 200, JSON.stringify(r.body));
    const k = (await call({ op: 'changes', since: n0 })).body.keys || [];
    assert.ok(k.includes('p:4170000001') && k.includes('s:' + SHEET) && !k.includes('p:*'), 'feed names the order and the sheet: ' + k.join());
  } finally { srv.close(); }
  console.log('art-everywhere: ok');
})().catch(e => { console.error(e); process.exit(1); });
