/* ART EDIT (Paul, 27 Oct 2026): one piece's own artwork edit in the cloud — charmNestLibrary.js artPut / artGet / artList, run by the REAL handler over the in-memory Firestore of
   tests/charm-nest/bridge-server.cjs (start()). No live service, no credential, no network.
     A  checked strictly, and refused whole (nothing written): the pool id, the face, who, the drawing key, the ids, the roles, the path vocabulary and its numbers, the colours, the sizes
        (bytes, parts, operators), a part removed and changed at once, a cut id that is neither, an edit of the back that is not an addition, a piece that is not pooled
     B  saved: the record (in the charm's own points, as strings: no array inside an array), who and when, the pool row's stamp (the other face's kept), read back by artGet and by artList
        (by keys or by pool ids and faces; only the documents asked for are read); a second save: the previous one named, the count; an edit made elsewhere since is refused (409)
     C  a piece on a saved sheet: its sheet owes its files (filesOwed); a cut line is not changed on it (cutRefused) unless it is the same cut edit saved before it was nested; a sheet
        already cut by the laser is never changed; clear takes the edit off (the record gone, the stamp null)
     D  the sandbox: its own records (Sandbox_Charm_Pool_Art, Sandbox_ pool rows), production untouched and unseen
     E  reads move no counter (artGet / artList); a save raises the placement counter (it wrote a pool row)
     F  the page module (charm-nest-art-edit.js) against the same handler, with a stand-in for the page: save → the record, the pool row's stamp on the page, the piece's new charm on its
        sheet (swap), the face's engraving and the sheet's files (changed); a piece made from the master again (piece) reads its edit once and only when its pool row names one; a cut line
        of a placed piece refused before anything is sent; after a checkpoint, sync makes again only the pieces whose pool row names an edit their charm does not carry
   node tests/charm-nest/art-edit-server.cjs */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path');
const { start } = require('./bridge-server.cjs');
const ROOT = path.join(__dirname, '..', '..');
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js'));
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const nested = v => Array.isArray(v) ? v.some(x => Array.isArray(x) || nested(x)) : v && typeof v === 'object' ? Object.values(v).some(nested) : false;

(async () => {
  const srv = await start({ receipts: [], strictArrays: true }), st = srv.st;
  try {
    const call = async body => { const out = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: out.statusCode, body: JSON.parse(out.body || '{}') }; };
    const ID = '4170000001_10010_1', ID2 = '4170000002_10020_1', SHEET = 'gold-art-1';
    st.put('Charm_Pool', ID, { poolId: ID, orderId: '4170000001', sku: 'CALENDAR', state: 'pooled', sheetId: null, artEdit: { back: 111 } });
    st.put('Charm_Pool', ID2, { poolId: ID2, orderId: '4170000002', sku: 'CALENDAR', state: 'pooled', sheetId: null });
    const art = (extra) => Object.assign({ baseKey: 'k1a2b3', removed: ['1-abc', '2-def'], changed: { '5-xyz': { kind: 'path', role: 'line', d: 'M10 10L20 10', keep: true } }, added: [{ id: 'jun0', kind: 'text', role: 'art', text: 'J', d: 'M1 1L3 1L3 3C3 4 2 4 1 3Z', stroke: null, fill: [0, 0, 1], weightMm: 0.0254 }] }, extra || {});
    const put = (b, extra) => call(Object.assign({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M', art: art(b) }, extra || {}));
    const docs = () => st.list('Charm_Pool_Art').length + st.list('Sandbox_Charm_Pool_Art').length;

    /* ── A · refused whole ── */
    const bad = async (body, re, m) => { const before = docs(), r = await call(Object.assign({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M', art: art() }, body)); ok(r.status >= 400 && re.test(r.body.error || '') && docs() === before, `A ${m}: ${r.status} ${r.body.error}`); };
    await bad({ poolId: 'nope' }, /bad pool id/, 'a pool id that is none');
    await bad({ face: 'side' }, /front or back/, 'a face that is neither');
    await bad({ by: '  ' }, /signed-in person/, 'no one named');
    await bad({ art: art({ baseKey: '' }) }, /which master drawing/, 'no drawing key');
    await bad({ art: art({ removed: ['not an id'] }) }, /removed/, 'a removed id that is no part id');
    await bad({ art: art({ changed: { 'bad key': { role: 'line', d: 'M1 1', keep: true } } }) }, /changed/, 'a changed key that is no part id');
    await bad({ art: art({ removed: ['5-xyz'] }) }, /both removed and changed/, 'a part removed and changed at once');
    await bad({ art: art({ added: [{ id: 'a', role: 'engrave', d: 'M1 1L2 2', fill: [0, 0, 1] }] }) }, /role must be cut, line, hatch or art/, 'a role that is none');
    await bad({ art: art({ added: [{ id: 'a', kind: 'image', role: 'art', d: 'M1 1L2 2', fill: [0, 0, 1] }] }) }, /kind must be path or text/, 'a picture');
    for (const [d, why] of [['M1 1Q2 2 3 3', 'a quadratic'], ['L1 1', 'no M first'], ['M1', 'a number short'], ['M1 1L2 NaN', 'not a number'], ['M1 1L2 99999', 'out of range'], ['M1 1 <script>', 'another word'], ['', 'empty']])
      await bad({ art: art({ added: [{ id: 'a', role: 'art', d, fill: [0, 0, 1] }] }) }, /M \/ L \/ C \/ Z/, 'a path: ' + why);
    await bad({ art: art({ added: [{ id: 'a', role: 'art', d: 'M1 1L2 2', fill: [0, 0, 2] }] }) }, /colour/, 'a colour out of 0..1');
    await bad({ art: art({ added: [{ id: 'a', role: 'art', d: 'M1 1L2 2', fill: '#00f' }] }) }, /colour/, 'a colour as a word');
    await bad({ art: art({ added: [{ id: 'a', role: 'art', d: 'M1 1L2 2' }] }) }, /neither stroked nor filled/, 'no paint');
    await bad({ art: art({ added: [{ id: 'a', role: 'line', d: 'M1 1L2 2', stroke: [1, 0, 0], lwPt: 50 }] }) }, /line width/, 'a line 50 pt wide');
    await bad({ art: art({ added: [{ id: 'a', role: 'art', d: 'M1 1L2 2', fill: [0, 0, 1] }, { id: 'a', role: 'art', d: 'M1 1L2 2', fill: [0, 0, 1] }] }) }, /used twice/, 'an id twice');
    await bad({ art: art({ added: [{ id: 'a', kind: 'text', role: 'hatch', d: 'M1 1L2 2L2 1Z', fill: [0, 0, 1], weightMm: 9 }] }) }, /weight is -5 to 5 mm/, 'words 9 mm heavier (27 Oct 2026: a weight is a few hundredths of a mm)');
    await bad({ art: art({ added: [{ id: 'a', kind: 'text', role: 'hatch', d: 'M1 1L2 2L2 1Z', fill: [0, 0, 1], weightMm: 'bold' }] }) }, /weight/, 'a weight that is a word');
    await bad({ art: art({ added: [{ id: 'has space', role: 'art', d: 'M1 1L2 2', fill: [0, 0, 1] }] }) }, /short name/, 'an id that is no short name');
    await bad({ art: art({ added: [{ id: 'k', role: 'art', d: 'M1 1L2 2', keep: true }] }) }, /neither stroked nor filled/, 'keep on a NEW part (only a changed part keeps its paint)');
    await bad({ art: art({ cutIds: ['9-zzz'] }) }, /cutIds/, 'a cut id that is neither removed nor changed');
    await bad({ art: art({ added: Array.from({ length: 401 }, (_, i) => ({ id: 'p' + i, role: 'art', d: 'M1 1L2 2', fill: [0, 0, 1] })) }) }, /at most 400/, '401 new parts');
    const huge = 'M0 0' + 'L1.5 2.5'.repeat(40000);
    await bad({ art: art({ added: [{ id: 'big', role: 'art', d: huge, fill: [0, 0, 1] }] }) }, /KB|path/, 'a path of 320 KB (one path at most 200 KB)');
    await bad({ art: art({ added: Array.from({ length: 4 }, (_, i) => ({ id: 'b' + i, role: 'art', d: 'M0 0' + 'L1 2'.repeat(20000), fill: [0, 0, 1] })) }) }, /path operators/, '80 000 operators');
    await bad({ art: art({ added: Array.from({ length: 6 }, (_, i) => ({ id: 'c' + i, role: 'art', d: 'M0 0' + 'L1.123 2.456'.repeat(9000), fill: [0, 0, 1] })) }) }, /KB/, 'an edit over 600 KB');
    await bad({ face: 'back' }, /back shows only the cut lines/, 'a removal on the back');
    await bad({ poolId: '4179999999_99999_1' }, /not in the pool/, 'a piece that is not pooled');
    await bad({ art: null }, /missing/, 'no edit at all');
    eq(docs(), 0, 'A nothing was written by any refusal');

    /* ── B · saved ── */
    let r = await put();
    ok(r.status === 200 && r.body.ok && r.body.key === ID + '~front' && r.body.art && r.body.art.at > 0 && r.body.art.by === 'Ana_M', 'B saved: ' + JSON.stringify(r.body).slice(0, 160));
    const doc = st.doc('Charm_Pool_Art', ID + '~front');
    ok(doc && doc.poolId === ID && doc.face === 'front' && doc.by === 'Ana_M' && doc.at === r.body.art.at && doc.edits === 1 && doc.sku === 'CALENDAR' && doc.orderId === '4170000001' && doc.sheetId === null && !doc.prev, 'B the record: the piece, the face, who and when, the count');
    eq(doc.removed, ['1-abc', '2-def'], 'B its removals'); eq(Object.keys(doc.changed), ['5-xyz'], 'B its change'); ok(doc.changed['5-xyz'].keep === true && doc.added[0].fill.join() === '0,0,1' && doc.added[0].text === 'J', 'B paint as sent');
    ok(!nested(doc), 'B no array inside an array (the fake refuses them as Firestore does: strictArrays)');
    ok(doc.added[0].weightMm === 0.025 && !('weightMm' in doc.changed['5-xyz']), 'B the weight words were made at is kept with them, to the micron (27 Oct 2026)');
    eq(st.doc('Charm_Pool', ID).artEdit, { back: 111, front: doc.at }, 'B the pool row says the front has an edit, the back\'s stamp kept');
    r = await call({ op: 'artGet', poolId: ID, face: 'front' }); ok(r.status === 200 && r.body.art && r.body.art.at === doc.at && r.body.art.removed.length === 2, 'B artGet reads it back');
    r = await call({ op: 'artGet', poolId: ID, face: 'back' }); ok(r.status === 200 && r.body.art === null, 'B the back has none');
    const reads0 = st.reads; r = await call({ op: 'artList', keys: [ID + '~front', ID2 + '~front'] });
    ok(r.status === 200 && Object.keys(r.body.arts).join() === ID + '~front' && st.reads - reads0 === 2, 'B artList by keys: the two documents asked for are read (' + (st.reads - reads0) + '), the one there answered');
    r = await call({ op: 'artList', poolIds: [ID], faces: ['front'] }); ok(Object.keys(r.body.arts).join() === ID + '~front', 'B artList by pool ids and faces');
    r = await call({ op: 'artList', keys: Array.from({ length: 201 }, (_, i) => ID.replace(/_1$/, '_' + (i + 1))) }); ok(r.status === 400 && /at most 200/.test(r.body.error), 'B a read of more than 200 records is refused');
    // the next save names the one it replaces
    r = await put({ added: [] }, { expectedAt: 1 }); ok(r.status === 409 && r.body.conflict && /edited elsewhere/.test(r.body.error), 'B a save that expected another edit is refused (made elsewhere since): ' + r.body.error);
    const was = doc.at; r = await put({ added: [] }, { expectedAt: was });
    const doc2 = st.doc('Charm_Pool_Art', ID + '~front'); ok(r.status === 200 && doc2.edits === 2 && doc2.prev && doc2.prev.at === was && doc2.prev.by === 'Ana_M' && doc2.added.length === 0, 'B the second save: the count, the one before it named');
    r = await put({}, { expectedAt: 0, poolId: ID2 }); ok(r.status === 200 && st.doc('Charm_Pool_Art', ID2 + '~front'), 'B a first save may expect none (expectedAt 0)');

    /* ── C · on a saved sheet ── */
    st.put('Charm_Pool', ID, { state: 'written', sheetId: SHEET }); st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', fileBase: 'GF_Oct.27.26_Set-1_Sheet-1', poolIds: [ID], setId: null });
    r = await put({}, { expectedAt: doc2.at }); const sh = st.doc('Charm_Nest_Sheets', SHEET);
    ok(r.status === 200 && r.body.sheetId === SHEET && r.body.filesOwed > 0 && sh.filesOwed === r.body.filesOwed && st.doc('Charm_Pool_Art', ID + '~front').sheetId === SHEET, 'C an engraving edit of a piece on a saved sheet: saved, and the sheet owes its files (filesOwed)');
    let at = st.doc('Charm_Pool_Art', ID + '~front').at;
    r = await put({ added: [{ id: 'hole', role: 'cut', d: 'M1 1L2 1L2 2Z', stroke: [0, 0, 0], fill: null }] }, { expectedAt: at });
    ok(r.status === 409 && r.body.cutRefused && /nested with/.test(r.body.error) && /take the piece off its sheet/.test(r.body.error), 'C a cut line on a piece on a sheet is refused, saying what a re-nest needs: ' + r.body.error);
    r = await put({ removed: ['1-abc'], cutIds: ['1-abc'] }, { expectedAt: at }); ok(r.status === 409 && r.body.cutRefused, 'C a cut line removed: refused too');
    // a cut edit saved BEFORE the piece was nested stands, and the same edit sent again is no change
    r = await call({ op: 'artPut', poolId: ID2, face: 'front', by: 'Ana_M', expectedAt: st.doc('Charm_Pool_Art', ID2 + '~front').at, art: art({ added: [{ id: 'hole', role: 'cut', d: 'M1 1L2 1L2 2Z', stroke: [0, 0, 0], fill: null }] }) });
    const cutDoc = st.doc('Charm_Pool_Art', ID2 + '~front'); ok(r.status === 200 && cutDoc.cut === true && /^[0-9a-f]{16}$/.test(cutDoc.cutKey), 'C a cut edit of a piece not on a sheet yet is kept (cut, cutKey)');
    st.put('Charm_Pool', ID2, { state: 'written', sheetId: SHEET }); st.put('Charm_Nest_Sheets', SHEET, { poolIds: [ID, ID2] });
    r = await call({ op: 'artPut', poolId: ID2, face: 'front', by: 'Ana_M', expectedAt: cutDoc.at, art: art({ removed: ['1-abc'], added: [{ id: 'hole', role: 'cut', d: 'M1 1L2 1L2 2Z', stroke: [0, 0, 0], fill: null }] }) });
    ok(r.status === 200, 'C ... once nested, its engraving still changes while the cut edit is sent again unchanged');
    r = await call({ op: 'artPut', poolId: ID2, face: 'front', by: 'Ana_M', expectedAt: st.doc('Charm_Pool_Art', ID2 + '~front').at, art: art({ added: [{ id: 'hole', role: 'cut', d: 'M1 1L3 1L3 3Z', stroke: [0, 0, 0], fill: null }] }) });
    ok(r.status === 409 && r.body.cutRefused, 'C ... and a different cut is refused');
    r = await call({ op: 'artPut', poolId: ID2, face: 'front', by: 'Ana_M', clear: true }); ok(r.status === 409 && r.body.cutRefused, 'C taking off an edit that changed the cut of a nested piece is refused as well');
    // the laser cut it
    st.put('Charm_Nest_Sheets', SHEET, { laserDoneAt: Date.now() }); at = st.doc('Charm_Pool_Art', ID + '~front').at;
    r = await put({}, { expectedAt: at }); ok(r.status === 409 && /already cut by the laser/.test(r.body.error) && st.doc('Charm_Pool_Art', ID + '~front').at === at, 'C a sheet already cut by the laser: refused, nothing changed: ' + r.body.error);
    st.put('Charm_Nest_Sheets', SHEET, { laserDoneAt: null });
    // taken back to the master
    r = await call({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M', clear: true, expectedAt: at });
    ok(r.status === 200 && r.body.art === null && !st.doc('Charm_Pool_Art', ID + '~front'), 'C clear: the record is gone');
    eq(st.doc('Charm_Pool', ID).artEdit, { back: 111, front: null }, 'C and the pool row\'s front stamp is null (the back\'s kept)');

    /* ── D · the sandbox ── */
    st.put('Sandbox_Charm_Pool', ID, { poolId: ID, orderId: '4170000001', sku: 'CALENDAR', state: 'pooled' });
    const prodBefore = JSON.stringify(st.list('Charm_Pool_Art')) + JSON.stringify(st.doc('Charm_Pool', ID));
    r = await call({ op: 'artPut', poolId: ID, face: 'front', by: 'Ana_M', sandbox: true, art: art() });
    ok(r.status === 200 && st.doc('Sandbox_Charm_Pool_Art', ID + '~front') && st.doc('Sandbox_Charm_Pool', ID).artEdit.front > 0, 'D the sandbox writes its own record and its own pool row');
    eq(JSON.stringify(st.list('Charm_Pool_Art')) + JSON.stringify(st.doc('Charm_Pool', ID)), prodBefore, 'D production untouched');
    r = await call({ op: 'artGet', poolId: ID, face: 'front' }); ok(r.body.art === null, 'D production does not see the sandbox\'s record');
    r = await call({ op: 'artGet', poolId: ID, face: 'front', sandbox: true }); ok(r.body.art && r.body.art.poolId === ID, 'D the sandbox reads its own');
    r = await call({ op: 'artPut', poolId: ID2, face: 'front', by: 'Ana_M', sandbox: true, art: art() }); ok(r.status === 404, 'D a piece pooled only in production is not in the sandbox\'s pool');

    /* ── E · counters ── */
    const gen = () => (st.doc('Charm_Nest_Rev', 'placement') || {}).n || 0, g0 = gen();
    await call({ op: 'artGet', poolId: ID2, face: 'front' }); await call({ op: 'artList', keys: [ID2 + '~front'] });
    eq(gen(), g0, 'E reads move no counter');
    await call({ op: 'artPut', poolId: ID2, face: 'front', by: 'Ana_M', expectedAt: st.doc('Charm_Pool_Art', ID2 + '~front').at, art: art({ added: [{ id: 'hole', role: 'cut', d: 'M1 1L2 1L2 2Z', stroke: [0, 0, 0], fill: null }] }) });
    ok(gen() === g0 + 1, 'E a save raises the placement counter once (it wrote a pool row)');

    /* ── F · the page module, with a stand-in page ── */
    const cal = await require('./_artCalendar.cjs')(ROOT), cb = cal.charm, PID = '4170000003_10030_1', PID2 = '4170000004_10040_1';
    for (const id of [PID, PID2]) st.put('Charm_Pool', id, { poolId: id, orderId: id.split('_')[0], sku: 'CALENDAR', state: 'pooled', sheetId: null });
    const page = { rows: new Map(), sheets: [{ sheetId: 'sheet-p', charms: [] }], swaps: [], changes: [], reads: [] };
    const clone = (id) => Object.assign({}, cb, { id: 's:' + id, poolId: id, pinned: null });   // (Pool.cloneCharm)
    page.sheets[0].charms = [clone(PID), clone(PID2)];
    AE.setHost({
      api: async (fn, body) => { page.reads.push(body.op); const r = await call(body); if (r.status >= 400) throw Object.assign(new Error(r.body.error || 'HTTP ' + r.status), { status: r.status, data: r.body }); return r.body; },
      who: () => 'Ana_M', rowOf: id => page.rows.get(id) || null, charmOf: id => page.sheets[0].charms.find(c => c.poolId === id) || null, placed: id => id === PID2,
      flag: (id, face, at) => { const r = page.rows.get(id) || {}; r.artEdit = Object.assign({}, r.artEdit, { [face]: at || null }); page.rows.set(id, r); },
      swap: (id, next) => { const i = page.sheets[0].charms.findIndex(c => c.poolId === id); if (i >= 0) { const l = page.sheets[0].charms.slice(); l[i] = next; page.sheets[0].charms = l; } page.swaps.push(id); },
      changed: async (id, face, info) => { page.changes.push([id, face, !!(info && info.record), info && info.sheetId]); },
      liveCharms: () => page.sheets[0].charms
    });
    const ids = AE.memberIds(cb), view = global.CharmNestGeom.backView(cb, { face: 'front', res: 6 }), fr = AE.frame(view), dayBox = cb.members[cal.day(0, 0)].bbox;
    const sq = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]].map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0], fr.toItem(o[1])])));
    const changes = { removed: cal.month.map(i => ids[i]), added: [{ id: 'd1', kind: 'text', role: 'art', text: '1', fill: '#000', subpaths: sq(dayBox[0] + 3, dayBox[1] + 2, 3, 6) }] };
    const live0 = page.sheets[0].charms[0], saved = await AE.save(PID, 'front', changes, { view });
    const stored = st.doc('Charm_Pool_Art', PID + '~front');
    ok(stored && stored.by === 'Ana_M' && stored.removed.length === 3 && stored.added.length === 1 && saved.record.at === stored.at, 'F save: the record in the cloud, who and when');
    ok(page.rows.get(PID).artEdit.front === stored.at && st.doc('Charm_Pool', PID).artEdit.front === stored.at, 'F the pool row\'s stamp, in the cloud and on the page');
    ok(page.sheets[0].charms[0] !== live0 && page.sheets[0].charms[0] === saved.charm && saved.charm.id === live0.id && saved.charm.members.length === 24 - 3 + 1, 'F the sheet holds the piece\'s new charm, same id, its own parts');
    ok(live0.members === cb.members && cb.members.every(m => !m.artRedraw), 'F the master is untouched');
    eq(page.changes, [[PID, 'front', true, null]], 'F the page is told: the face\'s engraving and the sheet\'s files (changed)');
    // a cut line of a placed piece: refused here, before anything is sent
    const sent = page.reads.length; let refused = null;
    try { await AE.save(PID2, 'front', { added: [{ id: 'hole', role: 'cut', subpaths: sq(dayBox[0] + 3, dayBox[1] + 3, 2, 2) }] }, { view }); } catch (e) { refused = e; }
    ok(refused && refused.refused && /nested with/.test(refused.refused[0].why) && page.reads.length === sent, 'F a cut line of a placed piece is refused before anything is sent: ' + (refused && refused.message));
    // the piece made from the master again (a reload, a recall): its pool row names the edit, read ONCE
    AE.forget(); page.reads.length = 0;
    const fresh = clone(PID), made = await AE.piece(fresh, PID, { poolId: PID, artEdit: { front: stored.at } });
    eq(page.reads, ['artList'], 'F the edit is read in one call');
    eq(made.members.map(m => m.artMemberId), saved.charm.members.map(m => m.artMemberId), 'F and the piece is made with it, as saved');
    ok(fresh.members === cb.members && made !== fresh, 'F a new charm; the clone handed in untouched');
    await AE.piece(clone(PID), PID, { poolId: PID, artEdit: { front: stored.at } }); eq(page.reads, ['artList'], 'F a second piece of it reads nothing more (held)');
    const other = clone(PID2), same = await AE.piece(other, PID2, { poolId: PID2 }); ok(same === other && page.reads.length === 1, 'F a piece with no edit on its pool row: no read, the same charm');
    // after a checkpoint: the restored sheet holds the edited piece (kept whole) and one whose edit was saved elsewhere since
    AE.forget(); page.reads.length = 0; page.swaps.length = 0; page.changes.length = 0;
    const elsewhere = await call({ op: 'artPut', poolId: PID2, face: 'front', by: 'Bea', expectedAt: 0, art: { baseKey: AE.baseKeyOf(cb), removed: [ids[cal.day(1, 1)]], added: [] } });
    ok(elsewhere.status === 200, 'F an edit saved on another computer');
    page.rows.set(PID, { artEdit: { front: stored.at } }); page.rows.set(PID2, { artEdit: { front: elsewhere.body.art.at } });
    page.sheets[0].charms = structuredClone(page.sheets[0].charms);   // (the checkpoint: a structured copy)
    eq(await AE.sync(), 1, 'F sync: one piece made again');
    eq(page.swaps, [PID2], 'F only the one whose pool row names an edit its charm does not carry'); eq(page.reads, ['artList'], 'F read in one call');
    ok(page.sheets[0].charms[1].members.length === 23 && page.sheets[0].charms[1].artEdit.front.by === 'Bea', 'F with that edit');
    eq(page.changes, [[PID2, 'front', true, undefined]], 'F and the page is told, as for a save');
    // taken back to the master
    page.reads.length = 0; const cleared = await AE.clear(PID, 'front', {});
    ok(cleared.record === null && !st.doc('Charm_Pool_Art', PID + '~front') && JSON.stringify(page.sheets[0].charms[0].members) === JSON.stringify(cb.members) && !page.sheets[0].charms[0].artEdit && !page.sheets[0].charms[0].dropIndices && page.rows.get(PID).artEdit.front === null, 'F clear: the record gone, the piece the master\'s again, the stamp null');
    console.log(`PASS ${n} checks: ART EDIT server — artPut / artGet / artList checked strictly, saved with who and when, the pool row's stamp, a sheet that owes its files, cut lines kept on a nested piece, the laser's sheet never changed, the sandbox apart, reads free of counters`);
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
