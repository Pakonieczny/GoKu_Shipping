// Adding a sheet to, and taking a sheet out of, a set that is already committed or in Laser cutting (Paul, 7 Oct 2026):
//   "The User should be able to add/remove individual sheets from a set of sheets even in the Laser cutting process as long as
//    A. there are NO shared pieces from a multi-piece order on that particular sheet shared with other sheets in the same Set,
//    B. the sheet has NOT been marked by the user as completed (Laser cut)."
// The real charmNestLibrary handler over the in-memory shop of bridge-server.cjs (op flowApply, step setMember / setFiles), and LibraryFlow's plan
// over it. A fixture only: nothing here touches the live site, Etsy or a paid service.
//   node tests/charm-nest/library-set-edit.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { start } = require('./bridge-server.cjs');
const LF = require('../../charm-nest-flow.js');
const SE = require('../../charm-nest-set-edit.js');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs', TL = 'Order_Timeline';

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const edit = (moves, extra = {}) => post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: moves.map(([sheetId, to, pulled]) => ({ sheetId, to, ...(pulled ? { pulled } : {}) })) }], ...extra });
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  // a sheet holding `pieces` (pool ids "order_line_copy"); in a set when `setId` is given
  const mk = (id, metal, pieces, extra = {}) => st.put(S, id, { id, runId: 'run-x', metal, day, status: 'complete', draft: !extra.setId, placedCount: pieces.length, charmCount: pieces.length, density: .36, stock: { wIn: 6, hIn: 4.5 },
    poolIds: pieces, orders: [...new Set(pieces.map(p => p.split('_')[0]))], verification: { ok: true }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
    page: 1, sheetIndex: extra.setId ? 1 : null, updatedAt: ts, createdAt: ts, ...extra });
  const file = (sheetId, setId) => ({ sheetId, sheet: sheetId, path: `${setId}/${sheetId}-qr.png`, url: image, payload: 'p-' + sheetId, orders: [] });
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, ''), day, runId: 'run-x', sheetIds, materials: ['gold'], orders: {}, status: 'complete', committedAt: now - 9000, committed: [], labelFiles: sheetIds.map(i => file(i, setId)),
    labels: { pdf: { path: 'x.pdf', url: 'u' } }, processSeals: [{ how: 'approved', by: 'Ann', at: now - 5000 }], updatedAt: ts, createdAt: ts, ...extra });
  const seal = { processSeals: [{ how: 'approved', by: 'Ann', at: now - 6000 }], processReady: true };

  // ── the scene ──
  // Set 1, in Laser cutting: GF Sheet 1 and SS Sheet 1 share order 4171450075 (rule A); GF Sheet 3 is completed (rule B); the Rose Gold sheet has its cut recorded
  mkSet('set-1', ['gf-1', 'ss-1', 'gf-2', 'gf-3', 'rg-1'], { materials: ['gold', 'silver', 'rose'] });
  mk('gf-1', 'gold', ['4171450075_1_1', '5100000001_1_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 1, ...seal });
  mk('ss-1', 'silver', ['4171450075_2_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 1, ...seal });
  mk('gf-2', 'gold', ['5100000002_1_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 2, ...seal, releaseFull: true });
  mk('gf-3', 'gold', ['5100000003_1_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 3, ...seal, laserDoneAt: now - 1000, laserDoneBy: 'Ben' });
  mk('rg-1', 'rose', ['5100000004_1_1'], { setId: 'set-1', setSeq: 1, sheetIndex: 1, ...seal, roseCutAt: now - 2000, roseStockId: 'stock-1' });
  // Set 2: committed, two sheets, the target of most additions
  mkSet('set-2', ['g2-a', 'g2-b']);
  mk('g2-a', 'gold', ['5100000010_1_1', '5100000011_1_1'], { setId: 'set-2', setSeq: 2, sheetIndex: 1, ...seal });
  mk('g2-b', 'gold', ['5100000012_1_1'], { setId: 'set-2', setSeq: 2, sheetIndex: 2, ...seal });
  // Set 3: completed (the set and its sheet are marked) / Set 4: one sheet cut, one not / Set 5: one sheet only / Sets 6 and 7: a straight move / Set 8: still open
  mkSet('set-3', ['g3-s'], { laserDoneAt: now - 500, laserDoneBy: 'Ben' }); mk('g3-s', 'gold', ['5100000020_1_1'], { setId: 'set-3', setSeq: 3, ...seal, laserDoneAt: now - 500, laserDoneBy: 'Ben' });
  mkSet('set-4', ['c4-a', 'c4-b']); mk('c4-a', 'gold', ['5100000040_1_1'], { setId: 'set-4', setSeq: 4, ...seal, laserDoneAt: now - 3000, laserDoneBy: 'Ben' }); mk('c4-b', 'gold', ['5100000041_1_1'], { setId: 'set-4', setSeq: 4, sheetIndex: 2, ...seal });
  mkSet('set-5', ['only5']); mk('only5', 'gold', ['5100000050_1_1'], { setId: 'set-5', setSeq: 5, ...seal });
  mkSet('set-6', ['m6-a', 'm6-b']); mk('m6-a', 'gold', ['5100000060_1_1'], { setId: 'set-6', setSeq: 6, ...seal }); mk('m6-b', 'gold', ['5100000061_1_1'], { setId: 'set-6', setSeq: 6, sheetIndex: 2, ...seal });
  mkSet('set-7', ['m7-a']); mk('m7-a', 'gold', ['5100000070_1_1'], { setId: 'set-7', setSeq: 7, ...seal });
  mkSet('set-8', ['o8-s'], { status: 'open', committedAt: null, committed: undefined, labelFiles: [] }); mk('o8-s', 'gold', ['5100000080_1_1'], { setId: 'set-8', setSeq: 8, runId: 'run-live' });
  // draft sheets (In progress)
  mk('d-solo', 'gold', ['5100000030_1_1']); mk('d-free', 'gold', ['5100000035_1_1']); mk('d-plan', 'gold', ['5100000036_1_1']);
  mk('d-m1', 'gold', ['5100000031_1_1']); mk('d-m2', 'silver', ['5100000031_2_1']);                      // share order 5100000031
  mk('d-c2', 'gold', ['5100000010_2_1']);                                                                // shares order 5100000010 with g2a (Set 2)
  mk('d-cut', 'gold', ['5100000032_1_1'], { laserDoneAt: now - 100, laserDoneBy: 'Ben' });             // completed
  mk('d-rose', 'rose', ['5100000033_1_1'], { roseStockId: 'stock-2' });
  mk('d-held', 'gold', ['5100000037_1_1'], { laserHold: { at: now - 700, by: 'Zed', note: 'wait for the glue' } });   // a person's own hold
  st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines: {} }); st.put(RUN, 'run-live', { runId: 'run-live', status: 'review', lines: {} });

  // what must never change by an edit: every seal, approval and cut record, of every sheet and set
  const PERMANENT = ['processSeals', 'processReady', 'laserDoneAt', 'laserDoneBy', 'roseCutAt', 'roseStockId', 'committedAt', 'status'];
  const permanent = () => JSON.stringify([...st.list(S), ...st.list(SET)].map(d => [d._id, ...PERMANENT.map(k => d[k] === undefined ? null : d[k])]));
  const sheet = id => st.doc(S, id), set = id => st.doc(SET, id), held = id => !!(sheet(id).laserHold && sheet(id).laserHold.at);
  const snap = ids => JSON.stringify(ids.map(i => [sheet(i)]).concat([['set-1', set('set-1')], ['set-2', set('set-2')]]));
  const refused = async (what, moves, re, extra = {}) => {
    const all = () => new Map([...st.docs.entries()].map(([k, v]) => [k, JSON.stringify(v)])), before = all(), r = await edit(moves, extra);
    assert.equal(r.status, 409, what + ' → ' + JSON.stringify(r)); assert.match(r.error, re, what + ': ' + r.error);
    // (the handler moves its revision counters after every flowApply, refused or not: that is the handler's own rule for all ops, not this edit's)
    const after = all(), changed = [...new Set([...before.keys(), ...after.keys()])].filter(k => before.get(k) !== after.get(k) && !k.startsWith('Charm_Nest_Rev/'));
    assert.deepEqual(changed, [], what + ': a refusal writes no sheet, set, order note or seal');
    return r;
  };
  const rule = fs.readFileSync(path.join(__dirname, '../../netlify/functions/charmNestLibrary.js'), 'utf8');
  try {
    // ═══ 0. the new writer raises the counters the other computers read (placement gen, library rev): it is not on any no-bump list ═══
    const revOps = rule.match(/const REV_OPS = new Set\(\[([\s\S]*?)\]\)/)[1], noGen = rule.match(/const NO_GEN_BUMP = new Set\(\[([\s\S]*?)\]\)/)[1];
    assert(/"flowApply"/.test(revOps) && !/"flowApply"/.test(noGen), 'flowApply raises Charm_Nest_Rev/library and the placement gen');

    // ═══ A. taking a sheet out: blocked by A (pieces of one order on two sheets of the set) ═══
    let r = await refused('A: GF Sheet 1', [['gf-1', null]], /^Order 4171450075 has pieces on GF Sheet 1 and SS Sheet 1: they stay in one set$/);
    assert.equal(r.reasons[0].key, 'sharedOrders'); assert.deepEqual(r.shared.map(x => x.orderId), ['4171450075']);
    await refused('A: SS Sheet 1', [['ss-1', null]], /Order 4171450075 has pieces on SS Sheet 1 and GF Sheet 1: they stay in one set/);
    // both leaving together is still refused: each sheet is checked on its own (a set keeps the order whole)
    await refused('A: both of them', [['gf-1', null], ['ss-1', null]], /Order 4171450075 has pieces on/);
    // ═══ B. blocked by B (completed, or a recorded cut), and a set is never emptied ═══
    await refused('B: completed', [['gf-3', null]], /GF Sheet 3 is completed/);
    await refused('B: Rose Gold cut', [['rg-1', null]], /RG Sheet 1 was already cut/);
    await refused('last sheet', [['only5', null]], /all that is in Set 5.*Undo set/);
    await refused('a completed set gives nothing', [['g3-s', null]], /Set 3 is completed|GF Sheet 1 is completed/);
    await refused('an open set is changed by its run, not from here', [['o8-s', null]], /Set 8 is still being made by its run/);

    // ═══ C. taking a sheet out: allowed ═══
    const permanentBefore = permanent(), orderLines = JSON.stringify(set('set-1').orders), rev = k => (st.doc('Charm_Nest_Rev', k) || {}).n || 0, revBefore = [rev('library'), rev('placement')];
    r = await edit([['gf-2', null]]);
    assert(rev('library') > revBefore[0] && rev('placement') > revBefore[1], 'the writer raises Charm_Nest_Rev/library and /placement: every screen follows');
    assert.equal(r.status, 200, JSON.stringify(r)); assert.deepEqual(r.applied.map(a => [a.id, a.type, a.to]), [['gf-2', 'setMember', null]]); assert.deepEqual(r.membership.sheets.map(x => x.id), ['gf-2']); assert.equal(r.membership.sets[0].setId, 'set-1');
    let g = sheet('gf-2');
    assert.equal(g.draft, true); assert.equal(g.setId, null); assert.equal(g.releaseFull, false); assert(held('gf-2') && /^Taken out of Set 1/.test(g.laserHold.note) && g.laserHold.by === 'Paul', 'back to In progress as a hold: the open run does not pull it into a set by itself');
    assert.equal(g.flowHistory.at(-1).type, 'setLeave'); assert.equal(g.flowHistory.at(-1).by, 'Paul');
    let s1 = set('set-1');
    assert.deepEqual(s1.sheetIds, ['gf-1', 'ss-1', 'gf-3', 'rg-1']); assert(!s1.labelFiles.some(f => f.sheetId === 'gf-2') && s1.labelFiles.length === 4, 'the set\'s QR label list is made without it');
    assert.equal(s1.status, 'complete'); assert.equal(s1.committedAt, set('set-1').committedAt); assert.equal(s1.labels, null, 'the old labels PDF no longer describes the set until the page remakes it');
    assert.deepEqual(s1.setEdit && { by: s1.setEdit.by }, { by: 'Paul' }); assert.equal(s1.flowHistory.at(-1).type, 'setEdit'); assert.deepEqual(s1.flowHistory.at(-1).out, ['gf-2']);
    assert.equal(permanent(), permanentBefore, 'no seal, approval, cut record or completion was touched'); assert.equal(JSON.stringify(s1.orders), orderLines, 'the order lines of the others are as they were');
    assert(st.list(TL).some(e => e.sheetId === 'gf-2' && e.orderId === '5100000002' && /taken out of Set 1/.test(e.text || '')), 'the order gets a note on its timeline');
    assert.equal((await edit([['gf-2', null]])).applied.length, 0, 'asked twice, done once'); assert.equal(sheet('gf-2').flowHistory.length, g.flowHistory.length);

    // ═══ D. the same sheet put back (its hold is lifted) / a person's own hold stays ═══
    r = await edit([['gf-2', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r));
    g = sheet('gf-2'); assert.equal(g.draft, false); assert.equal(g.setId, 'set-1'); assert.equal(g.sheetIndex, 4, 'it comes back with a number nobody of its metal has in the set (1 and 3 are taken)'); assert(!held('gf-2'), 'the hold this edit made is lifted');
    assert.deepEqual(g.flowHistory.slice(-2).map(x => x.type), ['setJoin', 'release']); assert.deepEqual(set('set-1').sheetIds, ['gf-1', 'ss-1', 'gf-3', 'rg-1', 'gf-2']);
    assert.equal(permanent(), permanentBefore, 'putting it back changes no seal or cut record either'); assert(!set('set-1').laserDoneAt, 'Set 1 is not completed by this');
    r = await edit([['d-held', 'set-2']]); assert.equal(r.status, 200, JSON.stringify(r)); assert(held('d-held') && sheet('d-held').laserHold.by === 'Zed', 'a person\'s own hold is theirs: it stays');

    // ═══ E. adding a sheet ═══
    r = await edit([['d-solo', 'set-2']]); assert.equal(r.status, 200, JSON.stringify(r));
    g = sheet('d-solo'); assert.equal(g.draft, false); assert.equal(g.setId, 'set-2'); assert.equal(g.setSeq, 2); assert(g.sheetIndex >= 3, 'a number nobody in the set has'); assert.equal(g.label, null, 'its QR label is made again by the page');
    assert.equal(g.flowHistory.at(-1).type, 'setJoin'); assert.equal(g.processSeals, undefined, 'no seal or approval is added for anyone'); assert(!g.laserDoneAt);
    let s2 = set('set-2'); assert(s2.sheetIds.includes('d-solo') && s2.sheetIds.includes('d-held')); assert(!s2.laserDoneAt, 'a set does not complete by getting a sheet'); assert.equal(s2.status, 'complete');
    const line = (s2.orders['5100000030'] || {}).lines; assert(line && line[0].copies.some(c => c.sheetId === 'd-solo' && c.poolId === '5100000030_1_1'), 'the order line of its copy is on the set');
    assert.deepEqual(s2.processSeals, [{ how: 'approved', by: 'Ann', at: s2.processSeals[0].at }]); assert.equal(s2.committedAt, set('set-2').committedAt);
    // refused: a completed sheet, a Rose Gold sheet, a completed set, an open set
    await refused('add: completed sheet', [['d-cut', 'set-2']], /GF Sheet 1 is completed/);
    await refused('add: Rose Gold sheet', [['d-rose', 'set-2']], /Rose Gold sheet.*Cut Sheet press/);
    await refused('add: completed set', [['d-free', 'set-3']], /Set 3 is completed.*takes no more sheets/);
    await refused('add: open set', [['d-free', 'set-8']], /Set 8 is still being made by its run/);
    await refused('add: unknown set', [['d-free', 'set-404']], /could not be found/);
    // sharing an order: a sheet cannot come in without its mates, and a mate that stays elsewhere blocks it
    r = await refused('add: a mate left behind', [['d-m1', 'set-2']], /^Order 5100000031 has pieces on GF Draft 1 and SS Draft 1: they go into one set together$/);
    assert.deepEqual(r.reasons[0].mates, ['d-m2']);
    await refused('add: a mate in another set', [['d-c2', 'set-1']], /Order 5100000010 has pieces on .*: they go into one set together/);
    // the mates in, in one yes: both are in the set, or neither
    r = await edit([['d-m1', 'set-2'], ['d-m2', 'set-2', true]]); assert.equal(r.status, 200, JSON.stringify(r)); assert.deepEqual(r.applied.map(a => a.id).sort(), ['d-m1', 'd-m2']);
    assert.equal(sheet('d-m1').setId, 'set-2'); assert.equal(sheet('d-m2').setId, 'set-2'); assert.deepEqual(set('set-2').materials.sort(), ['gold', 'silver']);
    // the mate may join the set its mate is in
    r = await edit([['d-c2', 'set-2']]); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(sheet('d-c2').setId, 'set-2');

    // ═══ F. straight from one committed set to another (both directions pass the rules), and refused when taking it out would not pass ═══
    r = await edit([['m6-b', 'set-7']]); assert.equal(r.status, 200, JSON.stringify(r));
    assert.equal(sheet('m6-b').setId, 'set-7'); assert.equal(sheet('m6-b').draft, false); assert(!held('m6-b'), 'no hold: it never stopped being in a set'); assert.equal(sheet('m6-b').flowHistory.at(-1).fromSetId, 'set-6');
    assert.deepEqual(set('set-6').sheetIds, ['m6-a']); assert.deepEqual(set('set-7').sheetIds, ['m7-a', 'm6-b']); assert.deepEqual(r.membership.sets.map(x => x.setId).sort(), ['set-6', 'set-7']);
    await refused('straight: its order stays whole', [['gf-1', 'set-7']], /Order 4171450075 has pieces on GF Sheet 1 and SS Sheet 1/);
    await refused('straight: out of the only sheet', [['m6-a', 'set-7']], /all that is in Set 6/);

    // ═══ G. the set advances as one over what it has: its remaining sheets all cut → the set is completed with the last cut's time and name ═══
    r = await edit([['c4-b', null]]); assert.equal(r.status, 200, JSON.stringify(r)); assert.deepEqual(r.setDone, ['set-4']);
    assert.equal(set('set-4').laserDoneAt, now - 3000); assert.equal(set('set-4').laserDoneBy, 'Ben', 'the name of the last real cut: nobody is marked here'); assert(!sheet('c4-b').laserDoneAt, 'the sheet taken out is not marked cut');
    assert.equal(sheet('c4-a').laserDoneAt, now - 3000); await refused('a set completed that way takes and gives nothing', [['d-free', 'set-4']], /Set 4 is completed/);

    // ═══ H. the server checks it again inside its transaction: a stale plan, a sheet cut meanwhile, a page that still holds the old membership ═══
    await refused('stale plan', [['gf-2', null]], /changed since this move was planned/, { steps: [{ type: 'setMember', moves: [{ sheetId: 'gf-2', to: null }], expect: { 'gf-2': { setId: 'set-2' } } }] });
    st.put(S, 'gf-2', { laserDoneAt: now - 10, laserDoneBy: 'Cy' });                       // another computer marked it completed after this one planned
    await refused('cut meanwhile', [['gf-2', null]], /GF Sheet 4 is completed/);
    st.put(S, 'gf-2', { laserDoneAt: undefined, laserDoneBy: undefined });
    assert.equal((await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'gf-2', to: 'set-2' }] }, { type: 'hold', sheetIds: ['gf-2'] }] })).status, 400, 'a set edit is applied on its own');
    assert.equal((await post({ op: 'flowApply', steps: [{ type: 'setMember', moves: [{ sheetId: 'gf-2', to: null }] }] })).status, 400, 'a change needs a name');
    // a page that still shows the old membership saves its sheet and set: the membership stays as the edit made it, the rest is saved
    r = await post({ op: 'putSheet', sheet: { id: 'gf-2', metal: 'gold', draft: true, setId: null, setSeq: null, sheetIndex: null, label: 'page label' } }); assert.equal(r.status, 200, JSON.stringify(r));
    assert.equal(sheet('gf-2').setId, 'set-1', 'putSheet leaves the set of a sheet in a committed set alone'); assert.equal(sheet('gf-2').draft, false); assert.equal(sheet('gf-2').label, 'page label', 'the rest of the sheet is saved');
    await post({ op: 'putSheet', sheet: { id: 'd-free', metal: 'gold', draft: false, setId: 'set-2', setSeq: 2, sheetIndex: 9 } });
    assert.equal(sheet('d-free').draft, true, 'a stale page does not put a sheet into a committed set'); assert.equal(sheet('d-free').setId || null, null);
    r = await post({ op: 'setUpdate', setId: 'set-2', patch: { sheetIds: ['g2-a', 'g2-b'], status: 'complete', note: 'saved' } }); assert.equal(r.status, 200, JSON.stringify(r));
    assert(set('set-2').sheetIds.includes('d-solo') && set('set-2').sheetIds.includes('d-m1'), 'setUpdate keeps the sheet list the edit made'); assert.equal(set('set-2').note, 'saved', 'the rest of the set is saved');

    // ═══ I. the set's files are written for its current sheets only ═══
    const was = JSON.stringify(set('set-1').sheetIds);
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setFiles', setId: 'set-1', labelFiles: [{ sheetId: 'gf-1', url: 'u1', path: 'a.png' }, { sheetId: 'gf-404', url: 'u2', path: 'b.png' }], labels: { pdf: { url: 'u-pdf', path: 'p.pdf' }, manifest: { url: 'u-man', path: 'm.pdf' }, files: [{ url: 'u1', path: 'a.png', sheet: 'GF Sheet 1' }] } }] });
    assert.equal(r.status, 200, JSON.stringify(r)); assert.deepEqual(set('set-1').labelFiles.map(f => f.sheetId), ['gf-1'], 'a sheet the set does not have is never written'); assert.equal(set('set-1').labels.pdf.url, 'u-pdf'); assert.equal(JSON.stringify(set('set-1').sheetIds), was, 'only the files');
    assert.equal((await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setFiles', setId: 'set-8' }] })).status, 409, 'an open set\'s files are its run\'s');
    assert.equal(permanent().includes('Ann'), true);

    // ═══ J. what LibraryFlow plans over the same records: the words, the one yes, the steps, and the server's last word ═══
    const hooksSeen = { relabel: [], files: [], applied: [] };
    LF.configure({ api: post, employee: () => 'Paul', rows: () => [], live: () => null, remakeLabel: async () => {}, include: async () => {},
      relabelSet: async s => { hooksSeen.relabel.push(s); }, setFiles: async s => { hooksSeen.files.push(s.setId); }, applyMembership: async m => { hooksSeen.applied.push(m); } });
    let p = await LF.plan({ kind: 'sheet', id: 'gf-1', to: { area: 'progress' } });
    assert.equal(p.ok, false); const sh = p.needs.find(n => n.key === 'sharedOrders'); assert(sh && /^Order 4171450075 has pieces on GF Sheet 1 and SS Sheet 1: they stay in one set$/.test(sh.detail), JSON.stringify(p.needs));
    p = await LF.plan({ kind: 'sheet', id: 'gf-3', to: { area: 'progress' } }); assert.equal(p.ok, false); assert(p.needs.some(n => n.key === 'sheetCompleted' && /GF Sheet 3 is completed/.test(n.label)), JSON.stringify(p.needs));
    p = await LF.plan({ kind: 'sheet', id: 'd-free', to: { set: 'set-3' } }); assert.equal(p.ok, false); assert(p.needs.some(n => n.key === 'setCompleted'), JSON.stringify(p.needs));
    // out: one yes, the steps, nothing written by planning
    const before = snap(['gf-2']);
    p = await LF.plan({ kind: 'sheet', id: 'gf-2', to: { area: 'progress' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm.map(c => c.key), ['leaveSet']); assert.deepEqual(p.steps.map(x => x.type), ['setMember', 'setFiles']);
    assert(p.auto.some(a => a.key === 'hold') && p.auto.some(a => a.key === 'setFiles'), JSON.stringify(p.auto.map(a => a.key))); assert.equal(snap(['gf-2']), before, 'planning writes nothing');
    // another computer marks it completed between the plan and the press: the server refuses, nothing is changed, the person is told
    st.put(S, 'gf-2', { laserDoneAt: now - 10, laserDoneBy: 'Cy' });
    let c = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet'] }); assert.equal(c.ok, false); assert.match(c.error, /GF Sheet 4 is completed/); assert.equal(sheet('gf-2').setId, 'set-1'); assert.equal(hooksSeen.files.length, 0, 'no file was remade for a move that was refused');
    st.put(S, 'gf-2', { laserDoneAt: undefined, laserDoneBy: undefined });
    c = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet'] }); assert.equal(c.ok, true, JSON.stringify(c));
    assert.equal(sheet('gf-2').draft, true); assert(held('gf-2')); assert.deepEqual(hooksSeen.files, ['set-1'], 'the set\'s files are made again'); assert.equal(hooksSeen.applied.length, 1, 'this computer\'s pages follow at once'); assert.equal(hooksSeen.applied[0].sheets[0].id, 'gf-2');
    // in: a lone sheet, then a sheet with its mate
    p = await LF.plan({ kind: 'sheet', id: 'd-plan', to: { set: 'set-2' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.steps.map(x => x.type), ['setMember', 'relabel', 'setFiles']); assert(!p.needs.some(n => n.key === 'setCommitted'));
    assert(p.auto.some(a => a.key === 'membership' && /GF Draft 1 added to Set 2/.test(a.label)), JSON.stringify(p.auto.map(a => a.label)));
    assert(!p.confirm.some(x => /approv/i.test(x.label)), 'nothing is approved for anyone');
    c = await LF.commit(p, { by: 'Paul', confirmed: p.confirm.map(x => x.key) }); assert.equal(c.ok, true, JSON.stringify(c)); assert.equal(sheet('d-plan').setId, 'set-2'); assert(hooksSeen.relabel.some(x => x.sheetIds.includes('d-plan')) && hooksSeen.files.includes('set-2'));
    mk('d-n1', 'gold', ['5100000090_1_1']); mk('d-n2', 'silver', ['5100000090_2_1']);
    p = await LF.plan({ kind: 'sheet', id: 'd-n1', to: { set: 'set-2' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert(p.confirm.some(x => x.key === 'together' && /SS Draft 1 joins Set 2/.test(x.label)), JSON.stringify(p.confirm)); assert.equal(p.steps[0].moves.length, 2);
    c = await LF.commit(p, { by: 'Paul', confirmed: p.confirm.map(x => x.key) }); assert.equal(c.ok, true, JSON.stringify(c)); assert.equal(sheet('d-n1').setId, 'set-2'); assert.equal(sheet('d-n2').setId, 'set-2');
    // the words alone (the page's rule and the server's are the one file)
    const v = SE.verifyMoves({ moves: [{ id: 'x', to: null }], recs: { x: { id: 'x', metal: 'gold', setId: 'set-1', draft: false, laserDoneAt: 5 } }, sets: { 'set-1': { doc: { setId: 'set-1', seq: 1, committedAt: 1 }, members: [{ id: 'x' }, { id: 'y' }] } } });
    assert.equal(v.ok, false); assert.equal(v.reasons[0].key, 'sheetCompleted'); assert.equal(SE.sayWhy(v), 'GF Sheet 1 is completed. It keeps its cut record and set until it is moved back to Laser cutting.');
    console.log('PASS: committed sets take and give sheets by rule A (no order split) and rule B (not completed / cut); a refusal says which order and sheets and writes nothing; seals and cut records are never touched; the server re-checks inside its transaction; the set files follow; stale pages cannot undo it');
  } finally { await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
