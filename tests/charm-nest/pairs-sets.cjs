// Pairs, mismatched pairs and disc necklaces in SETS OF SHEETS (Paul, 9 Oct 2026, rule R3: "whenever selling pairs of earrings, matching or mismatched ... if for any
// reason these pieces don't end up on the same sheet ... those sheets that have mixed sets ... must be considered when generating sets and when a user tries to move a
// sheet out of a set"). Offline only: the pure core (charm-nest-shared-orders.js), the one move rule (charm-nest-set-edit.js), the real charmNestLibrary handler over the
// in-memory shop (bridge-server.cjs), LibraryFlow's plan, and Readiness. Nothing here touches the live site, Etsy or a paid service.
//   node tests/charm-nest/pairs-sets.cjs
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const noNested = require('./_noNestedArrays.cjs');
const SO = require('../../charm-nest-shared-orders.js');
const SE = require('../../charm-nest-set-edit.js');
const RD = require('../../charm-nest-readiness.js');
const LF = require('../../charm-nest-flow.js');
const C = SO.core;
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs', POOL = 'Charm_Pool';

// ── the orders (receipt ids and transaction ids are long enough for the server's pool id test) ──
const MIS = '6100000001', MIS_T = '3100000001';         // MISMATCHED_7134: MITTENS 1 (left) + MITTENS 2 (right), TWO DIFFERENT DESIGNS, one line
const MIS2 = '6100000011', MIS2_T = '3100000011';        // a second mismatched pair, for a joining sheet
const PAIR = '6100000002', PAIR_T = '3100000002';        // a matching pair of studs (two pieces of one design)
const DISC = '6100000003', DISC_T = '3100000003';        // a necklace with three discs
const MIX = '6100000004';                                // one order: studs pair (line A), huggie pair (line B), three discs (line C), and a single (line D)
const [A_T, B_T, C_T, D_T] = ['3100000041', '3100000042', '3100000043', '3100000044'];
const SINGLE = '6100000005', PLAIN = '6100000006';       // orders with no pair: one piece each / two single lines (must behave exactly as before)
const id = (r, t, n) => `${r}_${t}_${n}`;

(async () => {
  // ═══ 0. the pure core: pieces are counted by ORDER, never by design ═══
  {
    const sh = (sid, setId, keys, extra = {}) => C.sheetOf({ id: sid, metal: 'gold', page: +sid.replace(/\D/g, '') || 1, setId, poolIds: keys, ...extra });
    // a mismatched pair: the records say only pool ids (no SKU, no design): MITTENS 1 and MITTENS 2 are different designs and still one order
    const a1 = sh('a1', 'set-1', [id(MIS, MIS_T, 1), id(PLAIN, '3100000061', 1)]), b2 = sh('b2', null, [id(MIS, MIS_T, 2)]);
    assert.deepEqual(C.groups([a1, b2]).map(g => g.ids.sort()), [['a1', 'b2']], 'left on one sheet, right on another: the two sheets are tied');
    assert.deepEqual(C.composition([a1, b2]).pull.map(x => [x.id, x.setId, x.because]), [['b2', 'set-1', [MIS]]], 'set generation: the sheet with the right earring is pulled into the set that holds the left one');
    // the same pool id on two sheets is one piece (a stale record) unless the record says the two are the two bodies (L and R)
    assert.deepEqual(C.groups([sh('e1', null, [id(MIS, MIS_T, 1)]), sh('e2', null, [id(MIS, MIS_T, 1)])]), [], 'one id on two sheets, no side: one piece listed twice, as before');
    const l = sh('l1', null, [id(MIS, MIS_T, 1)], { pieceSides: { [id(MIS, MIS_T, 1)]: 'L' } }), r = sh('r1', null, [id(MIS, MIS_T, 1)], { pieceSides: { [id(MIS, MIS_T, 1)]: 'R' } });
    assert.deepEqual(C.groups([l, r]).map(g => g.ids.sort()), [['l1', 'r1']], 'the same id with a left and a right side is TWO pieces: the split is seen');
    assert.deepEqual(C.groups([l, sh('l2', null, [id(MIS, MIS_T, 1)], { pieceSides: { [id(MIS, MIS_T, 1)]: 'L' } })]), [], 'two left pieces of one id are one piece');
    // what the item says: nothing told -> the old plain words; sides told -> left and right
    const plain = C.between([a1, b2], ['b2'], 'new'); assert.equal(plain.length, 1); assert.equal(plain[0].words, '', 'no one told what the pieces are: the plain words stay'); assert.equal(plain[0].kind, '');
    const meta = k => (k === id(MIS, MIS_T, 1) ? { side: 'L', form: 'earrings' } : k === id(MIS, MIS_T, 2) ? { side: 'R', form: 'earrings' } : null);
    const told = C.between([a1, b2], ['b2'], 'new', { meta });
    assert.equal(told[0].kind, 'mismatched'); assert.equal(told[0].words, 'has its left earring on GF Sheet 1 and its right earring on GF Sheet 2');
    assert.deepEqual(told[0].pieces.map(p => [p.side, p.sideLabel, p.sheetLabel, p.groupKey]), [['L', 'Left', 'GF Sheet 1', `${MIS}:${MIS_T}`], ['R', 'Right', 'GF Sheet 2', `${MIS}:${MIS_T}`]]);
    assert.equal(C.sentence(told, 'GF Sheet 2'), `Order ${MIS} has its left earring on GF Sheet 1 and its right earring on GF Sheet 2: sheets that share a multi-piece order stay in the same set.`);
    // a matching pair (two pieces of one design) and three discs
    const p1 = sh('p1', 'set-1', [id(PAIR, PAIR_T, 1)]), p2 = sh('p2', null, [id(PAIR, PAIR_T, 2)]);
    const pair = C.between([p1, p2], ['p2'], 'new', { meta: () => ({ form: 'earrings', kind: 'pair' }) });
    assert.equal(pair[0].kind, 'pair'); assert.equal(pair[0].words, 'has its two earrings on GF Sheet 1 and GF Sheet 2');
    const nk = C.between([p1, p2], ['p2'], 'new', { meta: () => ({ kind: 'pair', form: 'necklace' }) });
    assert.equal(nk[0].kind, 'multi'); assert.equal(nk[0].words, 'has its 2 pieces on GF Sheet 1 and GF Sheet 2', 'two necklaces of one line are two pieces, never two earrings');
    const d1 = sh('d1', 'set-1', [id(DISC, DISC_T, 1)]), d2 = sh('d2', 'set-1', [id(DISC, DISC_T, 2)]), d3 = sh('d3', null, [id(DISC, DISC_T, 3)]);
    assert.deepEqual(C.groups([d1, d2, d3]).map(g => g.ids.sort()), [['d1', 'd2', 'd3']], 'three discs on three sheets: one group of three');
    const discs = C.between([d1, d2, d3], ['d3'], 'new', { meta: () => ({ kind: 'multi', discs: true }) });
    assert.equal(discs[0].kind, 'multi'); assert.equal(discs[0].words, 'has its 3 discs on GF Sheet 1, GF Sheet 2 and GF Sheet 3'); assert.equal(discs[0].total, 3);
    assert.deepEqual(C.between([d1, d2, d3], ['d1', 'd2', 'd3'], 'new'), [], 'all three moving together split nothing');
    // a mixed sheet: studs, huggies, discs and a single of ONE order across three sheets
    const m1 = sh('m1', 'set-1', [id(MIX, A_T, 1), id(MIX, B_T, 1), id(MIX, C_T, 1), id(MIX, D_T, 1)]), m2 = sh('m2', 'set-1', [id(MIX, A_T, 2), id(MIX, B_T, 2), id(MIX, C_T, 2)]), m3 = sh('m3', null, [id(MIX, C_T, 3)]);
    assert.deepEqual(C.groups([m1, m2, m3]).map(g => g.ids.sort()), [['m1', 'm2', 'm3']], 'mixed sheets of one order are one group');
    assert.deepEqual(C.composition([m1, m2, m3]).pull.map(x => x.id), ['m3'], 'the sheet with disc 3 joins the set of the other two');
    const mixed = C.between([m1, m2, m3], ['m3'], 'new'); assert.equal(mixed.length, 1, 'one order, one item'); assert.deepEqual(mixed[0].thereIds.sort(), ['m1', 'm2']);
    const kinds = k => { const g = k.split('_')[1]; return g === A_T || g === B_T ? { form: 'earrings', kind: 'pair' } : g === C_T ? { kind: 'multi', discs: true } : { kind: 'single' }; };
    const mixedTold = C.between([m1, m2, m3], ['m3'], 'new', { meta: kinds })[0];
    assert.equal(mixedTold.kind, 'multi'); assert.equal(mixedTold.words, 'has 2 pairs of earrings, 3 discs and a piece across GF Sheet 1, GF Sheet 2 and GF Sheet 3', 'a mixed order says what it holds in one short line');
    // orders with no pair behave exactly as before: two single lines of one order on two sheets
    const s1 = sh('s1', 'set-1', [id(PLAIN, '3100000061', 1)]), s2 = sh('s2', null, [id(PLAIN, '3100000062', 1)]);
    const old = C.between([s1, s2], ['s2'], 'new', { meta: () => ({ kind: 'single', form: 'necklace' }) });
    assert.equal(old.length, 1); assert.equal(old[0].words, '', 'two single lines say nothing about pairs'); assert.equal(old[0].kind, '');
    assert.equal(SE.sharedWords(old, 'they stay in one set'), `Order ${PLAIN} has pieces on GF Sheet 2 and GF Sheet 1: they stay in one set`);
    assert.deepEqual(C.groups([sh('o1', null, [id(SINGLE, '3100000051', 1)]), sh('o2', null, [id(PLAIN, '3100000061', 1)])]), [], 'a lone single piece ties nothing');
  }

  // ═══ 1. the one move rule (SetEdit.verifyMoves): in or out, allowed or refused, one plain line ═══
  {
    const now = Date.now(), rec = (rid, metal, keys, extra = {}) => ({ id: rid, metal, sheetIndex: +rid.replace(/\D/g, '') || 1, poolIds: keys, orders: [...new Set(keys.map(k => k.split('_')[0]))], ...extra });
    const g1 = rec('g1', 'gold', [id(MIS, MIS_T, 1), id(PLAIN, '3100000061', 1)], { setId: 'set-1' }), g2 = rec('g2', 'gold', [id(MIS, MIS_T, 2)], { setId: 'set-1' }), g3 = rec('g3', 'gold', [id(SINGLE, '3100000051', 1)], { setId: 'set-1' });
    const sets = { 'set-1': { doc: { setId: 'set-1', seq: 1, committedAt: now - 9000, status: 'complete', sheetIds: ['g1', 'g2', 'g3'] }, members: [g1, g2, g3] } };
    const meta = k => (k === id(MIS, MIS_T, 1) ? { side: 'L' } : k === id(MIS, MIS_T, 2) ? { side: 'R' } : null);
    const out = (sid, m) => SE.verifyMoves({ moves: [{ id: sid, to: null }], recs: { [sid]: { g1, g2, g3 }[sid] }, sets, others: [g1, g2, g3], ...(m ? { meta: m } : {}) });
    let v = out('g2', meta); assert.equal(v.ok, false); assert.equal(v.reasons[0].key, 'sharedOrders');
    assert.equal(SE.sayWhy(v), `Order ${MIS} has its left earring on GF Sheet 1 and its right earring on GF Sheet 2: they stay in one set`, 'taking the right earring out of the set is refused, in one line that says what the pieces are');
    assert.equal(SE.sayWhy(out('g1', meta)), `Order ${MIS} has its left earring on GF Sheet 1 and its right earring on GF Sheet 2: they stay in one set`, 'and so is taking the left one');
    assert.equal(SE.sayWhy(out('g2')), `Order ${MIS} has pieces on GF Sheet 2 and GF Sheet 1: they stay in one set`, 'a caller that knows nothing of the pieces still gets the refusal, in the plain words (the rule never depends on the words)');
    assert.equal(out('g3', meta).ok, true, 'a sheet that shares no order leaves freely, exactly as before');
    // joining: the right earring's sheet cannot go to another set while the left one stays
    const draft = rec('dr', 'gold', [id(MIS2, MIS2_T, 2)], { draft: true }), mate = rec('mt', 'gold', [id(MIS2, MIS2_T, 1)], { setId: 'set-1' });
    const sets2 = { 'set-1': sets['set-1'], 'set-2': { doc: { setId: 'set-2', seq: 2, committedAt: now - 5000, status: 'complete', sheetIds: [] }, members: [] } };
    const into = (to, m) => SE.verifyMoves({ moves: [{ id: 'dr', to }], recs: { dr: draft }, sets: sets2, others: [g1, g2, g3, mate, draft], ...(m ? { meta: m } : {}) });
    const m2 = k => (k === id(MIS2, MIS2_T, 1) ? { side: 'L' } : k === id(MIS2, MIS2_T, 2) ? { side: 'R' } : null);
    assert.equal(SE.sayWhy(into('set-2', m2)), `Order ${MIS2} has its right earring on GF Sheet 1 and its left earring on GF Sheet 1: they go into one set together`.replace('its right earring on GF Sheet 1 and its left earring on GF Sheet 1', 'its left earring on GF Sheet 1 and its right earring on GF Sheet 1'), 'joining another set without the sheet that holds the left earring is refused');
    assert.equal(into('set-1', m2).ok, true, 'joining the set that already holds the left earring is allowed');
    // rule B stays per sheet: a completed sheet cannot move whatever its pieces are
    const done = rec('dn', 'gold', [id(SINGLE, '3100000051', 1)], { setId: 'set-1', laserDoneAt: now - 100 });
    assert.equal(SE.verifyMoves({ moves: [{ id: 'dn', to: null }], recs: { dn: done }, sets, others: [done] }).reasons[0].key, 'sheetCompleted');
  }

  // ═══ 2. the real handler over the shop: flowApply setMember refuses and allows on the same knowledge ═══
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-09';
  try {
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const edit = (moves, extra = {}) => post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: moves.map(([sheetId, to, pulled]) => ({ sheetId, to, ...(pulled ? { pulled } : {}) })) }], ...extra });
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    const mk = (sid, metal, pieces, extra = {}) => st.put(S, sid, { id: sid, runId: 'run-x', metal, day, status: 'complete', draft: !extra.setId, placedCount: pieces.length, charmCount: pieces.length, density: .36, stock: { wIn: 6, hIn: 4.5 },
      poolIds: pieces, orders: [...new Set(pieces.map(p => p.split('_')[0]))], verification: { ok: true }, outputs: { ai: { path: sid + '.ai', url: srv.sorterOrigin + '/' + sid + '.ai' }, preview: { path: sid + '.png', url: image } },
      page: 1, sheetIndex: extra.setId ? 1 : null, updatedAt: ts, createdAt: ts, ...extra });
    const file = (sheetId, setId) => ({ sheetId, sheet: sheetId, path: `${setId}/${sheetId}-qr.png`, url: image, payload: 'p-' + sheetId, orders: [] });
    const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, ''), day, runId: 'run-x', sheetIds, materials: ['gold'], orders: {}, status: 'complete', committedAt: now - 9000, committed: [], labelFiles: sheetIds.map(i => file(i, setId)),
      labels: { pdf: { path: 'x.pdf', url: 'u' } }, processSeals: [{ how: 'approved', by: 'Ann', at: now - 5000 }], updatedAt: ts, createdAt: ts, ...extra });
    const seal = { processSeals: [{ how: 'approved', by: 'Ann', at: now - 6000 }], processReady: true };
    const pool = (key, side, sku, extra = {}) => st.put(POOL, key, { poolId: key, orderId: key.split('_')[0], transactionId: key.split('_')[1], copy: +key.split('_')[2], sku, form: 'earrings', quantity: 1, state: 'written', ...(side ? { side, bodyIndex: side === 'L' ? 0 : 1 } : {}), updatedAt: ts, ...extra });

    // Set 1 (committed): the mismatched pair MIS on GF Sheet 1 (left, MITTENS 1) and GF Sheet 2 (right, MITTENS 2), the matching pair PAIR across Sheets 1 and 2, and a free sheet.
    mkSet('set-1', ['pg-1', 'pg-2', 'pg-3']);
    mk('pg-1', 'gold', [id(MIS, MIS_T, 1), id(SINGLE, '3100000051', 1)], { setId: 'set-1', setSeq: 1, sheetIndex: 1, ...seal });
    mk('pg-2', 'gold', [id(MIS, MIS_T, 2)], { setId: 'set-1', setSeq: 1, sheetIndex: 2, ...seal });
    // Set 3 (committed): the matching pair PAIR on two sheets
    mkSet('set-3', ['mp-1', 'mp-2']);
    mk('mp-1', 'gold', [id(PAIR, PAIR_T, 1)], { setId: 'set-3', setSeq: 3, sheetIndex: 1, ...seal }); mk('mp-2', 'gold', [id(PAIR, PAIR_T, 2)], { setId: 'set-3', setSeq: 3, sheetIndex: 2, ...seal });
    mk('pg-3', 'gold', [id(PLAIN, '3100000061', 1)], { setId: 'set-1', setSeq: 1, sheetIndex: 3, ...seal });
    mkSet('set-2', ['x-1'], { setId: 'set-2' }); mk('x-1', 'gold', [id('6100000090', '3100000090', 1)], { setId: 'set-2', setSeq: 2, sheetIndex: 1, ...seal });
    // a draft sheet holding the right earring of a second mismatched pair whose left earring is in Set 1
    mk('pg-4', 'gold', [id(MIS2, MIS2_T, 1)], { setId: 'set-1', setSeq: 1, sheetIndex: 4, ...seal });
    st.put(S, 'pg-4', { ...st.doc(S, 'pg-4') });
    mk('dr-1', 'gold', [id(MIS2, MIS2_T, 2)]);
    st.doc(SET, 'set-1').sheetIds.push('pg-4'); st.doc(SET, 'set-1').labelFiles.push(file('pg-4', 'set-1'));
    pool(id(MIS, MIS_T, 1), 'L', 'MISMATCHED_7134'); pool(id(MIS, MIS_T, 2), 'R', 'MISMATCHED_7134');
    pool(id(PAIR, PAIR_T, 1), null, 'MITTENS_1'); pool(id(PAIR, PAIR_T, 2), null, 'MITTENS_1');
    pool(id(MIS2, MIS2_T, 1), 'L', 'MISMATCHED_6849'); pool(id(MIS2, MIS2_T, 2), 'R', 'MISMATCHED_6849');
    st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines: {} });
    const all = () => new Map([...st.docs.entries()].map(([k, v]) => [k, JSON.stringify(v)]));
    const refused = async (what, moves, re) => {
      const before = all(), r = await edit(moves);
      assert.equal(r.status, 409, what + ' → ' + JSON.stringify(r)); assert.match(r.error, re, what + ': ' + r.error);
      const after = all(), changed = [...new Set([...before.keys(), ...after.keys()])].filter(k => before.get(k) !== after.get(k) && !k.startsWith('Charm_Nest_Rev/'));
      assert.deepEqual(changed, [], what + ': a refusal writes nothing');
      return r;
    };

    // taking a sheet out: the mismatched pair says left and right; the matching pair (checked after it is told apart by order) says two earrings
    let r = await refused('out: the sheet with the right earring', [['pg-2', null]], /^Order 6100000001 has its left earring on GF Sheet 1 and its right earring on GF Sheet 2: they stay in one set/);
    assert.equal(r.reasons[0].key, 'sharedOrders'); assert(r.shared.some(x => x.orderId === MIS && x.kind === 'mismatched'), JSON.stringify(r.shared));
    r = await refused('out: the sheet with the left earring', [['pg-1', null]], /^Order 6100000001 has its left earring on GF Sheet 1 and its right earring on GF Sheet 2: they stay in one set/);
    await refused('out: a matching pair (two earrings of one design)', [['mp-2', null]], new RegExp(`^Order ${PAIR} has its two earrings on GF Sheet 1 and GF Sheet 2: they stay in one set`));
    // adding the draft sheet: the left earring stays in Set 1, so Set 2 is refused and Set 1 is allowed
    await refused('in: the right earring to a set without the left', [['dr-1', 'set-2']], new RegExp(`^Order ${MIS2} has its left earring on GF Sheet 4.*they go into one set together`));
    r = await edit([['dr-1', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r));
    assert.equal(st.doc(S, 'dr-1').setId, 'set-1'); assert.equal(st.doc(S, 'dr-1').draft, false);
    const lines = st.doc(SET, 'set-1').orders[MIS2].lines, copies = lines.flatMap(l => l.copies);
    assert(copies.some(c => c.poolId === id(MIS2, MIS2_T, 2) && c.side === 'R' && c.sheetId === 'dr-1'), 'the set\'s copy list keeps the side of the piece that joined: ' + JSON.stringify(copies));
    for (const d of [...st.list(S), ...st.list(SET)]) noNested(d, 'doc ' + (d._id || d.id || d.setId));
    // now both earrings are in Set 1: neither sheet can leave it alone, and a sheet that shares no order moves as before
    await refused('out: the sheet with the left earring of the second pair', [['pg-4', null]], new RegExp(`^Order ${MIS2} has its left earring on GF Sheet 4 and its right earring on GF Sheet 5: they stay in one set`));
    await refused('out: the sheet that just joined', [['dr-1', null]], new RegExp(`^Order ${MIS2} has its left earring on GF Sheet 4 and its right earring on GF Sheet 5`));
    r = await edit([['pg-3', null]]); assert.equal(r.status, 200, 'a sheet with no pair leaves as before: ' + JSON.stringify(r)); assert.equal(st.doc(S, 'pg-3').draft, true);
    // the plain refusal for an order nobody told apart (no pool rows): the old words, byte for byte
    mk('qq-1', 'gold', [id('6100000100', '3100000100', 1)], { setId: 'set-2', setSeq: 2, sheetIndex: 2, ...seal }); mk('qq-2', 'gold', [id('6100000100', '3100000101', 1)], { setId: 'set-2', setSeq: 2, sheetIndex: 3, ...seal });
    st.doc(SET, 'set-2').sheetIds.push('qq-1', 'qq-2');
    await refused('out: two single lines of one order, no pool rows', [['qq-2', null]], /^Order 6100000100 has pieces on GF Sheet 3 and GF Sheet 2: they stay in one set$/);

    // ═══ 3. LibraryFlow's plan says it too (the page's own pieces tell the words; the records' answer is merged) ═══
    const pageSheets = () => ['pg-1', 'pg-2'].map(i => C.sheetOf(st.doc(S, i), { label: SE.wordOf(st.doc(S, i)), fixed: '' }));
    LF.configure({ api: post, employee: () => 'Paul', rows: () => [], live: () => null, remakeLabel: async () => {}, include: async () => {}, relabelSet: async () => {}, setFiles: async () => {}, applyMembership: async () => {},
      shared: { between: (sid, to, opts) => C.between(pageSheets(), [sid], to === 'new' ? 'new' : to, { meta: k => (k === id(MIS, MIS_T, 1) ? { side: 'L' } : k === id(MIS, MIS_T, 2) ? { side: 'R' } : null) }), groupOf: () => null, enrich: items => items } });
    const p = await LF.plan({ kind: 'sheet', id: 'pg-2', to: { area: 'progress' } });
    assert.equal(p.ok, false); const need = p.needs.find(n => n.key === 'sharedOrders'); assert(need, JSON.stringify(p.needs));
    assert.match(need.detail, /^Order 6100000001 has its left earring on GF Sheet 1 and its right earring on GF Sheet 2: they stay in one set/, need.detail);
    LF.configure({ shared: null });
  } finally { try { srv.close && srv.close(); } catch (_) { /* done */ } }

  // ═══ 4. Readiness: a line that makes more pieces than its quantity is waited for in full ═══
  {
    const key = `${MIS}_${MIS_T}`;
    assert.deepEqual(RD.copyIds({ poolIds: [], spec: { quantity: 1 } }, key), [`${key}_1`], 'a line that states no piece count: its quantity, as before');
    assert.deepEqual(RD.copyIds({ poolIds: [], spec: { quantity: 1, pieceCount: 2 } }, key), [`${key}_1`, `${key}_2`], 'a mismatched pair that lost its ids is waited for as TWO pieces');
    assert.deepEqual(RD.copyIds({ poolIds: [`${key}_2`], spec: { quantity: 1, pieceCount: 2 } }, key), [`${key}_1`, `${key}_2`]);
    assert.deepEqual(RD.copyIds({ poolIds: [`${key}_1`, `${key}_2`], quantity: 1 }, key), [`${key}_1`, `${key}_2`], 'a record that lists both ids keeps both');
    assert.deepEqual(RD.copyIds({ poolIds: [], quantity: 3, pieceCount: 2 }, key), [`${key}_1`, `${key}_2`, `${key}_3`], 'a piece count below the quantity changes nothing');
  }

  // ═══ 5. the set manifest tells a pair on two sheets (and prints nothing extra for a set without one) ═══
  {
    const orders = { [MIS]: { lines: [{ transactionId: MIS_T, sku: 'MISMATCHED_7134', copies: [{ copy: 1, sheet: 'GF Sheet 1', sheetId: 'a', side: 'L' }, { copy: 2, sheet: 'GF Sheet 2', sheetId: 'b', side: 'R' }] }] },
      [PLAIN]: { lines: { x: { transactionId: 'x', sku: 'ONE', copies: [{ copy: 1, sheet: 'GF Sheet 1', sheetId: 'a' }] } } } };
    assert.deepEqual(SE.spanLines(orders), [`${MIS}  MISMATCHED_7134#1 (Left) GF Sheet 1  MISMATCHED_7134#2 (Right) GF Sheet 2`]);
    assert.deepEqual(SE.spanLines({ [PLAIN]: orders[PLAIN] }), [], 'a set with no order on two sheets prints no extra section');
    assert.equal(SE.sideTag({ side: 'L' }), ' (Left)'); assert.equal(SE.sideTag({ side: null }), ''); assert.equal(SE.sideTag({}), '');
  }
  console.log('PASS: pairs in sets: rule A counts the pieces of mismatched pairs, matching pairs, discs and mixed sheets by order; moves in and out are allowed or refused with one plain line that says what the pieces are; old records and sets without pairs unchanged');
})().catch(e => { console.error(e); process.exit(1); });
