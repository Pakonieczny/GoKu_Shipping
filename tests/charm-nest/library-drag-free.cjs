// Library drag and drop between processes and sets (Paul, 10 Oct 2026, Image #2):
//   "I can not seem to Drag/Drop Sheets in the Library tab between processes. What does it mean this sheet is in process, I don't care
//    about that. Unless the move violates one of the core principals of the Set then I should be able to move it completely freely
//    between various processes in the Library tab."   (core principle: a Set needs at least 1 Completed GF sheet and 1 Completed SS sheet)
// Over the real charmNestLibrary handler on the in-memory shop of bridge-server.cjs (nothing here touches the live site, Etsy or a paid service)
// and LibraryFlow's engine: the SS sheet of the open Set 1, at Engraving step 2 (image 2), dragged to every place; every move that must be free
// goes through; every move that must be refused is refused (a) by the core principle, (b) by a multi-piece order split over two sets (rule A),
// (c) by a sheet that was laser cut (rule B), (d) because a set would be emptied, (e) by a physical-safety rule that already existed
// (a sheet is approved for Laser cutting only with its engravings, orders and QR labels in place; a partial Rose Gold sheet needs its Cut Sheet press),
// each with ONE plain line at the drop place, and a refusal writes nothing. Fake Firestore: no nested arrays are written.
//   node tests/charm-nest/library-drag-free.cjs
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const LF = require('../../charm-nest-flow.js');
const SE = require('../../charm-nest-set-edit.js');
const RULES = require('../../charm-nest-set-rules.js');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const lines = {};
  let n = 0;
  // a sheet of one order (or the pieces given), full and ready for Laser cutting unless `extra` says otherwise; needsBack: its back engraving is not approved yet (Engraving, step 2)
  const mk = (id, metal, extra = {}) => {
    const order = extra.order || String(3800000000 + ++n), pool = extra.pools || [order + '_1_1'], key = order + '_1';
    lines[key] = { orderId: order, state: 'written', quantity: 1, poolIds: pool, ...(extra.needsBack ? { engrave: { needed: true, state: 'review', approved: false } } : { engraveCandidate: false }) };
    const { needsBack, order: _o, pools: _p, ...rest } = extra;
    st.put(S, id, { id, runId: 'run-live', metal, day, status: 'complete', placedCount: pool.length, charmCount: pool.length, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: pool, orders: [...new Set(pool.map(p => p.split('_')[0]))], verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] }, sheetIndex: 1, page: 1, updatedAt: ts, createdAt: ts, ...rest });
  };
  const file = (sheetId, setId) => ({ sheetId, sheet: sheetId, path: `${setId}/${sheetId}-qr.png`, url: image, payload: 'p-' + sheetId, orders: [] });
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-live', sheetIds, materials: ['gold', 'silver'], orders: {}, status: 'open', group: 'dispatch', labelFiles: [], updatedAt: ts, createdAt: ts, ...extra });
  const committed = (setId, sheetIds, extra = {}) => mkSet(setId, sheetIds, { runId: 'run-old', status: 'complete', group: 'dispatch', committedAt: now - 9000, committed: [], labelFiles: sheetIds.map(i => file(i, setId)), labels: { pdf: { path: 'x.pdf', url: 'u' } }, processSeals: [{ how: 'approved', by: 'Ann', at: now - 5000 }], ...extra });
  const seal = { processSeals: [{ how: 'approved', by: 'Ann', at: now - 6000 }], processReady: true };
  const inSet = (setId, seq, idx, extra = {}) => ({ setId, setSeq: seq, sheetIndex: idx, ...extra });

  // ── the scene ──
  // Set 1: the OPEN set of the run that is open on this screen (image 2): GF Sheet 1 is ready; SS Sheet 1 is at Engraving step 2 (its back engraving is not approved)
  mkSet('set-1', ['gf-1', 'ss-1']);
  mk('gf-1', 'gold', inSet('set-1', 1, 1, { releaseFull: true }));
  mk('ss-1', 'silver', inSet('set-1', 1, 1, { releaseFull: true, needsBack: true }));
  // a spare full SS sheet of the open run, waiting as a draft (it can be put in Set 1 to give it a second completed SS sheet)
  mk('ss-2', 'silver', { releaseFull: true, draft: true, sheetIndex: null });
  // Set 2: committed (sent to the station), in Laser cutting: two completed GF, two completed SS, one more SS that is still filling, one GF that is laser cut
  committed('set-2', ['g2-a', 'g2-b', 's2-a', 's2-b', 's2-f']);
  mk('g2-a', 'gold', { runId: 'run-old', ...inSet('set-2', 2, 1, { releaseFull: true }), ...seal });
  mk('g2-b', 'gold', { runId: 'run-old', ...inSet('set-2', 2, 2, { releaseFull: true }), ...seal });
  mk('s2-a', 'silver', { runId: 'run-old', ...inSet('set-2', 2, 1, { releaseFull: true }), ...seal });
  mk('s2-b', 'silver', { runId: 'run-old', ...inSet('set-2', 2, 2, { releaseFull: true }), ...seal });
  mk('s2-f', 'silver', { runId: 'run-old', ...inSet('set-2', 2, 3), ...seal });                                       // still filling: not a Completed sheet
  // Set 3: committed, one GF and one SS only: neither may leave (the core principle), and a set of two keeps both
  committed('set-3', ['g3-gf', 's3-ss']);
  mk('g3-gf', 'gold', { runId: 'run-old', ...inSet('set-3', 3, 1, { releaseFull: true }), ...seal });
  mk('s3-ss', 'silver', { runId: 'run-old', ...inSet('set-3', 3, 1, { releaseFull: true }), ...seal });
  // Set 4: committed; GF Sheet 1 and SS Sheet 1 share order 4171450075 (rule A); a second completed GF and SS keep the principle out of it; one sheet is laser cut (rule B)
  committed('set-4', ['g4-a', 'g4-b', 's4-a', 's4-b', 'g4-cut', 'rg4-cut']);
  mk('g4-a', 'gold', { runId: 'run-old', order: '4171450075', pools: ['4171450075_1_1', '4171450099_1_1'], ...inSet('set-4', 4, 1, { releaseFull: true }), ...seal });
  mk('s4-a', 'silver', { runId: 'run-old', order: '4171450075', pools: ['4171450075_2_1'], ...inSet('set-4', 4, 1, { releaseFull: true }), ...seal });
  mk('g4-b', 'gold', { runId: 'run-old', ...inSet('set-4', 4, 2, { releaseFull: true }), ...seal });
  mk('s4-b', 'silver', { runId: 'run-old', ...inSet('set-4', 4, 2, { releaseFull: true }), ...seal });
  mk('g4-cut', 'gold', { runId: 'run-old', ...inSet('set-4', 4, 3, { releaseFull: true }), ...seal, laserDoneAt: now - 2000, laserDoneBy: 'Ben' });
  mk('rg4-cut', 'rose', { runId: 'run-old', ...inSet('set-4', 4, 1), ...seal, roseCutAt: now - 3000, roseStockId: 'stock-1', rosePlanHash: 'h-1' });
  // Set 5: committed, a single sheet (a set is never emptied)
  committed('set-5', ['only-5']); mk('only-5', 'gold', { runId: 'run-old', ...inSet('set-5', 5, 1, { releaseFull: true }), ...seal });
  // loose sheets of the open run: a partial Rose Gold sheet without its line, a draft that is not ready
  mk('rg-new', 'rose', { draft: true, roseStockId: 'stock-2', sheetIndex: null });
  mk('d-notready', 'gold', { draft: true, sheetIndex: null, needsBack: true });
  st.put(RUN, 'run-live', { runId: 'run-live', status: 'review', lines }); st.put(RUN, 'run-old', { runId: 'run-old', status: 'complete', lines: {} });

  // ── what the page holds (fakes of the parts the browser has) ──
  const log = { leave: [], include: [], relabel: [], files: [], applied: [], sync: [] };
  let leaveFails = false;
  const pageLive = {};                                                     // sheetId -> what SheetWin.joinInfo says
  const setList = () => st.list(SET).map(d => ({ ...d, setId: d._id || d.setId }));
  const recOf = id => st.doc(S, id);
  const membersOf = setId => st.list(S).filter(d => d.setId === setId && !d.draft && d.solidIncluded !== false && !d.archived);
  let gates = {}, snap = { sheets: {}, sets: {} };                         // gates: set id -> the card's one gate, as op flowState answers it (what the Approve button's grey line says); snap: the records as the page reads them
  const readGates = async ids => {
    const r = await post({ op: 'flowState', sheetIds: ids, setIds: [] }); gates = r.gates || {};
    snap = { sheets: Object.fromEntries((r.sheets || []).map(x => [x.id, x])), sets: Object.fromEntries((r.setDocs || []).map(x => [x.setId, x])) };
  };
  const zonesOf = async id => { await readGates([id]); return LF.explainTargets({ kind: 'sheet', id }); };
  LF.configure({
    api: post, employee: () => 'Paul', rows: () => [],
    live: id => pageLive[id] || null,
    include: async (id, o) => { log.include.push({ id, ...o }); const set = o.setId || 'set-1'; st.put(S, id, { draft: false, setId: set, setSeq: 1, sheetIndex: 5 }); st.put(SET, set, { sheetIds: [...new Set([...(st.doc(SET, set).sheetIds || []), id])] }); },
    remakeLabel: async () => {},
    leave: async (id, o) => {                                              // the open run's own assembly: the sheet is a draft, the set made again without it
      log.leave.push({ id, ...o });
      if (leaveFails) throw new Error('The sheet could not be taken out of the set: the run keeps it');
      const was = st.doc(S, id).setId; st.put(S, id, { draft: true, setId: null, setSeq: null, sheetIndex: null, label: null });
      st.put(SET, was, { sheetIds: (st.doc(SET, was).sheetIds || []).filter(x => x !== id) });
    },
    relabelSet: async s => { log.relabel.push(s); }, setFiles: async s => { log.files.push(s.setId); }, applyMembership: async m => { log.applied.push(m); },
    // the dock (page hooks the engine asks): where the card is, which set it is in, the sets on screen, the records, the card's own grey reason
    areaOf: it => { const v = LF.core.view(snap, it.kind, it.id); return v.error ? null : LF.core.areaOf(v); },
    setOf: it => { const d = recOf(it.id); return d && d.setId && !d.draft ? d.setId : null; },
    sets: () => setList(), rec: id => { const d = recOf(id); return d ? { ...d, id } : null; }, members: id => membersOf(id).map(d => ({ ...d, id: d._id || d.id })),
    notReady: it => { const d = it.kind === 'sheet' ? recOf(it.id) : null; const g = d && d.setId ? gates[d.setId] : null; return g && !g.ready ? g.reason : ''; }
  });
  const all = () => new Map([...st.docs.entries()].map(([k, v]) => [k, JSON.stringify(v)]));
  // a refusal (a plan with a need, or a refused step) writes no sheet, set, order note or seal; the handler's own revision counters are not records
  const wrote = (before, after) => [...new Set([...before.keys(), ...after.keys()])].filter(k => before.get(k) !== after.get(k) && !k.startsWith('Charm_Nest_Rev/'));
  const held = id => !!(recOf(id).laserHold && recOf(id).laserHold.at);
  const zoneBy = (zs, key) => zs.find(z => (key === 'progress' || key === 'laser' || key === 'completed') ? z.area === key : key === 'new' ? z.newSet : z.set === key);
  const refuses = async (what, item, to, key, re) => {
    const before = all(), calls = st.calls.length, p = await LF.plan({ kind: 'sheet', id: item, to });
    assert.equal(p.ok, false, what + ' → ' + JSON.stringify(p.needs)); assert(p.needs.some(x => x.key === key), what + ': ' + JSON.stringify(p.needs.map(x => x.key)));
    const line = p.needs.find(x => x.key === key); assert.match(line.label + ' ' + line.detail, re, what + ': ' + line.label);
    assert.deepEqual(p.steps, [], what + ': nothing to run');
    const c = await LF.commit(p, { by: 'Paul', confirmed: p.confirm.map(x => x.key) }); assert.equal(c.ok, false, what + ': commit refuses too');
    assert.deepEqual(wrote(before, all()), [], what + ': a refusal writes nothing'); assert(!st.calls.slice(calls).some(x => x.op === 'flowApply'), what + ': no step was sent to the server');
    return p;
  };
  const NOT_VAGUE = /^(already in (in progress|laser cutting|completed)|in process|this sheet cannot go there|no\b)/i;
  try {
    await readGates(['gf-1', 'ss-1']);
    // ═══ 1. IMAGE 2: the SS sheet of Set 1, at Engraving step 2, dragged to every place ═══
    pageLive['ss-1'] = pageLive['gf-1'] = { runHere: true, draft: false, dispatchSetId: 'set-1', can: { ok: false, reason: 'It is already in a set.' }, split: [] };
    let zs = await zonesOf('ss-1');
    let z = zoneBy(zs, 'progress');
    assert.equal(z.ok, false, 'Set 1 has no other completed SS sheet'); assert.equal(z.leaveSet, true); assert.equal(z.sub, 'out of Set 1');
    assert.equal(z.reason, 'Set 1 needs a completed SS sheet, and SS Sheet 1 is the only one it has.', 'In progress: the core principle, in plain words (image 2 said "Already in In progress")');
    z = zoneBy(zs, 'laser'); assert.equal(z.ok, false); assert.equal(z.reason, 'SS Sheet 1 · back engravings 0 of 1', 'Laser cutting: the card\'s own grey line, at the drop place');
    z = zoneBy(zs, 'completed'); assert.equal(z.ok, false); assert.equal(z.reason, 'SS Sheet 1 · back engravings 0 of 1', 'Completed: the same line (a sheet is cut only after it is approved)');
    z = zoneBy(zs, 'set-1'); assert.equal(z.ok, false); assert.equal(z.reason, 'It is already in Set 1.');
    z = zoneBy(zs, 'new'); assert.equal(z.ok, false); assert.equal(z.reason, 'Set 1 needs a completed SS sheet, and SS Sheet 1 is the only one it has.');
    z = zoneBy(zs, 'set-2'); assert.equal(z.ok, false); assert.equal(z.reason, 'Set 1 needs a completed SS sheet, and SS Sheet 1 is the only one it has.', 'another set: it cannot leave Set 1');
    for (const x of zs) assert(!NOT_VAGUE.test(x.reason || 'fine'), 'no vague word: ' + x.reason);
    // the same through the plan (what the Moving bar shows when it is dropped)
    let p = await refuses('image 2: In progress', 'ss-1', { area: 'progress' }, 'setPrinciple', /needs a completed SS sheet/);
    assert.equal(p.needs[0].label, 'Set 1 needs a completed SS sheet, and SS Sheet 1 is the only one it has'); assert(/at least 1 completed GF sheet and 1 completed SS sheet/.test(p.needs[0].detail));
    p = await refuses('image 2: Laser cutting', 'ss-1', { area: 'laser' }, 'setGate', /back engravings 0 of 1/);
    p = await refuses('image 2: Completed', 'ss-1', { area: 'completed' }, 'setGate', /back engravings 0 of 1/);
    p = await LF.plan({ kind: 'sheet', id: 'ss-1', to: { set: 'set-1' } }); assert.equal(p.noop, true); assert(p.notes.some(x => x === 'SS Sheet 1 is already in Set 1.'));
    p = await LF.plan({ kind: 'sheet', id: 'ss-1', to: { newSet: true } }); assert.equal(p.noop, true, 'its own set is the open one: a new set would be the same set'); assert(p.notes.some(x => /already in Set 1, the set the run is making/.test(x)));
    await refuses('image 2: another set', 'ss-1', { set: 'set-2' }, 'setPrinciple', /needs a completed SS sheet/);
    // the same sheet, seen on a screen that does not hold the run: only the plain truth
    delete pageLive['ss-1'];
    p = await LF.plan({ kind: 'sheet', id: 'ss-1', to: { area: 'progress' } }); assert.equal(p.needs[0].key, 'runElsewhere'); assert.equal(p.needs[0].label, "Set 1's run is open on another screen", 'a screen that does not hold the run says so, in plain words');
    pageLive['ss-1'] = pageLive['gf-1'] = { runHere: true, draft: false, dispatchSetId: 'set-1', can: { ok: false, reason: 'It is already in a set.' }, split: [] };

    // ═══ 2. the same sheet when Set 1 has another completed SS sheet: FREE ═══
    st.put(S, 'ss-2', { draft: false, setId: 'set-1', setSeq: 1, sheetIndex: 2 }); st.put(SET, 'set-1', { sheetIds: ['gf-1', 'ss-1', 'ss-2'] });
    pageLive['ss-2'] = { ...pageLive['ss-1'] }; await readGates(['gf-1', 'ss-1', 'ss-2']);
    zs = await zonesOf('ss-1');
    z = zoneBy(zs, 'progress'); assert.equal(z.ok, true, 'In progress takes it out of Set 1'); assert.equal(z.leaveSet, true); assert.equal(z.reason, ''); assert.equal(z.sub, 'out of Set 1');
    assert.equal(zoneBy(zs, 'laser').ok, false); assert.equal(zoneBy(zs, 'new').ok, false);
    assert.equal(zoneBy(zs, 'set-2').ok, true, 'a committed set takes it'); assert.equal(zoneBy(zs, 'set-1').reason, 'It is already in Set 1.');
    p = await LF.plan({ kind: 'sheet', id: 'ss-1', to: { area: 'progress' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.equal(p.exit, 'run'); assert.deepEqual(p.confirm.map(c => c.key), ['leaveSet']); assert.deepEqual(p.steps.map(x => x.type), ['setMember', 'runLeave']);
    assert.deepEqual(p.steps[0].moves, [{ sheetId: 'ss-1', to: null, run: true }]); assert.deepEqual(p.steps[0].owned, ['run-live']);
    assert(p.auto.some(a => a.key === 'hold') && p.auto.some(a => a.key === 'membership' && a.label === 'SS Sheet 1 taken out of Set 1'), JSON.stringify(p.auto.map(a => a.label)));
    let before = all(); await LF.plan({ kind: 'sheet', id: 'ss-1', to: { area: 'progress' } }); assert.deepEqual(wrote(before, all()), [], 'planning writes nothing');
    // the run lets go first-time-wrong: the hold the server wrote is lifted again, the sheet stays where the run left it, the person is told
    leaveFails = true;
    let c = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet'] });
    assert.equal(c.ok, false); assert(/could not be taken out of the set/.test(c.error), c.error); assert(!held('ss-1'), 'the hold is lifted again when the run cannot let it go'); assert.equal(recOf('ss-1').setId, 'set-1');
    leaveFails = false;
    c = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet'] });
    assert.equal(c.ok, true, JSON.stringify(c)); assert.deepEqual(log.leave.map(x => x.id).slice(-1), ['ss-1']); assert.equal(log.leave.at(-1).setId, 'set-1');
    let g = recOf('ss-1');
    assert.equal(g.draft, true); assert.equal(g.setId || null, null); assert.equal(g.releaseFull, true, 'a full sheet taken out is still a Completed sheet');
    assert(held('ss-1') && /^Taken out of Set 1/.test(g.laserHold.note) && g.laserHold.by === 'Paul', 'back to In progress as a hold: the open run does not pull it into a set by itself');
    assert.equal(g.flowHistory.at(-1).type, 'setLeave'); assert.equal(g.flowHistory.at(-1).setId, 'set-1'); assert.equal(recOf('ss-1').processSeals, undefined, 'no seal was added to it');
    assert.deepEqual(st.doc(SET, 'set-1').sheetIds, ['gf-1', 'ss-2'], 'the set is made again without it by the run (its assembly), not by the server');
    assert.equal(log.applied.at(-1).sheets[0].run, true); assert.equal(log.applied.at(-1).sheets[0].fromSetId, 'set-1');
    assert.equal(RULES.isCompleted(g), true); assert.equal(RULES.validSet(membersOf('set-1')).ok, true, 'Set 1 keeps a completed GF and a completed SS sheet (SS Sheet 2)');
    // it comes back: dropped on Set 1 (the open set), the hold it was given is lifted; its own seals and approvals stayed
    pageLive['ss-1'] = { runHere: true, draft: true, dispatchSetId: 'set-1', can: { ok: true, byHand: true }, split: [] };
    p = await LF.plan({ kind: 'sheet', id: 'ss-1', to: { set: 'set-1' } }); assert.equal(p.ok, true, JSON.stringify(p.needs));
    c = await LF.commit(p, { by: 'Paul', confirmed: p.confirm.map(x => x.key) }); assert.equal(c.ok, true, JSON.stringify(c)); assert.equal(log.include.at(-1).id, 'ss-1');
    assert.equal(recOf('ss-1').setId, 'set-1'); assert(!held('ss-1'), 'the hold taking it out made is lifted when it is put back'); assert.deepEqual(st.doc(SET, 'set-1').sheetIds.sort(), ['gf-1', 'ss-1', 'ss-2']);

    // ═══ 3. a sheet that is simply ready (in no set): In progress <-> Laser cutting <-> Completed are all free, both ways, each step keeping every seal ═══
    delete pageLive['ss-1'];
    mk('solo-ok', 'gold', { releaseFull: true, ...seal });
    const step = async (to, area, keys) => {
      const q = await LF.plan({ kind: 'sheet', id: 'solo-ok', to: { area: to } });
      assert.equal(q.ok, true, `${area} → ${to}: ` + JSON.stringify(q.needs)); assert.deepEqual(q.needs, []);
      const k = await LF.commit(q, { by: 'Paul', confirmed: q.confirm.map(x => x.key) }); assert.equal(k.ok, true, JSON.stringify(k));
      assert.equal((await LF.plan({ kind: 'sheet', id: 'solo-ok', to: { area: 'nowhere' } })).from.area, to, `now in ${to}`);
      assert.deepEqual(recOf('solo-ok').processSeals.slice(0, 1).map(x => x.how + ':' + x.by), ['approved:Ann'], 'a seal it had stays');
    };
    zs = await zonesOf('solo-ok');
    assert.equal(zoneBy(zs, 'laser').ok, false); assert.equal(zoneBy(zs, 'laser').reason, 'It is already in Laser cutting.'); assert.equal(zoneBy(zs, 'progress').ok, true); assert.equal(zoneBy(zs, 'completed').ok, true);
    await step('completed', 'laser'); await step('laser', 'completed'); await step('progress', 'laser'); assert(held('solo-ok'), 'back to In progress is a hold, as Paul asked'); await step('laser', 'progress');
    assert(!held('solo-ok'), 'and a drop on Laser cutting lifts it');
    // ═══ 4. REFUSALS, each with one plain line and nothing written ═══
    // (a) the core principle: the only completed GF / SS sheet of a committed set cannot leave it, and cannot go to another set either
    await readGates([]);
    await refuses('(a) the only completed GF of Set 3', 'g3-gf', { area: 'progress' }, 'setPrinciple', /Set 3 needs a completed GF sheet, and GF Sheet 1 is the only one it has/);
    await refuses('(a) the only completed SS of Set 3, to another set', 's3-ss', { set: 'set-2' }, 'setPrinciple', /Set 3 needs a completed SS sheet/);
    zs = await zonesOf('g3-gf'); z = zoneBy(zs, 'progress');
    assert.equal(z.ok, false); assert.equal(z.reason, 'Set 3 needs a completed GF sheet, and GF Sheet 1 is the only one it has.');
    // the server says the same, on its own, even when a page asks it directly (two computers, a stale page)
    let r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'g3-gf', to: null }] }] });
    assert.equal(r.status, 409); assert.match(r.error, /Set 3 needs a completed GF sheet, and GF Sheet 1 is the only one it has/); assert.equal(r.reasons[0].key, 'setPrinciple');
    assert.equal(recOf('g3-gf').setId, 'set-3');
    // a set that already lacks a kind is not made worse and not stopped from being mended: s2-f (still filling) leaves Set 2 freely
    zs = await zonesOf('s2-f'); assert.equal(zoneBy(zs, 'progress').ok, true, 'a sheet that is not Completed does not count towards the principle');
    // (b) rule A: a multi-piece order must not be split over two sets
    await refuses('(b) order 4171450075', 'g4-a', { area: 'progress' }, 'sharedOrders', /Order 4171450075 has pieces on GF Sheet 1 and SS Sheet 1: they stay in one set/);
    await refuses('(b) the same, to another set', 's4-a', { set: 'set-2' }, 'sharedOrders', /Order 4171450075 has pieces on/);
    // (c) rule B: a sheet that was laser cut, or whose Rose Gold cut is recorded, stays in its set
    await refuses('(c) laser cut', 'g4-cut', { area: 'progress' }, 'sheetCompleted', /GF Sheet 3 is completed/);
    await refuses('(c) Rose Gold cut', 'rg4-cut', { area: 'progress' }, 'sheetCut', /RG Sheet 1 was already cut/);
    zs = await zonesOf('g4-cut'); assert.equal(zoneBy(zs, 'progress').reason, 'GF Sheet 3 is completed (laser cut), so it stays in Set 4.', 'one line, at the drop place');
    // (d) a set is never emptied
    await refuses('(d) the last sheet', 'only-5', { area: 'progress' }, 'lastSheet', /GF Sheet 1 is all that is in Set 5/);
    zs = await zonesOf('only-5'); assert.equal(zoneBy(zs, 'progress').reason, 'GF Sheet 1 is the only sheet of Set 5: a set is never left empty.');
    // (e) the physical-safety rules that already existed: no approval for Laser cutting with an engraving unresolved; a partial Rose Gold sheet is never sent on silently
    delete pageLive['d-notready']; pageLive['d-notready'] = { runHere: true, draft: true, dispatchSetId: 'set-1', can: { ok: true, byHand: true }, split: [] };
    p = await LF.plan({ kind: 'sheet', id: 'd-notready', to: { area: 'laser' } }); assert.equal(p.ok, false); assert(p.needs.some(x => x.key === 'engraving' && /back engraving not approved/.test(x.label)), JSON.stringify(p.needs.map(x => x.key)));
    before = all(); c = await LF.commit(p, { by: 'Paul', confirmed: p.confirm.map(x => x.key) }); assert.equal(c.ok, false); assert.deepEqual(wrote(before, all()), [], 'nothing written for an unready sheet');
    delete pageLive['d-notready'];
    p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { area: 'laser' } });
    assert(p.confirm.some(x => x.key === 'roseLine'), 'a partial Rose Gold sheet asks for its green dash line (a yes) first: it is never moved silently');

    // ═══ 5. FREE MOVES out of a COMMITTED set (taken out; the set files remade; a set keeps its principle) ═══
    zs = await zonesOf('s2-b');
    assert.equal(zoneBy(zs, 'progress').ok, true, 'Set 2 keeps SS Sheet 1 (s2-a): s2-b may leave'); assert.equal(zoneBy(zs, 'progress').leaveSet, true);
    p = await LF.plan({ kind: 'sheet', id: 's2-b', to: { area: 'progress' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.equal(p.exit, 'committed'); assert.deepEqual(p.steps.map(x => x.type), ['setMember', 'setFiles']); assert.deepEqual(p.confirm.map(x => x.key), ['leaveSet']);
    c = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet'] }); assert.equal(c.ok, true, JSON.stringify(c));
    g = recOf('s2-b'); assert.equal(g.draft, true); assert.equal(g.setId || null, null); assert.equal(g.releaseFull, true, 'still a Completed sheet'); assert(held('s2-b')); assert.deepEqual(g.processSeals, [{ how: 'approved', by: 'Ann', at: g.processSeals[0].at }], 'its seal stays');
    assert.deepEqual(st.doc(SET, 'set-2').sheetIds, ['g2-a', 'g2-b', 's2-a', 's2-f']); assert.equal(st.doc(SET, 'set-2').committedAt > 0, true, 'Set 2 stays committed'); assert.deepEqual(log.files.slice(-1), ['set-2'], 'its labels PDF and manifest are made again');
    assert.equal(RULES.validSet(membersOf('set-2')).ok, true, 'Set 2 still has a completed GF and a completed SS sheet');
    // now s2-a is its only completed SS sheet: it may not follow
    await refuses('(a) after that, SS Sheet 1 is Set 2\'s only completed SS', 's2-a', { area: 'progress' }, 'setPrinciple', /Set 2 needs a completed SS sheet, and SS Sheet 1 is the only one it has/);
    // Laser cutting / In progress / Completed for the sheet that came out (a draft that is full and ready): the hold is released on a drop on Laser cutting (existing rule)
    p = await LF.plan({ kind: 'sheet', id: 's2-b', to: { area: 'laser' } }); assert(p.steps.some(x => x.type === 'release') && p.needs.length > 0, 'a drop on Laser cutting lifts the hold (existing rule) when the sheet is ready; this one is not in a set yet, so it says what it lacks');

    // ═══ 6. between sets: out of the set the run is making into a COMMITTED set; out of a committed set into the OPEN set ═══
    pageLive['ss-1'] = { runHere: true, draft: false, dispatchSetId: 'set-1', can: { ok: false, reason: 'It is already in a set.' }, split: [] };
    zs = await zonesOf('ss-1'); assert.equal(zoneBy(zs, 'set-2').ok, true);
    p = await LF.plan({ kind: 'sheet', id: 'ss-1', to: { set: 'set-2' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.steps.map(x => x.type), ['setMember', 'runLeave', 'setMember', 'relabel', 'setFiles']);
    assert.deepEqual(p.steps[0].moves, [{ sheetId: 'ss-1', to: null, run: true }]); assert.deepEqual(p.steps[2].moves, [{ sheetId: 'ss-1', to: 'set-2' }]);
    c = await LF.commit(p, { by: 'Paul', confirmed: p.confirm.map(x => x.key) }); assert.equal(c.ok, true, JSON.stringify(c));
    assert.equal(recOf('ss-1').setId, 'set-2'); assert.equal(recOf('ss-1').draft, false); assert(!held('ss-1'), 'no hold is left on a sheet that is in a set'); assert(st.doc(SET, 'set-2').sheetIds.includes('ss-1'));
    assert.deepEqual(st.doc(SET, 'set-1').sheetIds, ['gf-1', 'ss-2'], 'Set 1 goes on without it'); assert(log.relabel.some(x => x.sheetIds.includes('ss-1')), 'its QR label is made for the set it joined');
    delete pageLive['ss-1'];
    // a sheet of a committed set into the open set: it leaves (hold), the run takes it in as it would any full sheet, the hold is lifted
    pageLive['s2-a'] = { runHere: false, sent: true, draft: false, dispatchSetId: 'set-1', can: { ok: true, byHand: true, reason: '' }, split: [] };
    pageLive['g2-b'] = { runHere: false, sent: true, draft: false, dispatchSetId: 'set-1', can: { ok: true, byHand: true, reason: '' }, split: [] };
    p = await LF.plan({ kind: 'sheet', id: 'g2-b', to: { set: 'set-1' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.steps.map(x => x.type), ['setMember', 'setFiles', 'include', 'release']); assert.deepEqual(p.confirm.map(x => x.key), ['leaveSet']);
    c = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet'] }); assert.equal(c.ok, true, JSON.stringify(c));
    assert.equal(recOf('g2-b').setId, 'set-1'); assert(!held('g2-b'), 'the hold is lifted once it is in Set 1'); assert.equal(log.include.at(-1).id, 'g2-b'); assert(!st.doc(SET, 'set-2').sheetIds.includes('g2-b'));
    assert.equal(st.doc(SET, 'set-2').committedAt > 0, true, 'Set 2 stays committed');
    // ...but not when that would break Set 2 (its only completed GF is g2-a now)
    zs = await zonesOf('g2-a'); assert.equal(zoneBy(zs, 'set-1').ok, false); assert.match(zoneBy(zs, 'set-1').reason, /Set 2 needs a completed GF sheet/);
    // a screen that does not hold the run cannot start a new set from a sheet: the plain reason, not "committed"
    delete pageLive['s2-a']; zs = await zonesOf('s2-a'); assert.equal(zoneBy(zs, 'new').reason, 'Only a sheet of the open run can start a new set.', 'SS Sheet 1 may leave Set 2 now (SS Sheet 4 is there), but this screen does not hold its run');
    zs = await zonesOf('g2-a'); assert.equal(zoneBy(zs, 'new').reason, 'Set 2 needs a completed GF sheet, and GF Sheet 1 is the only one it has.');

    // ═══ 7. the server's own check of the run: a set an OPEN run makes is changed by the screen that holds the run ═══
    st.put(S, 'ss-9', { id: 'ss-9', runId: 'run-live', metal: 'silver', draft: false, setId: 'set-1', setSeq: 1, sheetIndex: 9, releaseFull: true, placedCount: 1, poolIds: ['3999999999_1_1'], orders: ['3999999999'], verification: { ok: true } });
    st.put(SET, 'set-1', { sheetIds: ['gf-1', 'ss-2', 'ss-9'] });
    before = all(); r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'ss-9', to: null, run: true }] }] });
    assert.equal(r.status, 409); assert.match(r.error, /Set 1 is still being made by its run/); assert.deepEqual(wrote(before, all()), [], 'the other screen\'s edit is refused whole');
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'ss-9', to: null, run: true }], owned: ['run-live'] }] });
    assert.equal(r.status, 200, JSON.stringify(r)); assert(held('ss-9')); assert.equal(recOf('ss-9').setId, 'set-1', 'the server left the sheet\'s membership to the run'); assert.deepEqual(st.doc(SET, 'set-1').sheetIds, ['gf-1', 'ss-2', 'ss-9'], 'and the set record too');
    assert.equal(recOf('ss-9').flowHistory.at(-1).type, 'setLeave');
    // ...and a run that is not open has no owner: the same edit is a plain edit of the records
    st.put(RUN, 'run-live', { status: 'complete' });
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'ss-9', to: null }] }] }); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(recOf('ss-9').draft, true);
    assert.deepEqual(st.doc(SET, 'set-1').sheetIds, ['gf-1', 'ss-2'], 'a set whose run is finished is changed by the records');
    st.put(RUN, 'run-live', { status: 'review' });

    // ═══ 8. no nested array is ever written (Firestore refuses them), and the record of every seal and cut is as it was ═══
    const nested = (v, path) => Array.isArray(v) ? (v.some(x => Array.isArray(x)) ? path : v.map((x, i) => nested(x, path + '[' + i + ']')).find(Boolean)) : v && typeof v === 'object' ? Object.entries(v).map(([k, x]) => nested(x, path + '.' + k)).find(Boolean) : undefined;
    for (const [k, v] of st.docs) { const bad = nested(JSON.parse(v ? JSON.stringify(v) : 'null'), k); assert(!bad, 'a nested array at ' + bad); }
    assert.equal(recOf('g4-cut').laserDoneAt, now - 2000); assert.equal(recOf('rg4-cut').roseCutAt, now - 3000); assert.equal(recOf('g4-cut').processSeals.length, 1);
    assert(SE.principleReasons, 'the page and the server read the same rule file');
    console.log('PASS: Library drag and drop is free unless a rule is broken: image 2 (the SS sheet of the open Set 1) answers every place with a plain reason or goes through; out of a set, between sets and into the open set are free; refused only by the core principle, a split multi-piece order, a laser-cut sheet, an emptied set or an approval rule; refusals write nothing');
  } finally { await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
