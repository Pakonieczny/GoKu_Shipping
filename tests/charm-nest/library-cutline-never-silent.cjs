// GF1 (Paul, 7 Oct 2026, 14:13 UTC): "The system did not prompt me about the Green cut line when dragging the 14k gold sheet into Laser Cutting.
// The sheet moved but now has this error." (14K Sheet 1, 13 of 13 charms, 36% full: Nesting blocked, "The green dash line has not been calculated: press Cut Sheet".)
// A partial 10K / 14K / Rose Gold sheet that is moved to Laser cutting must NEVER go on without the green dash line: either the one `roseLine` yes is asked
// (the line can be made) or the move is refused with ONE plain reason that names the sheet and the one place to fix it. Nothing is written in either case.
// This runs the REAL chain over a 14K sheet like Paul's (13 charms, partial, in a set, held In progress): LibraryFlow.plan / commit over the fake shop (the real
// charmNestLibrary handler on an in-memory Firestore), the real LibraryFlowRose.check, RoseStock and CharmNestReadiness in a jsdom page.
//   S1 live sheet that holds its physical sheet (blocked on its line)  S2 live sheet with no physical sheet yet   -> the yes is asked
//   S3 saved record only (not open on the Nest tab)                     -> one plain refusal
//   S4 the sheet cannot be read by the check                            -> one plain refusal, commit seals nothing   (before GF1: the move went on and sealed)
//   S5 a saved record with a stale protected-line copy and no saved line -> one plain refusal                          (before GF1: "already has its line")
//   S6 a live sheet the check calls full while its record still waits for the line (readiness blocks it)             -> one plain refusal  (before GF1: the plan said ok)
//   S7 a set of two: the sheet moved is open, its mate is not            -> one plain refusal naming the mate        (before GF1: only the first was asked about)
//   S8 the one shared test: CharmNestRose.owesLine / holdsLine, and readiness's lineMissing and flow's needsCutLine answer the same
//   S9 the yes (cutLine) makes the line for every sheet the window listed, not only the one dragged
//   node tests/charm-nest/library-cutline-never-silent.cjs   (jsdom: NODE_PATH=<node_modules with jsdom> when it is not installed here)
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('library-cutline-never-silent: SKIPPED (jsdom is not installed)'); process.exit(0); }
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const S = 'Charm_Nest_Sheets', RUN = 'Charm_Nest_Runs', SET = 'Charm_Nest_Sets';
const rect = (w, h) => ({ subpaths: [[['m', [0, 0]], ['l', [w, 0]], ['l', [w, h]], ['l', [0, h]], ['h']]] });
const A = 'sheet-14k-a-0001', B = 'sheet-14k-b-0002';
const same = (a, b, msg) => assert.equal(JSON.stringify(a), JSON.stringify(b), msg);   // (the page's arrays and objects come from the jsdom realm: compare their JSON)

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-07';
  try {
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    // Paul's sheet: 13 charms in a block on the left of a partial 14K sheet (300 x 280 pt)
    let size = { w: 300, h: 280 }, charms = [], placements = [];
    const layout = (kind) => {
      charms = []; placements = [];
      if (kind === 'full') { size = { w: 100, h: 100 }; [[26, 26], [76, 26], [26, 76], [76, 76]].forEach(([x, y], i) => { charms.push({ id: 'f' + i, outline: rect(46, 46), centerPt: [23, 23], members: [] }); placements.push({ id: 'f' + i, cxPt: x, cyPt: y, angle: 0, scale: 1 }); }); return; }
      size = { w: 300, h: 280 };
      for (let i = 0; i < 13; i++) { const c = i % 3, r = Math.floor(i / 3); charms.push({ id: 'c' + i, outline: rect(44, 44), centerPt: [22, 22], members: [] }); placements.push({ id: 'c' + i, cxPt: 26 + c * 50, cyPt: 26 + r * 50, angle: 0, scale: 1 }); }
    };
    const pools = [], lines = {};
    for (let i = 0; i < 13; i++) { const order = String(3900000100 + i), pool = order + '_1_1'; lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], engraveCandidate: false }; pools.push(pool); }
    const orders = pools.map(p => p.split('_')[0]);
    st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines });
    // a sheet held In progress (laserHold: a person moved it back), so a move to Laser cutting has to lift the hold: it is In progress, not already ready
    const rec = (id, extra = {}) => st.put(S, id, { id, runId: 'run-x', metal: 'gold14k', day, status: 'complete', placedCount: 13, charmCount: 13, density: .36, stock: { wIn: size.w / 72, hIn: size.h / 72 }, poolIds: pools, orders, verification: { ok: true }, setId: 'set-1', draft: false,
      laserHold: { at: now - 1000, by: 'Paul' }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      label: { files: [{ path: id + '-qr.png', url: image, payload: 'x', orders }], orders }, sheetIndex: id === B ? 2 : 1, updatedAt: ts, createdAt: ts, ...extra }, false);
    const setOf = ids => st.put(SET, 'set-1', { setId: 'set-1', seq: 1, runId: 'run-x', day, status: 'complete', sheetIds: ids, updatedAt: ts, createdAt: ts }, false);

    const dom = new JSDOM('<!doctype html><html><head></head><body></body></html>', { url: 'https://example.test', runScripts: 'outside-only', pretendToBeVisual: true });
    const w = dom.window; w.fetch = fetch; w.IntersectionObserver = class { observe() { } unobserve() { } };
    let live = [], readFails = false;
    w.CN = { S: { cloud: { ok: true }, settings: { stock: {} }, mode: 'library', library: { rows: [] } }, esc: s => String(s), uid: () => 'u', allSheets: () => live, pagesOf: () => live, drawPreview() { }, renderCard() { }, toast() { }, sheetDirty() { }, flushManualIntake() { },
      stockFor: (m, sh) => { const k = sh && sh.roseStock; return k && k.wPt ? { wPt: k.wPt, hPt: k.hPt, wIn: k.wPt / 72, hIn: k.hPt / 72 } : { wPt: size.w, hPt: size.h, wIn: size.w / 72, hIn: size.h / 72 }; },
      api: async (name, b) => { if (readFails && b.op === 'getSheet') throw new Error('The cloud did not answer'); return post(b); } };
    for (const f of ['charm-nest-rose.js', 'charm-nest-readiness.js', 'charm-nest-flow.js']) w.eval(fs.readFileSync(path.join(root, f), 'utf8'));
    w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:{runId:'run-x'}};var Session=window.Session={schedule(){}};`);
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-rose-ui.js'), 'utf8'));
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-flow-rose.js'), 'utf8'));
    const LF = w.LibraryFlow, CR = w.CharmNestRose, RD = w.CharmNestReadiness;
    LF.configure({ api: post, employee: () => 'Paul', rows: () => [], live: () => null });
    assert(w.RoseStock && w.LibraryFlowRose && LF && CR && RD);
    const liveSheet = (id, extra = {}) => ({ sheetId: id, metal: 'gold14k', page: id === B ? 2 : 1, charms, placements, persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, draft: false, setId: 'set-1', runId: 'run-x', ...extra });
    const stockOf = () => ({ id: 'rgs-1', metal: 'gold14k', wPt: size.w, hPt: size.h, revision: 0, profileJson: null });
    const writes = () => st.calls.filter(c => ['flowApply', 'roseClaim', 'rosePlan', 'roseRecordCut', 'laserDone'].includes(c.op) || (c.op === 'laserStatus' && c.body && c.body.recordSeals)).length;
    const move = (id = A) => LF.plan({ kind: 'sheet', id, to: { area: 'laser' } });
    // a refusal is ONE plain reason that names the sheet and the one place to fix it; the plan can still be read (needs), and it has no yes to give
    const refused = (p, name, why, button = true) => {
      assert.equal(p.ok, false, why + ': the move is refused'); same(p.confirm.map(c => c.key), [], why + ': no yes is offered for a line that cannot be made');
      assert.equal(p.needs.length, 1, why + ': ONE reason: ' + JSON.stringify(p.needs.map(n => n.label)));
      const text = p.needs[0].label + ' ' + p.needs[0].detail; assert(text.includes(name) && /Nest tab/.test(text) && (!button || /Cut Sheet/.test(text)), why + ': it names the sheet and the one place to fix it: ' + text);
    };

    // ── S1 · live, holds its physical sheet (readiness blocks it): the one yes is asked, nothing is written ──
    layout('partial'); setOf([A]); rec(A, { roseStockId: 'rgs-1', roseRevision: 0 }); live = [liveSheet(A, { roseStock: stockOf(), roseRevision: 0 })];
    assert.equal(RD.laserSheet(st.doc(S, A)).stages.layout, false, 'Paul\'s state: readiness holds the sheet on its line (Nesting blocked)');
    let p = await move(); assert.equal(p.from.area, 'progress'); assert.equal(p.ok, true, JSON.stringify(p.needs)); same(p.confirm.map(c => c.key), ['roseLine'], 'S1 asks the one yes');
    same(p.confirm[0].sheetIds, [A]); assert.equal(p.confirm[0].sheets[0].label, '14K Sheet 1'); assert.equal(p.confirm[0].sheets[0].charms, 13);
    assert(p.steps.some(x => x.type === 'roseLine') && p.steps.some(x => x.type === 'seal')); assert.equal(writes(), 0, 'a plan writes nothing');
    // ── S2 · live, no physical sheet yet ──
    rec(A, { roseStockId: null }); live = [liveSheet(A)];
    p = await move(); same(p.confirm.map(c => c.key), ['roseLine'], 'S2 asks the one yes'); assert.equal(p.needs.length, 0);
    // ── S3 · saved record only: the line needs the sheet's geometry, which only the Nest tab holds ──
    rec(A, { roseStockId: 'rgs-1', roseRevision: 0 }); live = [];
    p = await move(); refused(p, '14K Sheet 1', 'S3'); assert.equal(writes(), 0);
    // ── S4 · the check cannot read the sheet: it must not say "nothing to add" ──
    rec(A, { roseStockId: null }); live = []; readFails = true;
    p = await move(); refused(p, '14K Sheet 1', 'S4'); assert.equal(writes(), 0);
    const before = st.calls.length, done = await LF.commit(p, { confirmed: ['roseLine'], by: 'Paul' });
    assert.equal(done.ok, false, 'S4: commit refuses'); assert.equal(writes(), 0, 'S4: nothing was lifted, sealed or moved'); assert(!st.doc(S, A).processReady && !(st.doc(S, A).processSeals || []).length, 'S4: no ready seal on the sheet');
    assert(st.calls.slice(before).every(c => c.op === 'flowState' || c.op === 'getSheet' || c.op === 'laserStatus'), 'S4: commit only read: ' + st.calls.slice(before).map(c => c.op));
    readFails = false;
    // ── S5 · a saved record with a protected-line copy but no saved line (no plan hash): not "already has its line" ──
    rec(A, { roseStockId: 'rgs-1', roseProtectedJson: JSON.stringify({ placements: [], lines: [], profile: null }) }); live = [];
    p = await move(); refused(p, '14K Sheet 1', 'S5'); assert.equal(writes(), 0);
    // ── S6 · the check calls the live sheet full (it takes the rest of the metal whole), yet its record holds a physical sheet with no plan: readiness still blocks it ──
    layout('full'); rec(A, { roseStockId: 'rgs-1', roseRevision: 0, placedCount: 4, charmCount: 4, density: .9 }); live = [liveSheet(A, { roseStock: stockOf(), roseRevision: 0 })];
    assert.equal(w.RoseStock.addsLine(live[0]), false, 'S6 fixture: the page says there is no line to add'); assert.equal(RD.laserSheet(st.doc(S, A)).stages.layout, false, 'S6 fixture: readiness blocks the same sheet');
    p = await move(); refused(p, '14K Sheet 1', 'S6', false); assert.equal(writes(), 0);
    // a full sheet that holds nothing is not blocked by anything (it takes the rest of the metal whole): it is already in Laser cutting
    rec(A, { roseStockId: null, placedCount: 4, charmCount: 4, density: .9, laserHold: null }); live = [liveSheet(A)]; st.docs.get(S + '/' + A).laserHold = null;
    p = await move(); assert.equal(p.needs.length, 0, 'a full sheet with no physical sheet is not held for a line'); same(p.confirm.map(c => c.key), []);
    // ── S7 · a set of two: the sheet moved (A) is open, its mate (B) is not. B needs its line too, so the move is refused naming B ──
    layout('partial'); setOf([A, B]); rec(A, { roseStockId: null }); rec(B, { roseStockId: null }); live = [liveSheet(A)];
    p = await move(A); refused(p, '14K Sheet 2', 'S7'); assert.equal(writes(), 0);
    live = [liveSheet(A), liveSheet(B)]; p = await move(A);
    same(p.confirm.map(c => c.key), ['roseLine'], 'S7: both open: the one yes'); same([...p.confirm[0].sheetIds].sort(), [A, B], 'the window lists both sheets'); assert.equal(p.confirm[0].sheets.length, 2);
    st.docs.delete(SET + '/set-1'); setOf([A]);

    // ── S8 · the one shared test ──
    const facts = [{}, { roseStockId: 'x' }, { rosePlanHash: 'h' }, { roseStockId: 'x', rosePlanHash: 'h' }, { roseCutAt: 5 }, { roseStockId: 'x', roseCutAt: 5 }];
    for (const metal of ['rose', 'gold10k', 'gold14k', 'gold', 'silver']) for (const f of facts) {
      const s = { metal, ...f }, owes = CR.cuts(metal) && !f.roseCutAt && !f.rosePlanHash;
      assert.equal(CR.owesLine(s), !!owes, 'owesLine ' + JSON.stringify(s)); assert.equal(CR.holdsLine(s), !!(owes && f.roseStockId), 'holdsLine ' + JSON.stringify(s));
      assert.equal(LF.core.needsCutLine(s), !!owes, 'flow asks the same test: ' + JSON.stringify(s));
      const ready = RD.laserSheet({ id: 'x', poolIds: [], placedCount: 1, verification: { ok: true }, outputs: { ai: { url: 'a' }, preview: { url: 'p' } }, setId: 's', metal, ...f }).stages.layout;
      assert.equal(ready, !CR.holdsLine(s), 'readiness blocks exactly what holdsLine says: ' + JSON.stringify(s));
    }

    // ── S9 · the yes makes the line for every sheet the window listed ──
    const asked = []; LF.configure({ rose: () => ({ calculate: async (item, o) => { asked.push(item); return { ok: true, lines: 1, cut: true, sheets: [], warnings: [] }; } }) });
    await LF.cutLine({ kind: 'sheet', id: A, sheetIds: [A, B], by: 'Paul' });
    same([].concat(asked[0]).map(x => x.id), [A, B], 'the Cut Sheet press covers both sheets of the window');
    asked.length = 0; await LF.cutLine({ kind: 'sheet', id: A, by: 'Paul' }); same(asked[0], { kind: 'sheet', id: A }, 'one sheet: as before');

    console.log('PASS: library cut line never silent: the yes is asked for a live partial 14K sheet (with and without its physical sheet); a record, an unreadable sheet, a stale protected copy, a full sheet readiness still holds and a set mate that is not open are each refused with one plain reason and nothing is written; one shared test for readiness and the flow; the yes covers every sheet listed');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
