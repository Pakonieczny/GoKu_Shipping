// RGC1 (Paul, 9 Nov 2026): "after a cut line a sheet must continue to get filled with newer orders until the next cut line and so forth until the sheet is done and then and only then should the
// automatic system go to a brand new clean sheet. The user still should be able to override that if necessary by adding a new Custom sized sheet inside the options menu."
// The live Sandbox run of 8-9 Nov held four uncut Rose Gold drafts (Draft 3 and 4 on leftovers, Draft 5 and 6 on brand-new sheets), each sharing orders with another GF / SS unit: the intake opened a
// page for every order greenFits refused on the open layout.
//   1  the intake (the page's REAL attachPool, greenFits, greenPage, greenResume in a vm): orders of three successive sets arrive; exactly one uncut RG layout is open at any time; a refused order WAITS
//      (no page opened for it) and is placed after the cut; the next layout claims THAT cut's leftover (an exact, pending PartialNest claim); a brand-new sheet only when the leftover is used up
//   2  a waiting line is not made up again until its layout is cut (greenHeld)
//   3  a formed set never waits; Paul's Options choice (Partial Sheet: always a new sheet) is used as he set it
//   4  10K / 14K: the same continuation
//   5  the server's automatic choice (roseClaim without a stock) takes the metal's newest leftover with room before an older one or a full sheet
//   node tests/charm-nest/rg-continue.cjs      Offline: the page's own code in a vm, no browser, no network.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..'), code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const ok = (name, cond, detail) => { assert(cond, name + (detail ? ' :: ' + detail : '')); console.log('  ok  ' + name); };
const settle = async (n = 20) => { for (let i = 0; i < n; i++) await new Promise(r => setTimeout(r, 2)); };

const a = code.indexOf('  const lineCuts = m =>'), b = code.indexOf('  /* One placement of a line at a time.', a);
assert(a > 0 && b > a && code.slice(a, b).includes('function attachPool(') && code.slice(a, b).includes('function greenPage('), 'the intake code was found');
const SLICE = code.slice(a, b);

function world(o = {}) {
  const c = vm.createContext({ console, log: [], OPTS: o });
  vm.runInContext(`
const S = { sheets: { gold: { pages: [] }, silver: { pages: [] }, rose: { pages: [] }, gold14k: { pages: [] } }, settings: { nospread: 'off' } };
const B = { run: { runId: 'r', step: 'pool' }, pool: { rows: new Map() } };
const O = { stepIndex: s => ['pull', 'pool', 'nest', 'complete'].indexOf(s), pinPieces() {} };
const CharmNestRose = { cuts: m => m === 'rose' || m === 'gold10k' || m === 'gold14k' };
const rows = [];
const Orders = { rows: () => rows, render() {} };
const pagesOf = m => S.sheets[m].pages, allSheets = () => Object.values(S.sheets).flatMap(x => x.pages);
let made = 0;
function addPage(metal) { made++; const pg = { metal, page: pagesOf(metal).length + 1, charms: [], placements: [], runId: 'r', status: 'ready' }; pagesOf(metal).push(pg); return pg; }
const labelOf = m => ({ rose: 'Rose Gold', gold14k: '14K Gold', gold: 'Gold Filled', silver: 'Sterling Silver' })[m] || m;
const agent = (scope, kind, text) => log.push({ kind, text });
const settle = () => {}, update = async () => {}, charmOf = () => null, holding = () => [], renderCard = () => {}, sheetDirty = () => {};
const Gate = { modern: () => true };
const closed = p => !!p.roseCutAt || !!p.recalled || !!p.releaseFull || !!p.laserDoneAt;
const window = { LiveNest: { intakePage: (m) => pagesOf(m).at(-1) || addPage(m), closed, orderHome: () => null, lineFits: () => true, pairFits: () => true }, CN: { showPage() {} },
  PartialNest: { mode: m => (OPTS.mode && OPTS.mode[m]) || 'auto' }, Gate };
const LiveNest = window.LiveNest;
${SLICE}
async function addAll(run) { for (const r of rows.filter(r => (r.state === 'pulled' || r.state === 'waiting') && due(r))) { r.state = 'pulled'; attachPool(r, run, r._prep, [], [], []); } }
const RETRY_MS = 1, trySig = () => 'x';
const due = row => !greenHeld(row) && (['pulled', 'waiting'].includes(row.state));
this.api = { S, B, rows, attachPool, greenFits, greenResume, greenHeld, greenPage, addAll, made: () => made, pagesOf, addPage };
`, c);
  const A = c.api, run = A.B.run;
  const page = (metal, orders, set) => { const pg = A.addPage(metal); pg.draft = !set; pg.setId = set || null; pg.charms = orders.map(x => ({ order: x, id: x + metal })); pg.placements = pg.charms.map(ch => ({ id: ch.id })); return pg; };
  let n = 0;
  const line = (rid, metal = 'rose', pieces = 1) => {
    const charms = Array.from({ length: pieces }, (_, i) => ({ id: `${rid}-${metal}-${i}-${n++}`, poolId: `${rid}_${n}_${i}`, order: rid }));
    const row = { key: `${rid}|${metal}|${n}`, state: 'pulled', order: { receiptId: rid }, spec: { material: metal } };
    row._prep = { sp: { material: metal, designSku: 'X' }, pools: charms.map(ch => ({ poolId: ch.poolId })), charms, custom: false };
    A.rows.push(row); return row;
  };
  const place = row => A.attachPool(row, run, row._prep, [], [], []);
  const uncut = metal => A.pagesOf(metal).filter(p => p.charms.length && !p.roseCutAt);
  // a cut: the page's stock becomes the leftover (revision + 1), as RoseStock.record leaves it after roseRecordCut; `room: false` when the leftover holds nothing more
  let stockN = 0;
  const cut = async (p, room = true) => {
    const st = p.roseStock && p.roseStock.id ? p.roseStock : { id: 'rgs-' + (++stockN), revision: 0 };
    p.roseCutAt = Date.now() + (stockN++); p.roseStock = { id: st.id, revision: (+st.revision || 0) + 1, profileJson: '{}', available: room, owner: null, wPt: 283, hPt: 141 };
    p._partialPending = false;
    A.greenResume(p); await settle();
  };
  return { c, A, run, page, line, place, uncut, cut };
}

(async () => {
  /* ── 1 · three successive sets: one open layout, waits, continuation on the cut's leftover ── */
  for (const metal of ['rose', 'gold14k']) {
    const w = world(), { A } = w;
    // three GF + SS units (drafts, each pair sharing its own order), as the run fills them
    w.page('gold', ['g1', 'A1', 'A2']); w.page('silver', ['s1', 'A1', 'A2']);
    w.page('gold', ['g2', 'B1']); w.page('silver', ['s2', 'B1']);
    w.page('gold', ['g3', 'C1']); w.page('silver', ['s3', 'C1']);
    const madeAt0 = A.made();
    const rA1 = w.line('A1', metal), rA2 = w.line('A2', metal), rB1 = w.line('B1', metal, 2), rC1 = w.line('C1', metal);
    w.place(rA1); w.place(rA2);
    const L1 = A.pagesOf(metal)[0];
    ok(`${metal}: the first set's orders go on one layout`, A.pagesOf(metal).length === 1 && L1.charms.length === 2);
    const before = A.made();
    w.place(rB1); w.place(rC1);
    ok(`${metal}: the orders greenFits refuses there WAIT: no page is opened for them`, A.made() === before && A.pagesOf(metal).length === 1, `${A.made() - before} pages made`);
    ok(`${metal}: their lines say they wait for the green line, then continue on its leftover`, [rB1, rC1].every(r => r.state === 'waiting' && r.wait.kind === 'green' && /waits for the green line on .* then continues on its leftover/.test(r.reason)), rB1.reason);
    ok(`${metal}: and so does the layout's card`, /wait for this green line, then continue on its leftover/.test(L1.greenWaitNote || ''), L1.greenWaitNote);
    ok(`${metal}: exactly one uncut layout`, w.uncut(metal).length === 1);
    ok(`${metal}: a waiting line is not made up again while its layout is open`, A.greenHeld(rB1) && A.greenHeld(rC1));
    await A.addAll(w.run);
    ok(`${metal}: an intake meanwhile leaves them waiting`, A.pagesOf(metal).length === 1 && rB1.state === 'waiting');
    // the first layout is cut: the second set's order continues on ITS leftover; the third set's waits again
    await w.cut(L1);
    const L2 = A.pagesOf(metal)[1];
    ok(`${metal}: after the cut the waiting order goes on the next layout`, !!L2 && L2.charms.length === 2 && L2.charms.every(ch => ch.order === 'B1') && rB1.state !== 'waiting', JSON.stringify(rB1));
    ok(`${metal}: that layout is made on the leftover the cut made (an exact, pending claim of it)`, L2.roseStock && L2.roseStock.id === L1.roseStock.id && L2.roseStock.revision === 1 && L2._partialId === `${L1.roseStock.id}-1` && L2._partialPending === true, JSON.stringify(L2.roseStock));
    ok(`${metal}: the third set's order waits for L2's green line`, rC1.state === 'waiting' && A.greenHeld(rC1) && A.pagesOf(metal).length === 2);
    ok(`${metal}: still exactly one uncut layout`, w.uncut(metal).length === 1 && w.uncut(metal)[0] === L2);
    // L2 is cut and its leftover holds nothing more: only now a brand-new sheet
    await w.cut(L2, false);
    const L3 = A.pagesOf(metal)[2];
    ok(`${metal}: when the leftover is used up the next layout is a brand-new sheet`, !!L3 && L3.charms.every(ch => ch.order === 'C1') && !L3.roseStock && !L3._partialId, JSON.stringify(L3 && L3.roseStock));
    ok(`${metal}: one uncut layout, three layouts in all, no parallel draft ever`, w.uncut(metal).length === 1 && A.pagesOf(metal).length === 3 && A.made() - madeAt0 === 3);
  }

  /* ── 2 · a stock cut twice: only its newest cut's copy counts (no stale leftover is offered) ── */
  {
    const w = world(), { A } = w;
    const L1 = A.addPage('rose'), L2 = A.addPage('rose');
    L1.roseCutAt = 1; L1.roseStock = { id: 'rgs-9', revision: 1, available: true, owner: null, profileJson: '{}' };
    L2.roseCutAt = 2; L2.roseStock = { id: 'rgs-9', revision: 2, available: false, owner: null, profileJson: '{}' };
    const pg = A.greenPage('rose', w.run);
    ok('a stock cut again is not offered at its older revision', !pg.roseStock && !pg._partialId, JSON.stringify(pg.roseStock));
    L2.roseStock.available = true;
    const pg2 = A.greenPage('rose', w.run);
    ok('its newest leftover is', pg2._partialId === 'rgs-9-2');
    const pg3 = A.greenPage('rose', w.run);
    ok('and a leftover an uncut page already holds is not given to another page', !pg3._partialId, pg3._partialId);
  }

  /* ── 3 · a formed set never waits; Paul's Options choice (always a new sheet) stands ── */
  {
    const w = world(), { A } = w;
    w.page('gold', ['A1']); w.page('silver', ['A1']);
    w.page('gold', ['B1'], 'set-2'); w.page('silver', ['B1'], 'set-2');
    const rA = w.line('A1'), rB = w.line('B1'); w.place(rA); w.place(rB);
    ok('an order whose GF / SS sheets are in a formed set does not wait for another set\'s layout', rB.state !== 'waiting' && A.pagesOf('rose').length === 2, rB.reason);
  }
  {
    const w = world({ mode: { rose: 'new' } }), { A } = w;
    w.page('gold', ['A1']); w.page('silver', ['A1']); w.page('gold', ['B1']); w.page('silver', ['B1']);
    const rA = w.line('A1'), rB = w.line('B1'); w.place(rA); w.place(rB);
    ok('with Options set to a new sheet the order still waits for the open layout (one at a time)', rB.state === 'waiting');
    await w.cut(A.pagesOf('rose')[0]);
    const pg = A.pagesOf('rose')[1];
    ok('and the next layout is the brand-new sheet Paul set (no leftover is taken by itself)', pg && pg.charms.length === 1 && !pg.roseStock && !pg._partialId);
  }

  /* ── 4 · the server's automatic choice: the newest leftover with room first ── */
  {
    const RoseStock = require('../../netlify/functions/_charmNestRoseStock'), Readiness = require('../../charm-nest-readiness');
    const store = new Map(), clone = x => structuredClone(x);
    const ref = p => ({ path: p, id: p.split('/').at(-1), collection: n => query(p + '/' + n), get: async () => snap(p) });
    const snap = p => ({ id: p.split('/').at(-1), ref: ref(p), exists: store.has(p), data: () => clone(store.get(p)) });
    const query = (p, filters = [], lim = Infinity) => ({ doc: id => ref(p + '/' + id), where: (...f) => query(p, [...filters, f], lim), orderBy: () => query(p, filters, lim), limit: n => query(p, filters, n),
      get: async () => { let docs = [...store.keys()].filter(k => k.startsWith(p + '/') && !k.slice(p.length + 1).includes('/')).map(snap).filter(d => filters.every(([f, , v]) => d.data()[f] === v)).slice(0, lim); return { docs, size: docs.length }; } });
    const db = { runTransaction: async fn => { const writes = []; const r = await fn({ get: x => x.get(), set: (x, v) => writes.push(() => store.set(x.path, clone(v))), update: (x, v) => writes.push(() => store.set(x.path, { ...(store.get(x.path) || {}), ...clone(v) })), delete: x => writes.push(() => store.delete(x.path)) }); writes.forEach(f => f()); return r; } };
    const api = RoseStock({ db, col: query, FV: { serverTimestamp: () => 1, increment: n => n }, Readiness, sheetLabel: () => 'RG Sheet', stamp: async () => {} });
    const S = (id, v) => store.set('Charm_Nest_Rose_Stock/' + id, { id, metal: 'rose', wPt: 100, hPt: 50, owner: null, available: true, ...v });
    S('rgs-fresh', { revision: 0, profileJson: null, createdMs: 1 });
    S('rgs-old', { revision: 1, profileJson: '{"version":1}', createdMs: 2, lastCutAt: 100 });
    S('rgs-new', { revision: 3, profileJson: '{"version":1}', createdMs: 3, lastCutAt: 300 });
    S('rgs-14k', { metal: 'gold14k', revision: 1, profileJson: '{"version":1}', createdMs: 4, lastCutAt: 900 });
    const r = await api.roseClaim({ sheetId: 'rose-next', metal: 'rose', wPt: 100, hPt: 50 });
    ok('roseClaim without a chosen sheet takes the metal\'s NEWEST leftover (not the first one listed, not a full sheet)', r.stock.id === 'rgs-new', r.stock.id);
    const r2 = await api.roseClaim({ sheetId: 'rose-next-2', metal: 'rose', wPt: 100, hPt: 50 });
    ok('then the next newest leftover', r2.stock.id === 'rgs-old', r2.stock.id);
    const r3 = await api.roseClaim({ sheetId: 'rose-next-3', metal: 'rose', wPt: 100, hPt: 50 });
    ok('and only then a sheet that is not a leftover', r3.stock.id === 'rgs-fresh', r3.stock.id);
    const r4 = await api.roseClaim({ sheetId: 'g14-next', metal: 'gold14k', wPt: 100, hPt: 50, onlyRemnant: true });
    ok('14K takes its own newest leftover', r4.stock && r4.stock.id === 'rgs-14k');
    const r5 = await api.roseClaim({ sheetId: 'rose-fresh', metal: 'rose', wPt: 100, hPt: 50, fresh: true });
    ok('a fresh sheet asked for (Options: always a new sheet) is still a new sheet', r5.stock && /^rgs-/.test(r5.stock.id) && r5.stock.revision === 0 && !['rgs-fresh', 'rgs-old', 'rgs-new'].includes(r5.stock.id));
  }
  console.log('PASS: rg-continue: one green-line layout at a time, the next continues on the cut\'s leftover, a new sheet only when it is used up; Paul\'s own sheet choice stands');
})().catch(e => { console.error(e); process.exit(1); });
