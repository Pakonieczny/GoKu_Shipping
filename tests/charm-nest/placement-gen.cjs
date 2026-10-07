// FC3 (Firestore cost): the Sorter's placement feed asks getOrderPieces every 2.5 s per open tab for up to 48 orders (two requests of
// 30 and 18). Each ask used to read every pool row and every sheet of those orders (about 6 documents an order, 70 KB for 30 orders)
// only to find the answer was the same: `unchanged` saved the wire, not the reads. The cloud now keeps one small counter,
// Charm_Nest_Rev/placement { n }, that the handler raises after any op that may have written a pool row or a sheet; an ask that
// carries the answer's rev (ifRev) AND the counter it was made at (ifGen) is answered `unchanged` for one read.
//   node tests/charm-nest/placement-gen.cjs
// Over the real charmNestLibrary handler on the in-memory Firestore (bridge-server.cjs: st.reads counts the documents read as Firestore bills them).
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.message || e).split('\n').slice(0, 6).join('\n      ')); } };

const N = 48, rid = i => String(4190000000 + i);
const pidOf = (r, k) => `${r}_${r}0${k}_1`;
function seed(st, pre = '') {
  const put = (c, id, d) => st.put(pre + c, id, d);
  const sheets = ['gf1', 'gf2', 'ss1', 'ss2'].map((x, i) => ({ id: 'sheet-gen-' + x, metal: i < 2 ? 'gold' : 'silver', poolIds: [], orders: [] }));
  for (let i = 0; i < N; i++) {
    const r = rid(i);
    for (const k of [1, 2]) {
      const s = sheets[(i + k) % 4], poolId = pidOf(r, k);
      s.poolIds.push(poolId); if (!s.orders.includes(r)) s.orders.push(r);
      put('Charm_Pool', poolId, { poolId, orderId: r, transactionId: r + '0' + k, lineKey: r + '_' + r + '0' + k, sku: 'DUCK-' + k, material: s.metal, copy: 1, quantity: 1, state: 'written', sheetId: s.id, sheetName: s.id, setId: 'set-gen-1', runId: 'run-gen-1', charmHash: 'h'.repeat(40), masterHash: 'm'.repeat(40), aiPath: 'charmnest/masters/duck/size-1.ai', size: 'S', form: 'charm', chain: null, orderDate: 1790000000000, arrivedAt: 1790000000000, engrave: false, updateTs: 1790000000 });
    }
  }
  for (const s of sheets) put('Charm_Nest_Sheets', s.id, { id: s.id, metal: s.metal, sheetIndex: 1, setId: 'set-gen-1', setSeq: 1, folder: s.id, fileBase: s.id, day: '2026-10-07', status: 'written', orders: s.orders, poolIds: s.poolIds, page: 1 });
}

(async () => {
  const srv = await start(); srv.st.atomic = true; const st = srv.st; seed(st);
  const call = async (body, sandbox) => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(sandbox ? Object.assign({ sandbox: true }, body) : body) }); return { status: r.status, body: await r.json() }; };
  const reads = async fn => { const r0 = st.reads || 0; const out = await fn(); return { out, reads: (st.reads || 0) - r0 }; };
  const ids30 = Array.from({ length: 30 }, (_, i) => rid(i));
  const gen = () => (st.docs.get('Charm_Nest_Rev/placement') || {}).n;
  try {
    let a1, g1;
    await t('A1 a first (full) ask answers the pieces, a rev and the counter it was made at; one read more than before (the counter)', async () => {
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30 }));
      a1 = out.body; assert.equal(out.status, 200); assert(/^[0-9a-f]{12}$/.test(a1.rev)); assert.equal(Object.keys(a1.orders).length, 30);
      g1 = a1.gen; assert.equal(g1, 0, 'no write has raised it yet'); assert(n >= 60 + 4 + 1, 'a full read: ' + n + ' documents');
      console.log('        full ask for 30 orders: ' + n + ' documents read');
    });
    await t('B1 the same ask with ifRev + ifGen is `unchanged` for ONE document read (the counter), the rev and gen echoed', async () => {
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a1.rev, ifGen: g1 }));
      assert.deepEqual(out.body, { ok: true, unchanged: true, rev: a1.rev, gen: g1 }); assert.equal(n, 1, 'documents read: ' + n);
    });
    await t('B2 ifGen alone (no ifRev) never skips the read: the page must hold an answer to be told nothing changed', async () => {
      const out = (await call({ op: 'getOrderPieces', orderIds: ids30, ifGen: g1 })).body; assert(out.orders && out.rev === a1.rev);
    });
    await t('B3 ifRev alone (an older page) still works as before: the full read, then `unchanged` without the payload', async () => {
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a1.rev }));
      assert.deepEqual(out.body, { ok: true, unchanged: true, rev: a1.rev, gen: g1 }); assert(n > 60, 'read in full: ' + n);
    });
    await t('C1 read-only ops, the frequent ones above all (laserStatus, poolList, cancelList, timelineGet, bridgeLog), do not raise the counter', async () => {
      for (const body of [{ op: 'ping' }, { op: 'poolList', orderId: rid(0) }, { op: 'cancelList' }, { op: 'timelineGet', orderId: rid(0) }, { op: 'bridgeLog', session: 'sess-fc3-test', rows: [] }, { op: 'laserStatus', sheetIds: ['sheet-gen-gf1'] }, { op: 'getOrderPieces', orderIds: [rid(1)] }]) await call(body);
      assert.equal(gen(), undefined, 'the counter document does not even exist yet: ' + gen());
      assert.equal((await call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a1.rev, ifGen: g1 })).body.unchanged, true);
    });
    let a2;
    await t('D1 a hold (poolUpdate) raises the counter before it answers; the next ask reads in full and answers the new pieces with a new rev and gen', async () => {
      const p1 = pidOf(rid(3), 1);
      const r = await call({ op: 'poolUpdate', poolIds: [p1], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: 1790000009999 }, by: 'Paul' });
      assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(gen(), 1);
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a1.rev, ifGen: g1 }));
      a2 = out.body; assert(a2.orders, 'answered in full'); assert.notEqual(a2.rev, a1.rev); assert.equal(a2.gen, 1);
      assert.equal(a2.orders[rid(3)].placement[p1].state, 'held'); assert(n > 60, 'read in full: ' + n);
    });
    await t('D2 and then cheap again: the new rev + gen are `unchanged` for one read', async () => {
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a2.rev, ifGen: a2.gen }));
      assert.deepEqual(out.body, { ok: true, unchanged: true, rev: a2.rev, gen: 1 }); assert.equal(n, 1);
    });
    await t('E1 a write to an order this ask does not hold moves the counter: one full read, `unchanged` by rev (no payload), and cheap again after', async () => {
      assert.equal((await call({ op: 'poolUpdate', poolIds: [pidOf(rid(40), 1)], patch: { note: 'elsewhere' }, by: 'Paul' })).status, 200); assert.equal(gen(), 2);
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a2.rev, ifGen: a2.gen }));
      assert.deepEqual(out.body, { ok: true, unchanged: true, rev: a2.rev, gen: 2 }); assert(n > 60, 'read in full to find out: ' + n);
      assert.equal((await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: a2.rev, ifGen: 2 }))).reads, 1);
    });
    await t('F1 a failed write op (a 400) raises the counter too: it may have written part of what it meant to', async () => {
      const before = gen(); await call({ op: 'poolUpdate', poolIds: ['not a pool id'], patch: {} }); assert.equal(gen(), before + 1);
    });
    await t('G1 the sandbox keeps no counter: no gen in its answers, no Sandbox_Charm_Nest_Rev, no leftover of a reset; its asks are the full read as before', async () => {
      seed(st, 'Sandbox_');
      const s1 = (await call({ op: 'getOrderPieces', orderIds: ids30 }, true)).body; assert.equal(s1.gen, undefined); assert(s1.orders);
      await call({ op: 'poolUpdate', poolIds: [pidOf(rid(1), 1)], patch: { note: 'sandbox' } }, true);
      assert.equal([...st.docs.keys()].filter(k => /Charm_Nest_Rev/.test(k) && k.startsWith('Sandbox_')).length, 0);
      const { out, reads: n } = await reads(() => call({ op: 'getOrderPieces', orderIds: ids30, ifRev: s1.rev, ifGen: 0 }, true));
      assert(n > 60, 'full read: ' + n); assert.equal(out.body.gen, undefined);
    });
    await t('H1 the page (OrderPieces.load) sends ifGen on a forced re-read, takes the new gen after a change, and asks in full once a minute', async () => {
      const vm = require('vm'); const src = require('fs').readFileSync(path.join(root, 'charm-nest-order-pieces.js'), 'utf8');
      const sent = []; let clock = 1000000;
      const ctx = { console, setTimeout, clearTimeout, Date: Object.assign(function () { return new Date(clock); }, { now: () => clock }), Promise, Map, Set, JSON, Math, Number, String, Array, Object };
      ctx.self = ctx; ctx.window = ctx; ctx.document = undefined;
      let answers = [{ ok: true, rev: 'aaaaaaaaaaaa', gen: 5, orders: { '4190000000': { pools: [], sheets: [] } } }];
      ctx.CN = { api: async (fn, ask) => { sent.push(Object.assign({}, ask)); if (ask.ifRev && ask.ifGen === 5 && !ctx.__moved) return { ok: true, unchanged: true, rev: ask.ifRev, gen: 5 }; if (ctx.__moved) return { ok: true, rev: 'bbbbbbbbbbbb', gen: 6, orders: { '4190000000': { pools: [], sheets: [] } } }; return answers[0]; } };
      vm.createContext(ctx); vm.runInContext(src, ctx);
      const OP = ctx.OrderPieces; assert(OP && OP.load, 'OrderPieces is on the page object');
      await OP.load(['4190000000']);                                // first read, in full
      assert.equal(sent.at(-1).ifRev, undefined);
      clock += 2500; await OP.load(['4190000000'], { force: true }); // forced, gen known
      assert.equal(sent.at(-1).ifRev, 'aaaaaaaaaaaa'); assert.equal(sent.at(-1).ifGen, 5);
      clock += 2500; await OP.load(['4190000000'], { force: true }); assert.equal(sent.at(-1).ifGen, 5, 'still cheap');
      clock += 61000; await OP.load(['4190000000'], { force: true }); assert.equal(sent.at(-1).ifGen, undefined, 'a minute on: the page asks in full');
      assert.equal(sent.at(-1).ifRev, 'aaaaaaaaaaaa', 'the digest still saves the payload');
      clock += 2500; await OP.load(['4190000000'], { force: true }); assert.equal(sent.at(-1).ifGen, 5, 'and the minute starts again');
      ctx.__moved = true; clock += 2500; await OP.load(['4190000000'], { force: true }); clock += 2500; await OP.load(['4190000000'], { force: true });
      assert.equal(sent.at(-1).ifRev, 'bbbbbbbbbbbb'); assert.equal(sent.at(-1).ifGen, 6, 'the new gen is the one sent next');
    });
  } finally { srv.close(); }
  const failed = results.filter(([, e]) => e);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
