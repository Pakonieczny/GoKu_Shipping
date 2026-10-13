// REVSAND (review-send-1012): the world of review-send-real.cjs, for BOTH workspaces (sandbox and production).
//
// The REAL sorter page (charm-nest-1.html) in headless Chromium over the repo's fake site (bridge-server.cjs: the real charmNest* handlers over an in-memory Firestore),
// the same pieces as _orderFactsWorld.cjs / _engraveRealWorld.cjs: the master library holds the real drawings of tests/charm-nest/fixtures/*.ai, orders go through the REAL
// intake (Orders.interpretAll), the REAL saved links (Charm_Sku_Aliases through the real aliasGet / aliasPut) and the REAL Library (sheet records seeded in the fake Firestore, read by the page).
// Nothing is written anywhere real: every request that is not to the loopback fake is refused, the passcode of the purge is a throwaway of this process, every buyer is made up ("Name One").
//
//   const W = require('./_reviewSendWorld.cjs');
//   const w = await W.open('sandbox' | 'production');   // null without playwright-core / chrome
//   await W.addOrders(w, ['matched']);                  // pulled lines, read by the real intake
//   await W.seedSheet(w, { id: 'sh-rs1', orders: ['sheeted', 'mate'] });   // a saved sheet (Library) holding those orders' pieces
//   await W.reboot(w);                                  // a page reload (a second computer / the next morning): the cloud stays, the page forgets
//   await w.close();
'use strict';
// (set before the fake site loads the real handlers: strict about arrays in arrays, and a purge passcode that exists only in this process)
process.env.CN_STRICT_ARRAYS = '1';
process.env.CHARM_NEST_DELETE_CODE = 'offline-fake-only-' + Math.random().toString(36).slice(2, 10);
const path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
const Base = require('./_orderFactsWorld.cjs');
const Real = require('./_engraveRealWorld.cjs');

const V = (name, value) => ({ name, value });
const SHIP_DAYS = 5;
/** The orders (receipt and listing ids are made up and unique; buyers are made up). The master library (Real.DESIGNS) holds COMET_9427, SCHRODINGER CAT 1, SCHRODINGER CAT 2, TENNIS BALL (HUGGIE), 5X8.5. */
const ORDERS = {
  // a listing whose SKU is a charm of the library (matched by its own name), and whose NEAR charm in the library is SCHRODINGER CAT 2
  matched: { rid: '4291000001', listing: '1891000001', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] },
  // the NEXT order of that very listing (another buyer, another receipt), then one more for after the sandbox clean-up
  repeat: { rid: '4291000002', listing: '1891000001', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] },
  again: { rid: '4291000003', listing: '1891000001', sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Three', placed: { day: 0, h: 9, m: 30 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] },
  // the same listing, ANOTHER variation (another SKU, matched by its own name): the saved link of the first SKU is not its link
  variant: { rid: '4291000007', listing: '1891000001', sku: 'COMET_9427', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Seven', placed: { day: 0, h: 9, m: 35 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] },
  // a matched listing with NOTHING like it in the library: no suggestion area at all
  alone: { rid: '4291000004', listing: '1891000004', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Four', placed: { day: 0, h: 9, m: 40 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] },
  // two orders on ONE saved sheet (the second is the control: it is never marked)
  sheeted: { rid: '4291000005', listing: '1891000005', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Five', placed: { day: -1, h: 9, m: 50 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] },
  mate: { rid: '4291000006', listing: '1891000006', sku: 'COMET_9427', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Six', placed: { day: -1, h: 10, m: 0 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold')] }
};
const NEAR = 'SCHRODINGER CAT 2';          // the charm the person picks for the `matched` listing (near to its own SKU, in the library)
const keyOf = n => `${ORDERS[n].rid}_${ORDERS[n].rid}1`;
const poolIdOf = n => keyOf(n) + '_1';

/** Where the page is: the real page over the fake site, the master library seeded (with its files' download addresses, as a real index entry has), the workspace chosen. */
async function open(mode, opts = {}) {
  const w = await Real.world(Object.assign({ viewport: { width: 1700, height: 900 } }, opts));
  if (!w) return null;
  w.mode = mode; w.prefix = mode === 'sandbox' ? 'Sandbox_' : '';
  const st = w.srv.st;
  // a real index entry carries the address of its drawing: the sandbox reads it by that address (the production path reads it by name)
  for (const d of st.list('Charm_Master_Index')) st.put('Charm_Master_Index', d._id, { aiUrl: w.srv.blobUrl(d.aiPath) });
  // every request the page makes goes to the loopback fake: any other address is refused by the world, and noted here
  w.foreign = []; w.page.on('request', r => { if (!/^(http:\/\/(127\.0\.0\.1|localhost)[:/]|data:|blob:)/.test(r.url()) && !/fonts\.(googleapis|gstatic)/.test(r.url())) w.foreign.push(r.url()); });
  if (mode === 'sandbox') {
    await w.page.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'on', sandboxStream: 'off', pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.removeItem('cn.sandboxHold'); });
    await w.page.reload({ waitUntil: 'load' });
    await ready(w);
  } else await ready(w, true);
  const sb = await w.page.evaluate(() => !!WORKSPACE_SANDBOX);
  if (sb !== (mode === 'sandbox')) throw new Error(`the page is ${sb ? 'in' : 'not in'} the sandbox, the test wanted ${mode}`);
  return w;
}
/** The page is up and has read the library and the learned maps (as at the start of a shift). */
async function ready(w, noWait) {
  if (!noWait) await w.page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
  // the page's own start-up has finished (it recovers the saved workspace, opens the tab it was on, and only then listens: Session.ready()): until then it may
  // still switch the tab and empty the day's rows under the test's hands
  await w.page.waitForFunction(() => !window.Session || typeof Session.ready !== 'function' || Session.ready() === true || (Session.failure && Session.failure()), null, { timeout: 60000 });
  await w.page.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); try { localStorage.removeItem('cn.sandboxHold'); } catch (_) {} });
}
/** A page reload: the cloud stays as it is, the page forgets everything it held in memory (a second computer, or the next morning). */
async function reboot(w) {
  await w.page.reload({ waitUntil: 'load' });
  await ready(w);
  await w.page.evaluate(() => { B.orders.rows = []; B.orders.byKey = new Map(); });   // (the harness puts the day's orders in itself: the pull is not played)
}

/** Lines in, through the real intake (no pool, no run: a pulled order, as it arrives). `names` = keys of ORDERS. Resolves nothing. */
async function addOrders(w, names, o = {}) {
  // (a page that has only just reloaded can still be finishing its own start-up, which may empty the day's rows once more: an order that is not
  //  there after the settle is put in again, up to three times, so this helper never reports a start-up race as a missing order)
  for (let attempt = 0; attempt < 3; attempt++) {
    const have = await w.page.evaluate(rids => rids.filter(r => Orders.rows().some(x => String(x.order.receiptId) === r)), names.map(n => ORDERS[n].rid));
    const missing = names.filter(n => !have.includes(ORDERS[n].rid));
    if (!missing.length) return;
    await addOrdersOnce(w, missing, o);
  }
}
async function addOrdersOnce(w, names, o = {}) {
  await w.page.evaluate(async ({ fixtures, orderSrc, loadMaps }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    if (loadMaps) await Orders.loadMaps(true);
    for (const fx of fixtures) {
      if (Orders.rows().some(x => String(x.order.receiptId) === String(fx.rid))) continue;
      const order = orderOf(fx, now);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    }
    Orders.interpretAll(); try { Review.syncOrderItems(); } catch (_) {} CN.setMode('orders'); Orders.render();
  }, { fixtures: names.map(n => ORDERS[n]), orderSrc: Base.orderOf.toString(), loadMaps: o.loadMaps !== false });
  await Real.settle(w.page, 400);
}

/** What the intake read for an order: the charm it matched, how, what is wrong with it, and where the page keeps it. */
const readOf = async (w, name) => {
  // waits for the order to be in the page's rows and read (never loosens what is checked about it); null only when it is still not there after 6 s
  const pull = () => w.page.evaluate(rid => {
    const rows = Orders.rows().filter(r => String(r.order.receiptId) === rid), r = rows[0]; if (!r) return null;
    const s = r.spec || {};
    return { key: r.key, state: r.state, designSku: s.designSku || null, source: s.skuSource || null, bought: s.boughtSku || null, problems: (r.problems || []).map(p => p.kind), reason: r.reason || null, poolIds: r.poolIds || [], read: !!r.spec };
  }, ORDERS[name].rid);
  const t0 = Date.now(); let v = await pull();
  while ((!v || !v.read) && Date.now() - t0 < 6000) { await new Promise(r => setTimeout(r, 150)); v = await pull(); }
  if (v) delete v.read;
  return v;
};

/** The saved links the cloud holds for a listing: every document of the fake Firestore, in any collection, that carries the listing's id AND the picked charm under an alias-like name. Location-free on purpose: the test says WHERE it found it. */
function linksFor(w, listing, sku) {
  const out = [];
  for (const [k, v] of w.srv.st.docs.entries()) {
    const coll = k.split('/')[0], id = k.slice(coll.length + 1);
    if (!/alias|link/i.test(coll)) continue;
    const txt = JSON.stringify(v);
    if ((id === listing || txt.includes('"' + listing + '"')) && (!sku || txt.includes(sku))) out.push({ path: k, doc: v });
  }
  return out;
}
/** Every write the fake site's Firestore took since `mark` that touched a collection matching `re` (the call log of the real handlers is `st.calls`). */
const callsSince = (w, mark) => w.srv.st.calls.slice(mark);

/** A saved sheet (what the Library reads): the sheet record, its pool rows, the run's lines, and a set when `set`. `orders` = keys of ORDERS (one piece each, in this order). qr:false = no QR label yet (a step a press of Approve does by itself: so the button exists). */
async function seedSheet(w, o) {
  const st = w.srv.st, P = w.prefix, NOW = Date.now(), DAY = '2026-10-12', ts = { toMillis: () => NOW };
  const id = o.id || 'sh-rs1', run = 'run-rs', setId = o.set ? (o.setId || 'set-rs1') : null, metal = o.metal || 'gold', code = metal === 'silver' ? 'SS' : 'GF', n = o.n || 1, fileBase = `${code}_${DAY}_Set-1_Sheet-${n}`;
  const box = (cid, i) => ({ id: cid, cxPt: 24 + (i % 8) * 34, cyPt: 24 + Math.floor(i / 8) * 34, angle: 0, wPt: 30, hPt: 30 });
  const names = o.orders, lines = {};
  const pieces = names.map((nm, i) => ({ nm, cid: 'c' + i, rid: ORDERS[nm].rid, tid: ORDERS[nm].rid + '1', key: keyOf(nm), pid: poolIdOf(nm), sku: ORDERS[nm].sku }));
  for (const p of pieces) lines[p.key] = { orderId: p.rid, transactionId: p.tid, sku: p.sku, state: 'written', quantity: 1, material: metal, poolIds: [p.pid], engraveCandidate: false, noDesign: false };
  const prev = st.doc(P + 'Charm_Nest_Runs', run);
  st.put(P + 'Charm_Nest_Runs', run, { runId: run, lines: Object.assign({}, prev && prev.lines || {}, lines) });
  const outUrl = `${w.srv.sorterOrigin}/${id}`, rids = pieces.map(p => p.rid);
  const sheet = { id, setId, setSeq: setId ? 1 : null, sheetIndex: n, runId: run, metal, day: DAY, fileBase, folder: fileBase, status: 'complete', placedCount: pieces.length, charmCount: pieces.length, density: .6, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 },
    placements: pieces.map((p, i) => box(p.cid, i)), charms: pieces.map(p => ({ id: p.cid, name: `${p.rid} · ${p.sku}`, poolId: p.pid, order: p.rid, sku: p.sku })), poolIds: pieces.map(p => p.pid), orders: rids, verification: { ok: true },
    outputs: { ai: { path: id + '.ai', url: outUrl + '.ai' }, preview: { path: id + '.png', url: outUrl + '.png' } }, updatedAt: ts, createdAt: ts };
  if (o.qr !== false) sheet.label = { files: [{ path: id + '-qr.png', url: outUrl + '-qr.png', payload: rids.join(','), orders: rids }], orders: rids };
  if (!setId) delete sheet.setSeq;
  st.put(P + 'Charm_Nest_Sheets', id, sheet, false);
  for (const p of pieces) st.put(P + 'Charm_Pool', p.pid, { poolId: p.pid, orderId: p.rid, transactionId: p.tid, lineKey: p.key, sku: p.sku, material: metal, copy: 1, quantity: 1, runId: run, createdAt: NOW - 3600e3, updatedAt: NOW, state: 'written', sheetId: id, sheetName: fileBase, ...(setId ? { setId } : {}) });
  if (setId) {
    const orders = {}; for (const p of pieces) orders[p.rid] = { held: null, lines: [{ transactionId: p.tid, sku: p.sku, copies: [{ copy: 1, sheetId: id, sheet: fileBase, poolId: p.pid, backPoolId: null }] }] };
    st.put(P + 'Charm_Nest_Sets', setId, { setId, seq: 1, day: DAY, runId: run, sheetIds: [id], materials: [metal], orders, labelFiles: [], status: 'labelled', updatedAt: ts, createdAt: ts });
  }
  // the page's rows for these orders: written lines that hold their pieces (as a page that recalled the last run has them)
  await w.page.evaluate(async ({ fixtures, orderSrc, pools }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    for (const fx of fixtures) {
      const order = orderOf(fx, now);
      for (const line of order.lines) {
        const key = CharmNestOrders.lineKey(order, line), had = B.orders.byKey.get(key);
        if (had) { had.state = 'written'; had.poolIds = [pools[fx.rid]]; continue; }
        const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'written', reason: null, claimedBy: null, poolIds: [pools[fx.rid]], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row);
      }
    }
    Orders.interpretAll(); try { Review.syncOrderItems(); } catch (_) {} CN.setMode('orders'); Orders.render();
  }, { fixtures: names.map(n => ORDERS[n]), orderSrc: Base.orderOf.toString(), pools: Object.fromEntries(pieces.map(p => [p.rid, p.pid])) });
  return { id, setId, fileBase, name: `${code} Sheet ${n}`, pieces };
}

/** The snapshot of WHERE the sheet's pieces are: the sheet's layout (placements, charms, pool ids, orders, its set and state), each piece's pool row (sheet, set, state) and the set's members and order lines.
 *  The proof that nothing was lifted off, re-nested or moved. The server's own bookkeeping of readiness (processReady, stepState, stepStamps, updatedAt) is left out on purpose: it follows the hold, it never moves a piece. */
function sheetRecords(w, sh) {
  const st = w.srv.st, P = w.prefix, canon = v => JSON.stringify(v, (k, x) => (x && typeof x === 'object' && !Array.isArray(x) && !x.toMillis ? Object.fromEntries(Object.keys(x).sort().map(q => [q, x[q]])) : x));
  const pick = (d, keys) => d ? Object.fromEntries(keys.filter(k => d[k] !== undefined).map(k => [k, d[k]])) : null;
  return canon({ sheet: pick(st.doc(P + 'Charm_Nest_Sheets', sh.id), ['id', 'setId', 'sheetIndex', 'runId', 'metal', 'status', 'placements', 'charms', 'poolIds', 'orders', 'draft', 'solidIncluded', 'archived', 'fileBase', 'outputs']),
    pool: sh.pieces.map(p => pick(st.doc(P + 'Charm_Pool', p.pid), ['poolId', 'orderId', 'lineKey', 'sheetId', 'sheetName', 'setId', 'state', 'removedAt'])), set: sh.setId ? pick(st.doc(P + 'Charm_Nest_Sets', sh.setId), ['setId', 'sheetIds', 'orders', 'status']) : null });
}
/** Whether the sheet carries a laser seal / cut mark (it must not while it is held). */
function cutOf(w, sh) { const d = w.srv.st.doc(w.prefix + 'Charm_Nest_Sheets', sh.id) || {}; return { laserDoneAt: +d.laserDoneAt || 0, seals: (d.processSeals || []).map(x => x.how) }; }

/** What differs between two snapshots of sheetRecords (the paths, with the before and after values clipped): the message of a failed "nothing changed". */
function recordsDiff(a, b) {
  if (a === b) return '';
  const out = [], walk = (x, y, p) => {
    if (JSON.stringify(x) === JSON.stringify(y)) return;
    if (x && y && typeof x === 'object' && typeof y === 'object' && !Array.isArray(x) && !Array.isArray(y)) { for (const k of new Set([...Object.keys(x), ...Object.keys(y)])) walk(x[k], y[k], p + '.' + k); return; }
    out.push(`${p}: ${JSON.stringify(x) === undefined ? 'absent' : JSON.stringify(x).slice(0, 80)} -> ${JSON.stringify(y) === undefined ? 'absent' : JSON.stringify(y).slice(0, 80)}`);
  };
  try { walk(JSON.parse(a), JSON.parse(b), 'rec'); } catch (e) { return 'unreadable ' + e.message; }
  return out.slice(0, 6).join(' ; ');
}

/** The sandbox clean-up and the history purge, on the fake site only (the page's own ops; the purge passcode is this process's throwaway). */
async function resetSandbox(w) {
  return w.page.evaluate(async () => { try { return await CN.api('charmNestLibrary', { op: 'sandboxReset', sandbox: true }); } catch (e) { return { error: String(e && e.message || e), status: e && e.status }; } });
}
async function purgeHistory(w) {
  const code = process.env.CHARM_NEST_DELETE_CODE;
  return w.page.evaluate(async code => { try { return await CN.api('charmNestLibrary', { op: 'purgeHistory', code, force: true }); } catch (e) { return { error: String(e && e.message || e), status: e && e.status }; } }, code);
}

module.exports = { open, ready, reboot, addOrders, readOf, linksFor, callsSince, seedSheet, sheetRecords, cutOf, recordsDiff, resetSandbox, purgeHistory, ORDERS, NEAR, keyOf, poolIdOf, settle: Real.settle, shot: Real.shot, root };
