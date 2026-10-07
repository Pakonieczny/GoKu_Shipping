// FC9 (sandbox) cost measurements, offline: the real etsySandbox / firebaseOrders / charmNestLibrary code over the FC1 meter's
// in-memory Firestore. Nothing touches a network or the real Firestore.
//   node tests/cost/fc9-sandbox.cjs            (N=400 open orders by default; N=800 node tests/cost/fc9-sandbox.cjs 800)
// Part A  the server work of ONE sandbox stream check once every order has come ("done"), the way the station asked it before
//         (every open order asked about again: dcFor, staffNotesFor, all locks and claims) and the way it asks it now.
// Part B  what the Sorter's check loop reads in the learned maps and the library (shared with production), per call.
// Part C  Reset the sandbox / Purge on a seeded sandbox: reads, deletes, bytes, calls, and a check that every family is gone.
'use strict';
const path = require('path'), assert = require('assert');
const meter = require('./meter.cjs');
const ROOT = process.env.FC9_ROOT || path.join(__dirname, '../..');
const fnDir = path.join(ROOT, 'netlify/functions');
const N = +process.argv[2] || 400;
const TS = meter.Timestamp;
const m = meter.create(); m.install();
const db = m.db;
const bucket = m._storage;
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const fo = require(path.join(fnDir, 'firebaseOrders.js'));
const etsy = require(path.join(fnDir, 'etsySandbox.js'));
const call = async (h, ev) => { const r = await h(ev); const body = r.body ? JSON.parse(r.body) : null; if (r.statusCode >= 400) throw new Error(h.name + ' ' + r.statusCode + ' ' + r.body.slice(0, 200)); return body; };
const L = (op, b) => call(lib.handler, { httpMethod: 'POST', headers: {}, body: JSON.stringify(Object.assign({ op, sandbox: true }, b)) });
const FO = q => call(fo.handler, { httpMethod: 'GET', headers: {}, queryStringParameters: Object.assign({ sandbox: '1' }, q) });
const E = q => call(etsy.handler, { httpMethod: 'GET', headers: {}, queryStringParameters: q });
const fmt = n => String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
const rows = [];
const line = (label, d, note) => { rows.push([label, d.reads + (d.aggs || 0), d.bytes, d.writes, d.deletes]); console.log(`${label.padEnd(58)} reads ${String(d.reads + (d.aggs || 0)).padStart(7)}  bytes ${String(d.bytes).padStart(9)}  writes ${String(d.writes).padStart(5)}  deletes ${String(d.deletes).padStart(6)}${note ? '  ' + note : ''}`); };
const measure = async (name, fn) => { const a = m.snapshot(); const out = await m.op(name, fn); return { d: m.since(a), out }; };

(async () => {
  const STEP = 600000, DAY = 86400, now0 = Date.now(), snapAt = now0 - 86400e3 * 18;
  /* ── a snapshot of N open orders (each with two transactions, as Etsy writes them), stored as the real one is: a file in Storage ── */
  const receipt = i => { const rid = 3521000000 + i, created = Math.floor(snapAt / 1000) - (N - i) * 3600, ship = created + (2 + i % 6) * DAY;
    const tx = k => ({ transaction_id: Number(`${rid}${k}`), listing_id: 1718000 + k, receipt_id: rid, sku: `BR-TST-0${k + 1}`, title: 'Personalised 14k gold filled initial charm necklace, dainty bridesmaid gift, custom letter charm ' + k, quantity: 1, create_timestamp: created, created_timestamp: created, paid_timestamp: created, expected_ship_date: ship, is_digital: false, personalization: ['Name: Emily'], variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }, { formatted_name: 'Length', formatted_value: '16 inches' }, { formatted_name: 'Personalization', formatted_value: 'Emily' }], message_from_buyer: 'Please ship by the date, thank you so much! ' });
    return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', is_paid: true, create_timestamp: created, created_timestamp: created, update_timestamp: created, expected_ship_date: ship, status: 'Paid', transactions: [tx(0), tx(1)] }; };
  const snapshot = Array.from({ length: N }, (_, i) => receipt(i));
  const path0 = 'charmnest/sandbox/orders-fc9.json', meta = { path: path0, count: N, at: snapAt, takenBy: 'fc9' };
  bucket.files.set(path0, Buffer.from(JSON.stringify({ at: snapAt, count: N, receipts: snapshot })));
  db.seed({ 'Charm_Sandbox/current': meta });
  console.log(`snapshot: ${N} open orders, ${fmt(bucket.files.get(path0).length)} bytes in Storage (${fmt(bucket.files.get(path0).length / N)} per order)\n`);
  // the stream, played until every order has come
  const s0 = { on: true, v: 2, seed: 4242, speed: 1000, stepMs: STEP, min: 2, max: 5, simStart: Math.floor(now0 / STEP) * STEP, tick: 0, snapshotPath: path0, startedAt: now0, tickAt: now0, total: N };
  let k = 1; for (let got = 0; got < N; k++) got += etsy.batch(s0, snapshot, k, meta).length;
  const steps = k - 1;
  const done = Object.assign({}, s0, { tick: steps, simNow: s0.simStart + steps * STEP, brought: N, done: true });
  db.seed({ 'Charm_Sandbox/stream': done });
  // what the open list holds in a sandbox by then: every order claimed by the sorter (the gold dot), some finished at the station,
  // a few with a staff note or messages
  const ids = snapshot.map(r => String(r.receipt_id));
  const at = TS.fromMillis(now0 - 3600e3);
  const seed = {};
  ids.forEach(id => { seed['Sandbox_Design_RealTime_Selected_Orders/' + id] = { claimed: true, claimedBy: 'sorter', claimRun: 'run-1', claimAt: at, at, selected: false, selectedBy: null }; });
  ids.slice(0, Math.floor(N / 20)).forEach(id => { seed['Sandbox_Design_Completed Orders/' + id] = { orderId: id, completed: true, completedAt: TS.fromMillis(now0 - 30000) }; });
  ids.slice(0, 5).forEach(id => { seed['Sandbox_Brites_Orders/' + id] = { 'Staff Note': 'check the engraving', touched: now0 }; });
  ids.slice(0, 30).forEach((id, i) => { seed[`Sandbox_Brites_Orders/${id}/messages/m${i}`] = { text: 'hello', senderName: 'Staff', timestamp: at }; });
  db.seed(seed);
  console.log(`stream played ${steps} steps (${N} orders in); sandbox holds ${ids.length} claims, ${Math.floor(N / 20)} finished orders\n`);

  /* ═════ Part A: one check at "done" ═════ */
  console.log('PART A  one sandbox check, every order in (server work only)');
  const pages = async () => { const out = []; for (let off = 0; ; off += 100) { const r = await E({ fn: 'listOpenOrders', offset: off }); out.push(...r.results); if (r.results.length < 100) return out; } };
  const chunks = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; };
  const common = async () => {
    const tick = await measure('A.tick', () => L('sandboxStream', { action: 'tick', speed: 1000, expect: done.simNow })); line('  sorter: stream tick', tick.d);
    const arr = await measure('A.arrivalRecord', () => L('arrivalRecord', { orders: [], sandbox: true, now: done.simNow })); line('  sorter: arrivalRecord (no new order)', arr.d);
    const list = await measure('A.list', pages); line(`  station: listOpenOrders pages (${list.out.length} listed)`, list.d);
    return [tick.d, arr.d, list.d, list.out.length];
  };
  await E({ fn: "status" });
  const before = await (async () => {
    const [t, a, l, open] = await common();
    const openIds = (await pages()).map(r => String(r.receipt_id));
    const dc = await measure('A.dcFor', async () => { for (const c of chunks(openIds, 100)) await FO({ dcFor: c.join(',') }); }); line('  station BEFORE: dcFor, every open order', dc.d);
    const sn = await measure('A.staffNotesFor', async () => { for (const c of chunks(openIds, 100)) await FO({ staffNotesFor: c.join(',') }); }); line('  station BEFORE: staffNotesFor, every open order', sn.d);
    const rt = await measure('A.rt', () => FO({ rt: '1' })); line('  station BEFORE: rt=1 (all locks and claims)', rt.d);
    const sum = d => d.reads + (d.aggs || 0);
    const tot = { reads: sum(t) + sum(a) + sum(l) + sum(dc.d) + sum(sn.d) + sum(rt.d), bytes: t.bytes + a.bytes + l.bytes + dc.d.bytes + sn.d.bytes + rt.d.bytes };
    console.log(`  => BEFORE per check: ${fmt(tot.reads)} reads, ${fmt(tot.bytes)} bytes\n`);
    return tot;
  })();
  const after = await (async () => {
    const [t, a, l] = await common();
    const dcs = await measure('A.dcSince', () => FO({ dcSince: String(now0 - 60000) })); line('  station NOW: dcSince (completions since the last sweep)', dcs.d);
    const rts = await measure('A.rtSince', () => FO({ rtSince: String(now0 + 1000) })); line('  station NOW: rtSince (lock delta, nothing changed)', rts.d);
    const sum = d => d.reads + (d.aggs || 0);
    const tot = { reads: sum(t) + sum(a) + sum(l) + sum(dcs.d) + sum(rts.d), bytes: t.bytes + a.bytes + l.bytes + dcs.d.bytes + rts.d.bytes };
    console.log(`  => NOW per check: ${fmt(tot.reads)} reads, ${fmt(tot.bytes)} bytes (plus one full sweep of the BEFORE kind every 5 minutes)\n`);
    return tot;
  })();
  {   // a cold emulator instance (a new Netlify instance, a deploy, 15 minutes idle): reads which listed orders are finished, one document each, and downloads the snapshot
    delete require.cache[require.resolve(path.join(fnDir, 'etsySandbox.js'))];
    const cold = require(path.join(fnDir, 'etsySandbox.js'));
    const first = await measure('A.coldList', async () => { for (let off = 0; ; off += 100) { const r = JSON.parse((await cold.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: { fn: 'listOpenOrders', offset: off } })).body); if (r.results.length < 100) break; } });
    line('  a COLD emulator instance, first list (once per new instance)', first.d, `+ ${fmt(first.d.storage.downloadBytes)} bytes of snapshot downloaded from Storage`);
    console.log('');
  }
  const per = (x, n) => ({ reads: x.reads * n, bytes: x.bytes * n });
  const rate = [['1000x, a check about every 2.5 s (assumed pace; the Chromium replay measured 2.0 to 2.5 s per step; 0.6 s is the floor)', 1440, 1440], ['1000x at the 0.6 s floor', 6000, 6000], ['50x default (12 s)', 300, 300]];
  console.log('Per hour, one open sandbox tab, every order in (reads and Firestore bytes, server work above only):');
  for (const [label, nb] of rate) console.log(`  ${label.padEnd(86)} BEFORE ${fmt(before.reads * nb)} reads/h, ${fmt(before.bytes * nb / 1e6)} MB/h`);
  console.log(`  NOW  a check every 60 s (48 incremental + 12 full sweeps an hour)${' '.repeat(26)} ${fmt(after.reads * 48 + before.reads * 12)} reads/h, ${fmt((after.bytes * 48 + before.bytes * 12) / 1e6)} MB/h\n`);

  /* ═════ Part B: the loop's TTL'd reads of the shared maps and library, per call ═════ */
  console.log('PART B  shared reads the Sorter check repeats (per call; sizes are assumptions: the real collections are not readable from here)');
  const A = +process.env.ALIASES || 1500, O = +process.env.OPTMAP || 400, ND = +process.env.NODESIGN || 100;
  const seedB = {};
  for (let i = 0; i < A; i++) seedB['Charm_Sku_Aliases/' + (1700000 + i)] = { listingId: String(1700000 + i), sku: 'BR-TST-01', v: 2, by: 'operator', title: 'x'.repeat(120), updatedAt: at, bySku: { 'AB-1': 'BR-TST-01', 'AB-2': 'BR-TST-02' } };
  for (let i = 0; i < O; i++) seedB['Charm_Option_Map/' + (1700000 + i)] = { listingId: String(1700000 + i), map: { size: { small: { field: 'size', value: 'S', by: 'operator', at: 1 }, large: { field: 'size', value: 'L', by: 'operator', at: 1 } } }, updatedAt: at };
  for (let i = 0; i < ND; i++) seedB['Charm_Sku_NoDesign/n' + i] = { pattern: '^Chain only ' + i, by: 'operator', note: 'by title', createdAt: at };
  for (let i = 0; i < 200; i++) seedB['Charm_Master_Files/f' + i] = { masterHash: 'f' + i, name: 'BRITES-master-' + i + '.ai', indexedAt: at, skus: Array.from({ length: 300 }, (_, j) => 'BR-' + i + '-' + j) };
  db.seed(seedB);
  const maps = await measure('B.maps', async () => { await L('optionMapGet'); await L('aliasGet'); await L('noDesignGet'); }); line(`  loadMaps (optionMapGet+aliasGet+noDesignGet; ${A}+${O}+${ND} docs)`, maps.d);
  const mf = await measure('B.masterListFiles', () => L('masterListFiles')); line('  Master.load quiet (masterListFiles, 200 file records)', mf.d);
  console.log(`  per hour: every 60 s (TTL before) ${fmt((maps.d.reads) * 60)} + ${fmt(mf.d.reads * 30)} reads, ${fmt((maps.d.bytes * 60 + mf.d.bytes * 30) / 1e6)} MB;  every 10 min while quiet (now) ${fmt(maps.d.reads * 6 + mf.d.reads * 6)} reads, ${fmt((maps.d.bytes * 6 + mf.d.bytes * 6) / 1e6)} MB\n`);

  /* ═════ Part C: Reset the sandbox ═════ */
  console.log('PART C  Reset the sandbox on a seeded sandbox');
  const seedC = {}, K = Math.min(N, 300);
  const fam = ['Charm_Pool', 'Charm_Pool_Back', 'Charm_Nest_Sets', 'Charm_Nest_Counters', 'Charm_Nest_Runs', 'Charm_Nest_Run_Lines', 'Charm_Nest_Run_Live', 'Charm_Nest_Release', 'Charm_Nest_Arrivals', 'Charm_Nest_Cancelled', 'Charm_Nest_Cancelled_History', 'Charm_Custom_Orders', 'Charm_Custom_Sheet', 'Charm_Nest_Sheets', 'Charm_Nest_Rose_Stock', 'Design_Bridge', 'Order_Timeline', 'Station_Sessions', 'Station_Activity', 'Efficiency_Daily', 'Design_Order_Archive', 'Design_RealTime_Selected_Orders', 'Design_Completed Orders', 'Charm_Nest_Agent', 'Charm_Nest_Rose_Rehearsals'];
  for (const f of fam) for (let i = 0; i < K; i++) seedC[`Sandbox_${f}/${f.slice(0, 4)}${i}`] = { id: String(i), orderId: String(3521000000 + i), at: now0, payload: 'x'.repeat(200) };
  for (let i = 0; i < K; i++) { seedC[`Sandbox_Brites_Orders/${3521000000 + i}`] = { touched: now0 }; seedC[`Sandbox_Brites_Orders/${3521000000 + i}/messages/m1`] = { text: 'hi', timestamp: at }; seedC[`Sandbox_Charm_Nest_Rose_Stock/Char${i}/cuts/c1`] = { x: 1 }; }
  for (let i = 0; i < 40; i++) seedC['EtsyMail_OrderLinks/olsb_' + i] = { sandbox: true, receiptId: String(i) };
  seedC['EtsyMail_OrderLinks/ol_keep'] = { sandbox: false };
  for (let i = 0; i < 30; i++) { bucket.files.set(`charmnest/sandbox/sets/f${i}.ai`, Buffer.alloc(50)); }
  seedC['Charm_Production_Canary/keep'] = { keep: true };
  db.seed(seedC);
  const kept = ['Charm_Production_Canary/keep', 'EtsyMail_OrderLinks/ol_keep', 'Charm_Sandbox/current'];
  const sandboxLeft = () => [...db.docs.keys()].filter(p => /^Sandbox_/.test(p) && !/^Sandbox_Charm_Nest_Agent_Cache/.test(p));
  const seeded = sandboxLeft().length;
  let calls = 0, more = true; const a = m.snapshot(); let tot = null;
  await m.op('C.reset', async () => { while (more && calls < 400) { const r = await L('sandboxReset'); calls++; more = !!r.more; } });
  const d = m.since(a);
  line(`  sandboxReset: ${seeded} sandbox docs in ${calls} call(s)`, d, `(${(d.reads + (d.aggs || 0)) / seeded > 0 ? ((d.reads + (d.aggs || 0)) / seeded).toFixed(2) : 0} reads per document deleted)`);
  assert.strictEqual(sandboxLeft().length, 0, 'every sandbox record is gone: ' + sandboxLeft().slice(0, 5));
  assert(!db.has('Charm_Sandbox/stream'), 'the stream is gone');
  for (const p of kept) assert(db.has(p), 'kept: ' + p);
  assert.strictEqual([...bucket.files.keys()].filter(f => f.startsWith('charmnest/sandbox/sets/')).length, 0, 'sandbox files gone');
  assert(bucket.files.has(path0), 'the snapshot file stays');
  console.log('  verified: every Sandbox_ record, the stream and the sandbox files are gone; production canary, olsb-less inbox record and the snapshot stay\n');
  m.uninstall();
})().catch(e => { console.error(e); process.exit(1); });
