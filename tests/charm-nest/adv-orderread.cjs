// ADV-ORDERREAD (Paul, 15 Oct 2026: "perform your tests as if we are running the complete Etsy production order pulls"): reading the open orders of a shop with 400 orders in several pages.
//   1 · the station walks Etsy's open list by offset, newest first. An order that arrives while the pages are walked pushes every later one down a place, so the last order of one page is the first of the
//       next: the list held that receipt twice and the sorter pulled its lines twice (two rows with one key, one order counted twice). A receipt is listed once now, the fresher copy in the first copy's place.
//   2 · the sorter's pull (run start and Resume) stopped with "only 399 of 400 orders could be read" for as long as ONE open order stayed unreadable (no items, a refused read): no run could start and no run could
//       be restored. The check that brings new orders in already lets such an order wait for the next check; the pull does the same now. A run being restored still needs its OWN orders whole.
//   3 · one line the reader cannot read (it throws) stopped interpretAll, so no new order came in and no run could start or be resumed: every line is read in the same pass, and the unreadable one waits for a
//       person with one plain line (like a form-guard hold: no design on the row, nothing pooled) while the others go on. A line the reader reads is read exactly as before.
// Offline: the station's and the sorter's own source, sliced into a vm over stand-ins (no Etsy, no cloud, nothing is written anywhere).
//   node tests/charm-nest/adv-orderread.cjs
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const station = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
let n = 0; const ok = name => { n++; console.log('  ✓ ' + name); };
const plain = x => JSON.parse(JSON.stringify(x));   // (arrays made inside the vm have the vm's own Array: copy them out before comparing)
const slice = (src, from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a); assert(a >= 0 && b > a, 'source not found: ' + from); return src.slice(a, b); };

(async () => {
  /* ── 1 · the station's walk of the open list ── */
  const walkSrc = slice(station, 'async function fetchOpenOrdersSequential(onPage) {', '/** Decorate a raw Etsy receipt');
  const walk = async (listOf, onFirstPage) => {
    let live = listOf(), calls = 0;
    const c = { PAGE_SIZE: 100, sleep: async () => {}, apiFetch: async url => { const off = +/offset=(\d+)/.exec(url)[1], page = live.slice(off, off + 100); if (++calls === 1 && onFirstPage) live = onFirstPage(live); return { ok: true, json: async () => ({ results: page }), text: async () => '' }; }, console };
    vm.createContext(c); vm.runInContext(walkSrc + '\nthis.walk = fetchOpenOrdersSequential;', c);
    const seen = []; const all = await c.walk(k => seen.push(k)); return { all, seen, calls };
  };
  const mk = id => ({ receipt_id: id, update_timestamp: 1 });
  const base = () => Array.from({ length: 250 }, (_, i) => mk(2000 - i));       // 250 open orders, newest first
  {
    const quiet = await walk(base);
    assert.equal(quiet.all.length, 250); assert.deepEqual(plain(quiet.all.map(r => r.receipt_id)), base().map(r => r.receipt_id)); assert.deepEqual(quiet.seen, [100, 200, 250], 'progress counts the receipts held');
    ok('a quiet shop: 250 orders in 3 pages, in Etsy\'s order');
    const shifted = await walk(base, live => [mk(2001)].concat(live));            // a new order arrives after page 1 was read
    const ids = plain(shifted.all.map(r => r.receipt_id));
    assert.equal(new Set(ids).size, ids.length, 'no receipt twice: ' + ids.filter((x, i) => ids.indexOf(x) !== i));
    assert.deepEqual(ids, base().map(r => r.receipt_id), 'all 250 are there, in order (the new one is the next sweep\'s)');
    ok('an order arrives between page 1 and page 2: the last order of page 1 is not listed again');
    const stamped = await walk(base, live => [mk(2001)].concat(live.map(r => r.receipt_id === 1901 ? Object.assign({}, r, { update_timestamp: 9 }) : r)));
    assert.equal(stamped.all.find(r => r.receipt_id === 1901).update_timestamp, 9, 'the later copy, the fresher, stands');
    assert.equal(stamped.all.findIndex(r => r.receipt_id === 1901), 99, 'in the place of the first');
    ok('the fresher copy of a repeated receipt stands, in the first copy\'s place');
    const shorter = await walk(base, live => live.filter(r => r.receipt_id !== 1950));   // an order ships meanwhile: one later order is skipped (the next sweep reads it)
    const ids2 = plain(shorter.all.map(r => r.receipt_id)); assert.equal(new Set(ids2).size, ids2.length); assert(ids2.length >= 248);
    ok('an order shipped meanwhile: no repeat, nothing breaks');
    const odd = await walk(() => [{ receipt_id: null }, {}, { receipt_id: 7 }, { receipt_id: '7' }]);
    assert.equal(odd.all.length, 3, 'two receipts with no id are two (they are not folded into one), 7 and "7" are one');
    ok('a receipt with no id is kept as it is');
  }

  /* ── 2 · the sorter's pull ── */
  const pullSrc = slice(bridge, '  async function pull(run, { silent = false, receiptIds = null } = {}) {', '  let liveEv = null;');
  const line = (rid, tx) => ({ transactionId: tx, listingId: '1', sku: 'A', title: 't', quantity: 1, variations: [] });
  const order = (rid, hydrated = true) => ({ receiptId: String(rid), updateTs: 1, hydrated, lines: hydrated ? [line(rid, rid + '1'), line(rid, rid + '2')] : [] });
  const pullWith = async (orders, opts, snapOver) => {
    const logs = [], recorded = [], B = { orders: {} }, run = { runId: 'r1' }, saved = [];
    const hydrated = orders.filter(o => o.hydrated).length;
    const c = { DesignLink: { ensure: async () => {}, etsyBudgetOk: () => true, call: async () => Object.assign({ total: orders.length, hydrated, etsy: { signedIn: true }, orders }, snapOver || {}), meter() {} }, Sandbox: { ready: async () => {} }, loadMaps: async () => {}, Master: { load: async () => {} },
      window: {}, B, applyPullRule: o => o, Arrivals: { record: async list => { recorded.push(list.map(o => o.receiptId)); }, at: () => 1 }, TL: { pulled() {} }, O: { lineKey: (o, l) => o.receiptId + '_' + l.transactionId }, Carry: { adopt() {} }, interpretAll() {},
      RunCtl: { save: async r => { saved.push(JSON.parse(JSON.stringify(r))); } }, lineRecord: row => [row.key, {}], agent: (a, k, t) => logs.push(k + ': ' + t), toast() {}, groupPieces: rows => rows.length, render() {}, ListMedia: { prepare() {} }, agentLiveLine() {}, Date, Set, Map, Error, String, Promise, Object, Array };
    vm.createContext(c); vm.runInContext(pullSrc + '\nthis.pull = pull;', c);
    let error = null, rows = null; try { rows = await c.pull(opts && opts.noRun ? null : run, opts && opts.receiptIds ? { silent: true, receiptIds: opts.receiptIds } : { silent: true }); } catch (e) { error = e; }
    return { error, rows, logs, recorded, B, saved, run };
  };
  {
    const four = await pullWith([order(1), order(2), order(3), order(4)]);
    assert(!four.error); assert.equal(four.rows.length, 8); assert.deepEqual(four.recorded, [['1', '2', '3', '4']]);
    ok('four readable orders: 8 lines, as before');

    const dup = await pullWith([order(1), order(2), order(2), order(3)], null, { total: 4, hydrated: 4 });
    assert(!dup.error); assert.equal(dup.rows.length, 6, 'order 2 once: ' + dup.rows.map(r => r.key));
    assert.equal(new Set(dup.rows.map(r => r.key)).size, dup.rows.length, 'one key, one row'); assert.deepEqual(dup.recorded, [['1', '2', '3']], 'the arrival ledger is told order 2 once');
    assert.deepEqual(dup.saved[0].orders, ['1', '2', '3'], 'the run lists order 2 once'); assert.equal(dup.B.orders.filtered, 0, 'a repeat is not an order the pull rule left out');
    ok('an order listed twice in the snapshot is one order: one row per line, one arrival, one entry in the run');

    const one = await pullWith([order(1), order(2, false), order(3), order(4)]);
    assert(!one.error, 'one unreadable order does not stop the run start: ' + (one.error && one.error.message));
    assert.deepEqual(plain(one.rows.map(r => r.order.receiptId)), ['1', '1', '3', '3', '4', '4']);
    assert.deepEqual(one.recorded, [['1', '3', '4']], 'the unread order is not recorded as arrived');
    assert.deepEqual(one.saved[0].orders, ['1', '3', '4'], 'and is not in the run');
    assert(one.logs.some(l => /^warn: 1 of 4 order\(s\) could not be read from Etsy \(2\)/.test(l) && /the other 3 came in now/.test(l)), 'it is named in the log: ' + one.logs.join(' | '));
    ok('one unreadable order of four: the other three come in, the unread one is named and left for the next check');

    const own = await pullWith([order(1), order(2, false), order(3), order(4)], { receiptIds: ['2', '3'], noRun: true });
    assert(own.error && /only 3 of 4 orders could be read from Etsy/.test(own.error.message), 'a run being restored whose own order is unread still stops: ' + (own.error && own.error.message));
    ok('restoring a run whose OWN order is unreadable still stops, loudly, as before');

    const other = await pullWith([order(1), order(2, false), order(3), order(4)], { receiptIds: ['3', '4'], noRun: true });
    assert(!other.error, 'an unrelated unreadable order does not stop a restore: ' + (other.error && other.error.message));
    assert.deepEqual(plain(other.rows.map(r => r.order.receiptId)), ['3', '3', '4', '4']);
    ok('restoring a run: an unreadable order that is not the run\'s does not stop it');

    const none = await pullWith([order(1, false), order(2, false)], null, { etsy: { signedIn: false } });
    assert(none.error && /only 0 of 2 orders could be read from Etsy — the station is not signed in: press Connect Etsy/.test(none.error.message), String(none.error && none.error.message));
    ok('nothing readable (not signed in): stops with the old words');
  }

  /* ── 3 · a line the reader cannot read ── */
  {
    const O = require(path.join(root, 'charm-nest-orders.js'));
    const guard = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/huggie-guard-world.json'), 'utf8'));
    const M = new Map(guard.master.map(e => [e.sku.toUpperCase(), e])), loose = new Map([...M.keys()].map(k => [O.looseKey(k), k]));
    const ctx = () => ({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: ['CHAIN ONLY'] }, masterEntry: x => M.get(String(x || '').toUpperCase()) || null, masterLoose: x => loose.get(O.looseKey(x)) || '' });
    const logs = [], readSrc = slice(bridge, '  function readLine(row) {', '  function interpretAll() {');
    const c = { O, ctx, agent: (a, k, t) => logs.push(k + ': ' + t), Object, String, Number, Error };
    vm.createContext(c); vm.runInContext(readSrc + '\nthis.readLine = readLine;', c);
    const SKU = [...M.keys()].find(k => /^ACOUSTIC_23145$/.test(k)) || [...M.keys()][0];
    const ord = { receiptId: '4190000009', updateTs: 1, createTs: 1, staffNote: '', buyerMessage: '', messages: [] };
    const ln = over => Object.assign({ transactionId: '5300000009', listingId: '1111', sku: SKU, title: 'Charm necklace', quantity: 3, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold Filled' }] }, over || {});
    const good = c.readLine({ order: ord, line: ln() });
    assert.deepEqual(plain(good), plain(O.interpretLine(ord, ln(), ctx())), 'a line the reader reads is read exactly as before'); assert.deepEqual(logs, []);
    assert.equal(good.designSku, SKU); assert.deepEqual(plain(good.problems), []);
    assert.throws(() => O.interpretLine(ord, ln({ variations: [null, { name: 'Metal Choice', value: 'Gold Filled' }] }), ctx()), /null/, 'the reader itself throws on this line (a null among the options): the fault this guards');
    ok('a line the reader reads is read exactly as before, with nothing logged');
    const bad = c.readLine({ order: ord, line: ln({ variations: [null, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['Mom'] }) });
    assert.equal(bad.designSku, null, 'no design on the row: ' + bad.designSku); assert.equal(bad.skuSource, null);
    assert.equal(bad.problems[0].kind, 'unmatchedSku'); assert.equal(bad.problems[0].formHold, true); assert.equal(bad.problems[0].readFailed, true);
    assert.match(bad.problems[0].reason, /^this line could not be read \(.*\): it waits for a person, the other lines go on$/); assert.equal(bad.problems[0].sku, SKU);
    assert.equal(bad.formHold.reason, bad.problems[0].reason); assert.equal(bad.formHold.readFailed, true); assert.equal(bad.noDesign, false);
    assert.equal(bad.quantity, 3, 'the quantity is kept'); assert.equal(O.pieceCountOf(bad), 3, 'and counts as 3 pieces'); assert.equal(bad.material, 'gold', 'and so is the metal');
    assert(Array.isArray(bad.options) && bad.pair && Array.isArray(bad.pair.asks), 'the spec is whole: every reader of a spec finds what it looks for');
    assert(logs.length === 1 && /^warn: 4190000009 · /.test(logs[0]) && /could not be read/.test(logs[0]), 'the activity log names the order: ' + logs.join(' | '));
    ok('a line that makes the reader throw waits with one plain line, no design, its quantity and metal kept, and is named in the log');
    const nometal = c.readLine({ order: ord, line: ln({ variations: [null], metalKey: '' }) });
    assert.deepEqual(plain(nometal.problems.map(p => p.kind)), ['unmatchedSku', 'needsMaterial'], 'a metal nobody read is still asked about'); assert.equal(nometal.problems[0].formHold, true);
    ok('its metal is still asked about when the line does not say it');
    const listed = c.readLine({ order: ord, line: ln({ sku: 'CHAIN ONLY', variations: [null] }) });
    assert.equal(listed.noDesign, true); assert.deepEqual(plain(listed.problems), [], 'a line on the no-design list is cut from nothing: nothing waits on it');
    ok('a line on the no-design list is still nothing to cut');
    const worse = ln({ variations: [null] }); const OThrows = { interpretLine() { throw new Error('boom'); } };
    const c2 = { O: OThrows, ctx, agent() {}, Object, String, Number, Error }; vm.createContext(c2); vm.runInContext(readSrc + '\nthis.readLine = readLine;', c2);
    assert.throws(() => c2.readLine({ order: ord, line: worse }), /boom/, 'a reader that fails on a line with nothing on it is a fault bigger than one line: it throws as before');
    ok('a reader broken for every line still says so, as before');
  }

  /* ── 5 · a shop day: 400 receipts as Etsy sends them, through the station's reader and the sorter's ── */
  {
    const O = require(path.join(root, 'charm-nest-orders.js'));
    const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/addon-snapshot-lines.json'), 'utf8')), guard = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/huggie-guard-world.json'), 'utf8'));
    // the station's own line reader, sliced out of the page (design-1.html): metal classifier, decorateReceipt, lineForBridge
    const st = { orderCache: {}, receiptStamp: r => Number(r.update_timestamp || 0), txNeedsAttention: () => false, console };
    vm.createContext(st);
    vm.runInContext(station.slice(station.indexOf('const METALS = ['), station.indexOf('const PAGE_SIZE =')) + '\n' + station.slice(station.indexOf('function normMetalText(s)'), station.indexOf('/* ═══ 5 · ETSY REQUEST PIPELINE')) + '\n' + slice(station, 'function metalLabelFor(tx) {', '\n}\n') + '\n}\n' + slice(station, 'function decorateReceipt(r) {', "/** Load one order's items") + '\n' + slice(station, '  const metalOf = tx =>', '  /* Staff notes and messages read for the sorter') + '\n;this.api = { lineForBridge, decorateReceipt, metalOf };', st);
    const api = st.api;
    const M = new Map(guard.master.map(e => [e.sku.toUpperCase(), e])); for (const k of Object.keys(FIX.known)) if (!M.has(k)) M.set(k, Object.assign({ sku: k }, FIX.known[k].blocked ? { blocked: FIX.known[k].blocked } : {}, FIX.known[k].sizes ? { sizes: Object.fromEntries(FIX.known[k].sizes.map(z => [z, {}])) } : {}));
    const loose = new Map([...M.keys()].map(k => [O.looseKey(k), k]));
    const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: x => M.get(String(x || '').toUpperCase()) || null, masterLoose: x => loose.get(O.looseKey(x)) || '' }, extra || {});
    const SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
    const orderOf = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'B ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], metals: {}, lines });
    const byRid = new Map(); for (const l of FIX.lines) { if (!byRid.has(l.rid)) byRid.set(l.rid, []); byRid.get(l.rid).push(l); }
    const rids = [...byRid.keys()], recs = [];
    for (let i = 0; i < 400; i++) recs.push({ receipt_id: String(4180000000 + i), order_number: String(4180000000 + i), update_timestamp: SHIP - 5 * 86400 + i, create_timestamp: SHIP - 5 * 86400, name: 'Buyer ' + i, message_from_buyer: '',
      transactions: byRid.get(rids[i % rids.length]).map((l, j) => ({ transaction_id: String(5300000000 + i * 10 + j), listing_id: Number(l.listing), product_id: 1, sku: l.sku, title: l.title, quantity: l.q, expected_ship_date: SHIP, variations: l.vars.map(([nm, v], k) => ({ property_id: 100 + k, value_id: 900 + k, formatted_name: nm, formatted_value: v })) })) });
    const heap0 = process.memoryUsage().heapUsed, t0 = Date.now();
    const orders = recs.map(r => { api.decorateReceipt(r); return orderOf(r.receipt_id, r.transactions.map(tx => api.lineForBridge(tx, r))); });
    assert.equal(orders.reduce((a, o) => a + o.lines.length, 0), recs.reduce((a, r) => a + r.transactions.length, 0), 'no transaction is dropped by the station\'s reader');
    O.cleanOrders({ orders });
    const lines = orders.flatMap(o => o.lines.map(l => ({ o, l }))), keys = lines.map(x => O.lineKey(x.o, x.l));
    assert.equal(new Set(keys).size, keys.length, 'every line has its own key');
    const seen = O.seenSkus(lines.map(x => x.l)), count = { read: 0, held: 0, noDesign: 0, special: 0 }; let pieces = 0;
    for (const { o, l } of lines) {
      const sp = O.interpretLine(o, l, ctx({ seen }));       // (a throw here fails the test: no line of a real shop's day may throw)
      const verdict = sp.noDesign ? 'noDesign' : sp.special ? 'special' : sp.problems.length ? 'held' : sp.designSku ? 'read' : 'SILENT';
      assert.notEqual(verdict, 'SILENT', `${o.receiptId}/${l.transactionId} ${l.sku}: neither read nor held`);
      if (verdict === 'held') for (const p of sp.problems) assert(p.kind && (p.kind !== 'unmatchedSku' || (p.reason && String(p.reason).length > 5)), `${l.sku}: a held line says why: ${JSON.stringify(p).slice(0, 120)}`);
      count[verdict]++;
      const ps = O.piecesOf({ spec: sp, line: l }); assert(Number.isInteger(sp.pieceCount) && sp.pieceCount >= 1 && ps.length === sp.pieceCount, `${l.sku}: ${ps.length} pieces for a count of ${sp.pieceCount}`); pieces += ps.length;
    }
    const took = Date.now() - t0, grew = (process.memoryUsage().heapUsed - heap0) / 1e6;
    assert(count.read >= 350 && count.held >= 1, JSON.stringify(count)); assert(took < 30000, `455 lines in ${took} ms`); assert(grew < 400, `the heap grew ${grew | 0} MB`);
    ok(`400 receipts (${lines.length} lines, ${pieces} pieces): none throws, none dropped, none silent (${JSON.stringify(count)}), ${took} ms, +${grew | 0} MB`);

    // the odd lines a real day holds, one by one: what each reads as
    const LN = over => Object.assign({ transactionId: '5300000001', listingId: '1111', sku: 'ACOUSTIC_23145', title: 'Guitar charm necklace', quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Necklace Length', value: '16"' }] }, over || {});
    const rd = (over, extra) => { const l = LN(over), sp = O.interpretLine(orderOf('4190000001', [l]), l, ctx(extra)); return sp; };
    assert.equal(rd().designSku, 'ACOUSTIC_23145'); assert.deepEqual(rd().problems, []);
    assert.equal(rd({ quantity: 0 }).quantity, 1, 'Etsy never sends 0: it reads as one'); assert.equal(rd({ quantity: 50 }).pieceCount, 50, 'quantity 50 is 50 pieces'); assert.equal(rd({ quantity: 50 }).quantity, 50);
    for (const metalKey of ['', 'platinum', 'solid_unknown']) assert.deepEqual(rd({ metalKey }).problems.map(p => p.kind), ['needsMaterial'], 'an unknown metal (' + metalKey + ') is asked about');
    assert.deepEqual(rd({ sku: 'NO_SUCH_DESIGN_9' }).problems.map(p => p.kind + ':' + p.reason), ['unmatchedSku:not in any master file'], 'a SKU with no design is held with its reason');
    assert.equal(rd({ sku: '' }).designSku, null); assert.deepEqual(rd({ sku: '' }).problems.map(p => p.kind), ['unmatchedSku']);
    for (const raw of [' acoustic_23145 ', 'ACOUSTIC_23145 ', 'ACOUSTIC_​23145', 'acoustic_23145-co']) { const sp = rd({ sku: raw }); assert.equal(sp.designSku, 'ACOUSTIC_23145', JSON.stringify(raw) + ' is that design'); assert.deepEqual(sp.problems, []); }
    for (const variations of [[], undefined]) assert.equal(rd({ variations }).designSku, 'ACOUSTIC_23145', 'a line with no options is still read');
    const words = ['Forever & Always '.repeat(70).slice(0, 1024), Array.from({ length: 30 }, (_, i) => 'Line ' + i).join('\n'), 'Mom 💛 Dad 👨‍👩‍👧‍👦 🇨🇦', 'עברית é ñ', 'A\r\nB\r\n\r\nC', 'Mom&#39;s &amp; Dad&#39;s'];
    for (const w of words) { const sp = rd({ personalization: [w] }); assert.equal(sp.engraveCandidate, true); assert.equal(sp.designSku, 'ACOUSTIC_23145'); assert.deepEqual(sp.problems, []); }
    assert.deepEqual(rd({ personalization: ['Mom&#39;s &amp; Dad&#39;s'] }).personalization, ["Mom's & Dad's"], 'the customer\'s words are read decoded');
    assert.deepEqual(rd({ personalization: ['Mom 💛 Dad 👨‍👩‍👧‍👦 🇨🇦'] }).personalization, ['Mom 💛 Dad 👨‍👩‍👧‍👦 🇨🇦'], 'emoji (a family, a flag) come through whole');
    const l30 = Array.from({ length: 30 }, (_, i) => LN({ transactionId: String(5300000000 + i), sku: i % 3 ? 'ACOUSTIC_23145' : 'NOSUCH_' + i })), o30 = orderOf('4190000030', l30), seen30 = O.seenSkus(l30);
    const r30 = l30.map(l => O.interpretLine(o30, l, ctx({ seen: seen30 }))); assert.equal(new Set(l30.map(l => O.lineKey(o30, l))).size, 30); assert.equal(r30.filter(x => !x.problems.length).length, 20); assert.equal(r30.filter(x => x.problems.length).length, 10);
    ok('the odd lines: quantity 0 and 50, an unknown metal, a SKU with no design or none, SKU spellings, no options, long / multi-line / emoji / RTL / escaped words, an order of 30 lines');

    // the station's reader on transactions Etsy leaves fields out of
    const odd = [{ transaction_id: 1, quantity: 2 }, { transaction_id: 2, listing_id: null, sku: null, title: null, quantity: null, variations: null }, { transaction_id: 3, variations: [null, { formatted_name: 'Metal', formatted_value: 'Gold Filled' }] }, { transaction_id: 4, personalization: 'Mom', variations: [{ formatted_name: 'Personalization', formatted_value: 'Not requested on this item.' }] }, { transaction_id: 5, variations: [{ formatted_name: 'Personalization', formatted_value: 'Mom &quot;M&quot;' }] }];
    const rec = { receipt_id: 4190000099, update_timestamp: 1, message_from_buyer: null, transactions: odd.map(x => Object.assign({}, x)) };
    api.decorateReceipt(rec);
    const got = rec.transactions.map(tx => api.lineForBridge(tx, rec)); assert.equal(got.length, 5, 'every transaction becomes a line');
    assert.deepEqual(plain(got.map(l => [l.transactionId, l.quantity, l.sku, l.listingId, l.personalization])), [['1', 2, '', '', []], ['2', 1, '', '', []], ['3', 1, '', '', []], ['4', 1, '', '', []], ['5', 1, '', '', ['Mom "M"']]]);
    assert.equal(got[2].metalKey, 'gold', 'a null among the options does not hide the metal');
    const oo = orderOf('4190000099', got); for (const l of got) assert.doesNotThrow(() => O.interpretLine(oo, l, ctx()), 'transaction ' + l.transactionId);
    ok('the station reads transactions Etsy leaves fields out of (no SKU, listing, title, quantity or options, a null among the options) into lines the sorter reads without a throw');
  }

  /* ── 4 · the real page: a line the reader cannot read among two it can ── */
  {
    const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
    let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser part was not run'); }
    if (chromium) {
      const { start } = require('./bridge-server.cjs');
      const srv = await start({ receipts: [] });
      const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
      try {
        const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
        await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
        await context.addInitScript(cfg => { try { localStorage.setItem('cn.settings', JSON.stringify(cfg)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; }, { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
        const page = await context.newPage(); page.setDefaultTimeout(20000);
        const errors = []; page.on('pageerror', e => errors.push(e.message));
        await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
        await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.Pool && CN.S.cloud.ok === true, null, { timeout: 60000 });
        const r = await page.evaluate(async () => {
          await Orders.loadMaps(true);
          const mk = (i, over) => { const rid = String(4290000500 + i), order = { receiptId: rid, orderNumber: rid, createTs: 1789000000 + i, updateTs: 1789000000 + i, shipBy: 1790000000, buyer: { name: 'Buyer ' + i }, buyerMessage: '', staffNote: '', messages: [],
              lines: [Object.assign({ transactionId: rid + '1', listingId: String(1990000 + i), sku: 'NOPE_' + i, title: 'Nope ' + i, quantity: 2, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF', personalization: [], buyerMessage: '' }, over || {})] };
            const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
            const row = { key, order, line, arrivedAt: Date.now() - i, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
            B.orders.rows.push(row); B.orders.byKey.set(key, row); return key; };
          const a = mk(0), bad = mk(1, { sku: 'BAD_ONE', variations: [null, { name: 'Metal', value: '14k Gold Filled' }] }), c = mk(2);
          let threw = ''; try { Orders.interpretAll(); Review.syncOrderItems(); } catch (e) { threw = String(e && e.message); }
          const row = k => B.orders.byKey.get(k), sp = k => row(k).spec;
          const out = { threw, a: sp(a) && sp(a).problems.map(p => p.kind), c: sp(c) && sp(c).problems.map(p => p.kind), bad: sp(bad) && { design: sp(bad).designSku, hold: !!sp(bad).formHold, why: sp(bad).formHold && sp(bad).formHold.reason, qty: sp(bad).quantity, probs: sp(bad).problems.map(p => p.kind + ':' + !!p.readFailed) }, badRowProblems: row(bad).problems.map(p => p.kind) };
          await Pool.poolAdd(row(bad), null);
          Object.assign(out, { badState: row(bad).state, badReason: row(bad).reason, badPool: row(bad).poolIds.length, cards: Review.items().filter(it => it.kind === 'unmatchedSku' && (it.rows || [it.row]).some(x => x.key === bad)).length });
          return out;
        });
        assert.equal(r.threw, '', 'interpretAll does not throw: ' + r.threw);
        assert.deepEqual(r.a, ['unmatchedSku'], 'the line before it is read (its SKU is in no master file: a plain question)'); assert.deepEqual(r.c, ['unmatchedSku'], 'and so is the line after it');
        assert.equal(r.bad.design, null); assert.equal(r.bad.hold, true); assert.match(r.bad.why, /could not be read/); assert.equal(r.bad.qty, 2); assert.deepEqual(r.bad.probs, ['unmatchedSku:true']); assert.deepEqual(r.badRowProblems, ['unmatchedSku']);
        assert.equal(r.badState, 'unmatched'); assert.match(r.badReason, /could not be read/); assert.equal(r.badPool, 0, 'nothing of it is pooled'); assert.equal(r.cards, 1, 'and a person finds it among the Unknown SKU cards');
        assert.deepEqual(errors, [], 'no page errors');
        ok('the real page: the unreadable line among two readable ones waits as "unmatched" with its reason, is not pooled, has its Review card; the others are read');
      } finally { await browser.close(); await srv.close(); }
    }
  }
  console.log('adv-orderread: ' + n + ' checks passed');
})().catch(e => { console.error(e); process.exitCode = 1; });
