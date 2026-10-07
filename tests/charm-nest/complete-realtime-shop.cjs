// The LARGE shop of tests/charm-nest/complete-realtime.cjs (LB2, 7 Oct 2026): Paul's screenshot reads Orders 359, Review 176, Engrave 26, and the Library holds about 300
// sheets. Offline fixture data only: the cloud documents go into the fake backend (placement-oracle.cjs `backend`), the same orders go into every page's rows.
//   buildShop() -> { orders: [[order, [poolId|null per line]]], sheets, pool, sets, subject: {complete, print}, counts }
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', SETS = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000), TODAY = '2026-10-06', RUN = 'run-large';
const N_SHEETS = 300, N_ORDERS = 359, N_REVIEW = 176, N_ENGRAVE = 26;

const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
// a piece in Review that asks a question (a rework: its SKU is in no master file, so it is one decision, counted on the Review badge); Complete Order or Print QR label settles it
const chainLine = (tid, n) => Object.assign(line(tid, 'RE_' + (5000 + n), null, ''), { title: 'MODIFICATION REWORK FREE SHIPPING', variations: [{ name: 'Price', value: String(100 + n) }], metalKey: null, metalLabel: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP + (+rid.slice(-3)) * 60, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const pidOf = (rid, tid, copy = 1) => `${rid}_${tid}_${copy}`;
const sheetId = i => 'sh-' + String(i).padStart(3, '0');
const setIdOf = i => 'set-' + String(Math.floor(i / 3)).padStart(3, '0');
const metalOf = i => (i % 2 ? 'silver' : 'gold');
const CODE = { gold: 'GF', silver: 'SS' };
const SKUS = ['TEST-1', 'TEST-2', 'TEST-3', 'TEST-4', 'TEST-5'];

function buildShop() {
  const orders = [], sheets = Array.from({ length: N_SHEETS }, (_, i) => ({ id: sheetId(i), i, metal: metalOf(i), items: [] }));
  const subjects = {};
  const onSheet = (rid, tid, sku, metal, sheetIndex) => {
    const sh = sheets[sheetIndex]; sh.items.push({ rid, tid, sku, poolId: pidOf(rid, tid) }); return pidOf(rid, tid);
  };
  // the two subjects (Paul's screenshot: a piece in Review (chain only) and a piece on a sheet): one is completed, one is printed
  for (const [which, rid, sh] of [['complete', '4191000001', 3], ['print', '4191000002', 4]]) {
    const metal = metalOf(sh), ml = metal === 'silver' ? 'Sterling Silver' : '14k Gold Filled';
    const chain = chainLine(rid + '1', 1), charm = line(rid + '2', SKUS[0], metal, ml);
    orders.push([order(rid, 'Subject ' + which, [chain, charm]), [null, onSheet(rid, rid + '2', SKUS[0], metal, sh)]]);
    subjects[which] = { rid, chainKey: `${rid}_${rid}1`, charmKey: `${rid}_${rid}2`, sheetId: sheetId(sh), sheetIndex: sh };
  }
  // the rest of Review: orders that are one piece in Review (chain only)
  for (let n = 0; n < N_REVIEW - 2; n++) { const rid = String(4190000100 + n); orders.push([order(rid, 'Chain ' + n, [chainLine(rid + '1', 10 + n)]), [null]]); }
  // regular orders: about 110 on the sheets that are in use (the first 45), the rest waiting; N_ENGRAVE of them are engraved pieces
  const regular = N_ORDERS - orders.length, engraving = [];
  for (let n = 0; n < regular; n++) {
    const rid = String(4192000100 + n), metal = n % 3 ? 'gold' : 'silver', ml = metal === 'silver' ? 'Sterling Silver' : '14k Gold Filled', sku = SKUS[n % SKUS.length];
    const l = line(rid + '1', sku, metal, ml); if (n < N_ENGRAVE) { l.personalization = [{ name: 'Personalization', value: 'Mia ' + n }]; engraving.push(rid); }
    const on = n < 110;
    const si = on ? (metal === 'silver' ? 5 : 6) + (n % 20) * 2 : null;   // (a sheet of the metal: gold sheets have even numbers, silver odd)
    orders.push([order(rid, 'Buyer ' + n, [l]), [on ? onSheet(rid, rid + '1', sku, metal, si) : null]]);
  }
  // the other sheets hold what was cut before (orders that are no longer open): a library of N_SHEETS in all
  for (const sh of sheets) { let k = 0; while (sh.items.length < 24) { const rid = String(4170000000 + sh.i * 40 + k++); sh.items.push({ rid, tid: rid + '1', sku: SKUS[k % 5], poolId: pidOf(rid, rid + '1') }); } }
  return { orders, sheets, subjects, engraving, counts: { orders: orders.length, review: N_REVIEW, engrave: N_ENGRAVE, sheets: N_SHEETS } };
}

/** the cloud documents of the shop: [collection, id, doc] */
function cloudDocs(shop) {
  const now = Date.now(), out = [], runSheets = {};
  const sets = new Map();
  for (const sh of shop.sheets) {
    const sid = setIdOf(sh.i), seq = Math.floor(sh.i / 3) + 1, idx = (sh.i % 3) + 1, fileBase = `${CODE[sh.metal]}_${TODAY}_Set-${seq}_Sheet-${idx}`;
    const charms = [], placements = [], poolIds = [], orders = [];
    sh.items.forEach((it, i) => {
      const id = `${sh.id}-c${i}`; charms.push({ id, poolId: it.poolId, order: it.rid, sku: it.sku, name: `${it.rid} · ${it.sku}` }); poolIds.push(it.poolId); if (!orders.includes(it.rid)) orders.push(it.rid);
      placements.push({ id, cxPt: 30 + (i % 6) * 40, cyPt: 30 + Math.floor(i / 6) * 40, angle: 0, wPt: 28, hPt: 28 });
      out.push([POOL, it.poolId, { poolId: it.poolId, orderId: it.rid, transactionId: it.tid, lineKey: `${it.rid}_${it.tid}`, sku: it.sku, material: sh.metal, copy: 1, quantity: 1, runId: RUN, state: 'written', sheetId: sh.id, setId: sid, sheetName: fileBase, createdAt: now - 3600e3, updatedAt: now - 600e3 }]);
    });
    out.push([SHEETS, sh.id, { id: sh.id, setId: sid, setSeq: seq, sheetIndex: idx, runId: RUN, metal: sh.metal, day: TODAY, fileBase, folder: fileBase, status: 'complete', placedCount: placements.length, charmCount: placements.length, density: .5, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 }, placements, charms, poolIds, orders, verification: { ok: true }, outputs: {}, label: { files: [], orders }, createdAt: now - 3600e3, updatedAt: now - 600e3 }]);
    if (!sets.has(sid)) sets.set(sid, { setId: sid, seq, day: TODAY, runId: RUN, sheetIds: [], materials: [], orders: {}, labelFiles: [], status: 'labelled' });
    const s = sets.get(sid); s.sheetIds.push(sh.id); if (!s.materials.includes(sh.metal)) s.materials.push(sh.metal);
  }
  for (const s of sets.values()) out.push([SETS, s.setId, s]);
  out.push([RUNS, RUN, { runId: RUN, status: 'complete', step: 'done', day: TODAY, lines: {}, orders: [], sheets: {}, holds: {}, errors: [] }]);
  return out;
}
module.exports = { buildShop, cloudDocs, SKUS, N_SHEETS, N_ORDERS, N_REVIEW, N_ENGRAVE };
