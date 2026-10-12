// The fixture of order-one-row.cjs (and of the screenshots of the order window's "Its pieces" list): fake orders, fake names, an in-memory store.
// Nothing live, no Etsy, no paid call. One order for each shape the list has to draw:
//   SINGLE  one charm on a sheet                     MATCH  a matching earring pair (both ears on one sheet)
//   MISMATCH a mismatched pair, the Right waiting    SHOT   the screenshot case: a necklace on a draft + a pair in Review (Left+Right, Print QR label / Complete Order)
//   DISC3   a 3-disc necklace (Disc 3 waiting)       SET4   a set of four charms (all on one sheet)
//   HELD    a matching pair on hold                          DONE   a pair completed by hand (its own seal)
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000);
const GF1 = 'sheet-or-gf1', GF2 = 'sheet-or-gf2', GFD = 'sheet-or-gfd';
const O = {
  SINGLE: { rid: '4180000001', tx: ['41800000011'] },
  MATCH: { rid: '4180000002', tx: ['41800000021'] },
  MISMATCH: { rid: '4180000003', tx: ['41800000031'] },
  SHOT: { rid: '4180000004', tx: ['41800000041', '41800000042'] },
  DISC3: { rid: '4180000005', tx: ['41800000051'] },
  SET4: { rid: '4180000006', tx: ['41800000061'] },
  HELD: { rid: '4180000007', tx: ['41800000071'] },
  DONE: { rid: '4180000008', tx: ['41800000081'] }
};
const kOf = (o, i = 0) => `${o.rid}_${o.tx[i]}`;
const pid = (o, i, copy) => `${o.rid}_${o.tx[i]}_${copy}`;
const line = (tid, sku, title, vars, extra) => Object.assign({ transactionId: tid, listingId: '19000' + tid.slice(-5), sku, title, quantity: 1, expectedShipDate: SHIP, variations: vars || [], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' }, extra || {});
const GF = { name: 'Metal', value: '14k Gold Filled' }, EAR = { name: 'Style', value: 'Earrings' };
const order = (o, buyer, lines) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

const LINES = () => [
  [order(O.SINGLE, 'Fixture Buyer A', [line(O.SINGLE.tx[0], 'STAR_11001', 'Star charm necklace', [GF])]), [[pid(O.SINGLE, 0, 1)]]],
  [order(O.MATCH, 'Fixture Buyer B', [line(O.MATCH.tx[0], 'HEART_STUD', 'Heart stud earrings', [GF, EAR])]), [[pid(O.MATCH, 0, 1), pid(O.MATCH, 0, 2)]]],
  [order(O.MISMATCH, 'Fixture Buyer C', [line(O.MISMATCH.tx[0], 'MISMATCHED_7134', 'Mismatched mittens earrings', [GF, EAR])]), [[pid(O.MISMATCH, 0, 1), pid(O.MISMATCH, 0, 2)]]],
  [order(O.SHOT, 'Fixture Buyer D', [line(O.SHOT.tx[0], 'COMEDY_89578', 'Comedy mask necklace', [GF]), line(O.SHOT.tx[1], 'MASKS HAPPY/SAD', 'Happy and sad masks earrings', [GF, EAR])]), [[pid(O.SHOT, 0, 1)], []]],
  [order(O.DISC3, 'Fixture Buyer E', [line(O.DISC3.tx[0], 'INITIAL_8391', 'Initial disc necklace 3 discs', [{ name: 'Necklace options', value: 'GOLD - 3 Disc' }], { personalization: ['Tag 1: Mom, Tag 2: Dad, Tag 3: Sis'] })]), [[pid(O.DISC3, 0, 1), pid(O.DISC3, 0, 2), pid(O.DISC3, 0, 3)]]],
  [order(O.SET4, 'Fixture Buyer F', [line(O.SET4.tx[0], 'FOUR_SET_4400', 'Charm set of four', [GF, { name: 'Number of charms', value: '4 Charms' }])]), [[pid(O.SET4, 0, 1), pid(O.SET4, 0, 2), pid(O.SET4, 0, 3), pid(O.SET4, 0, 4)]]],
  [order(O.HELD, 'Fixture Buyer G', [line(O.HELD.tx[0], 'MOON_20202', 'Moon stud earrings', [GF, EAR])]), [[]]],
  [order(O.DONE, 'Fixture Buyer H', [line(O.DONE.tx[0], 'MASKS DONE/PAIR', 'Masks earrings', [GF, EAR])]), [[]]]
];

/** Fill the in-memory store (srv.st): the sheets, the pool rows, the orders' timelines and the custom record of the completed pair. */
function seed(srv) {
  const st = srv.st, now = Date.now();
  const ids = [];
  const box = (id, i) => ({ id, cxPt: 30 + i * 34, cyPt: 30, angle: 0, wPt: 30, hPt: 30 });
  const sheet = (id, extra, pools) => {
    const placements = [], charms = [];
    pools.forEach((p, i) => { const cid = 'c' + (ids.push(p)); placements.push(box(cid, i)); charms.push({ id: cid, name: p, poolId: p, order: p.split('_')[0], sku: 'X' }); });
    st.put('Charm_Nest_Sheets', id, Object.assign({ id, metal: 'gold', day: '2026-10-12', status: 'written', stock: { wPt: 360, hPt: 160 }, orders: [...new Set(pools.map(p => p.split('_')[0]))], placements, charms, updatedAt: now }, extra));
  };
  const SET = 'set-or-1';
  const one = [pid(O.SINGLE, 0, 1), pid(O.MATCH, 0, 1), pid(O.MATCH, 0, 2), pid(O.MISMATCH, 0, 1), pid(O.DISC3, 0, 1), pid(O.DISC3, 0, 2)];
  const two = [pid(O.SET4, 0, 1), pid(O.SET4, 0, 2), pid(O.SET4, 0, 3), pid(O.SET4, 0, 4)];
  sheet(GF1, { sheetIndex: 1, setId: SET, setSeq: 1, folder: '2026-10-12_GF_Set-1_Sheet-1', fileBase: '2026-10-12_GF_Set-1_Sheet-1' }, one);
  sheet(GF2, { sheetIndex: 2, setId: SET, setSeq: 1, folder: '2026-10-12_GF_Set-1_Sheet-2', fileBase: '2026-10-12_GF_Set-1_Sheet-2' }, two);
  sheet(GFD, { draft: true, page: 1, status: 'nested' }, [pid(O.SHOT, 0, 1)]);
  const side = (p, s, g) => (s ? { side: s, bodyIndex: s === 'L' ? 0 : 1, groupKey: g, groupSize: 2, mirror: s === 'R' } : {});
  const prow = (o, i, copy, sku, sheetId, s, gsz) => st.put('Charm_Pool', pid(o, i, copy), Object.assign({ poolId: pid(o, i, copy), orderId: o.rid, transactionId: o.tx[i], lineKey: kOf(o, i), sku, material: 'gold', copy, quantity: 1, state: sheetId ? 'written' : 'ready', sheetId: sheetId || null, setId: sheetId && sheetId !== GFD ? SET : null, updatedAt: now },
    s ? side(null, s, `${o.rid}:${o.tx[i]}`) : {}, gsz ? { groupKey: `${o.rid}:${o.tx[i]}`, groupSize: gsz, bodyIndex: copy - 1 } : {}));
  prow(O.SINGLE, 0, 1, 'STAR_11001', GF1);
  prow(O.MATCH, 0, 1, 'HEART_STUD', GF1, 'L'); prow(O.MATCH, 0, 2, 'HEART_STUD', GF1, 'R');
  prow(O.MISMATCH, 0, 1, 'MISMATCHED_7134', GF1, 'L'); prow(O.MISMATCH, 0, 2, 'MISMATCHED_7134', null, 'R');
  prow(O.SHOT, 0, 1, 'COMEDY_89578', GFD);
  prow(O.DISC3, 0, 1, 'INITIAL_8391', GF1, null, 3); prow(O.DISC3, 0, 2, 'INITIAL_8391', GF1, null, 3); prow(O.DISC3, 0, 3, 'INITIAL_8391', null, null, 3);
  for (let c = 1; c <= 4; c++) prow(O.SET4, 0, c, 'FOUR_SET_4400', GF2, null, 4);
  prow(O.HELD, 0, 1, 'MOON_20202', null, 'L'); prow(O.HELD, 0, 2, 'MOON_20202', null, 'R');   // (taken off its sheet by the hold: its pieces wait)
  // the orders' timelines: arrived, each piece on a sheet placed
  let n = 0;
  const ev = (o, type, ago, extra) => st.put('Order_Timeline', `${o.rid}~${type}~e${++n}`, Object.assign({ orderId: o.rid, type, at: now - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  for (const o of Object.values(O)) ev(o, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  const placed = (o, i, copy, label, sheetId, ago) => ev(o, 'placed', ago, { sheet: label, sheetId, lineKey: kOf(o, i), data: { poolId: pid(o, i, copy) } });
  placed(O.SINGLE, 0, 1, 'GF Sheet 1', GF1, 3000);
  placed(O.MATCH, 0, 1, 'GF Sheet 1', GF1, 3000); placed(O.MATCH, 0, 2, 'GF Sheet 1', GF1, 2990);
  placed(O.MISMATCH, 0, 1, 'GF Sheet 1', GF1, 3000);
  placed(O.SHOT, 0, 1, 'GF Draft 1', GFD, 3000);
  placed(O.DISC3, 0, 1, 'GF Sheet 1', GF1, 3000); placed(O.DISC3, 0, 2, 'GF Sheet 1', GF1, 2990);
  for (let c = 1; c <= 4; c++) placed(O.SET4, 0, c, 'GF Sheet 2', GF2, 3000 - c);
  // the completed pair: completed by hand (Complete Order), with the seal of that press
  const at = now - 90 * 60000;
  st.put('Charm_Custom_Orders', kOf(O.DONE), { key: kOf(O.DONE), receiptId: O.DONE.rid, transactionId: O.DONE.tx[0], sku: 'MASKS DONE/PAIR', title: 'Masks earrings', category: 'Unknown SKU', kind: '', state: 'completed', how: 'button', completedAt: at, completedBy: 'Test Operator', stamps: [{ how: 'button', at, by: 'Test Operator' }], updatedAtMs: now });
  ev(O.DONE, 'sealCompleted', 90, { id: `${kOf(O.DONE)}-button-${at}`, lineKey: kOf(O.DONE), transactionId: O.DONE.tx[0], text: 'Custom order completed', data: { how: 'button', prints: 0 } });
}

/** In the page: take the orders in as the intake would have, open the Orders tab. (`hold`: the held order's text, as the Hold button writes it.) */
async function load(page) {
  await page.evaluate(async ({ orders, held }) => {
    const orig = Master.entryFor.bind(Master);
    Master.entryFor = sku => String(sku) === 'MISMATCHED_7134' ? { sku, pair: { v: 1, bodies: 2, mismatched: true }, updatedAt: 1 } : orig(sku);
    await Orders.loadMaps(true);
    for (const sku of ['STAR_11001', 'HEART_STUD', 'COMEDY_89578', 'INITIAL_8391', 'FOUR_SET_4400', 'MOON_20202']) B.master.entries.set(sku, { sku, updatedAt: 1 });   // (designs in a master file: their pieces are placed on sheets, in no Review card)
    for (const [order, pools] of orders) order.lines.forEach((ln, i) => {
      const key = CharmNestOrders.lineKey(order, ln), isNew = !B.orders.byKey.has(key);
      const row = { key, order, line: ln, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] && pools[i].length ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] || [], engrave: null, material: null };
      if (isNew) { B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    });
    Orders.interpretAll();
    for (const r of B.orders.rows) if (String(r.order.receiptId) === held) { r.hold = 'Put on hold by Test Operator: waiting for a new chain'; r.state = 'held'; }
    Review.syncOrderItems && Review.syncOrderItems();
    CN.setMode('orders'); Orders.render();
  }, { orders: LINES(), held: O.HELD.rid });
}

module.exports = { O, GF1, GF2, GFD, SHIP, kOf, pid, LINES, seed, load };
