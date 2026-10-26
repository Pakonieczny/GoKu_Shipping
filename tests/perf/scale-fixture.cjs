// SCALE FIXTURE (Paul, 26 Oct 2026: "I don't see anything for how fast things actually happen and the performance of the overall platform once it's being used at scale with tens of thousands
// of orders in live and historical"). A generator, not a checked-in blob: the open orders of a live run (the mix the sandbox snapshots show: multi-piece orders, earrings, engraving, holds, gift
// notes) and the history behind them (sets of sheets the laser has cut, cancelled orders), seeded into tests/charm-nest/bridge-server.cjs's fake Firestore.
//   const F = require('./scale-fixture.cjs'); const orders = F.openOrders(3000); F.seedHistory(srv.st, { orders: 30000 });
'use strict';
const DAY = 86400, NOW = Math.floor(Date.UTC(2026, 9, 26, 15) / 1000);
const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;
const METALS = [['gold', 'GF 14/20', '14k Gold Filled'], ['silver', 'SS', 'Sterling Silver'], ['rose', 'RG 14/20', 'Rose Gold Filled']];
const KINDS = ['Initial Charm Necklace', 'Zodiac Gem Necklace', 'Letter Stud Earrings', 'Birth Flower Charm', 'Name Bar Necklace', 'Heart Locket', 'Paw Print Charm', 'Moon Phase Charm'];
const NAMES = ['Ava', 'Mia', 'Leo', 'Noah', 'Ella', 'Zoe', 'Liam', 'Ivy', 'Owen', 'Ruby', 'Max', 'Nora'];

/** n open orders of a live run: 1–4 lines (a third multi-piece), a fifth earrings (quantity 2), a quarter engraved, one in 25 held, ship-by over the next 10 days. */
function openOrders(n, seed = 7) {
  const r = rng(seed), out = [];
  for (let i = 0; i < n; i++) {
    const rid = String(4300000000 + i), created = NOW - Math.floor(r() * 9 * DAY), lines = [];
    const nLines = r() < 0.66 ? 1 : 1 + Math.ceil(r() * 3);
    for (let k = 0; k < nLines; k++) {
      const [metalKey, metalLabel, value] = METALS[Math.floor(r() * (r() < 0.85 ? 2 : 3))], kind = Math.floor(r() * KINDS.length), earring = /Earrings/.test(KINDS[kind]), engraved = r() < 0.25;
      lines.push({ transactionId: rid + k, listingId: String(1100000000 + kind * 1000 + Math.floor(r() * 40)), sku: 'SCALE_' + kind + '-' + Math.floor(r() * 26), title: KINDS[kind], quantity: earring ? 2 : 1 + (r() < 0.08 ? 1 : 0),
        expectedShipDate: created + 10 * DAY, metalKey, metalLabel, buyerMessage: '',
        variations: [{ name: 'Metal', value }, { name: 'Letter', value: String.fromCharCode(65 + Math.floor(r() * 26)) }],
        personalization: engraved ? [{ name: 'Personalization', value: NAMES[Math.floor(r() * NAMES.length)] + ' ' + (1990 + Math.floor(r() * 35)) }] : [] });
    }
    const held = r() < 0.04;
    out.push({ receiptId: rid, orderNumber: rid, createTs: created, updateTs: created + 60, shipBy: created + 10 * DAY, buyer: { name: 'Buyer ' + i }, buyerMessage: held ? 'Please change the letter to B' : '',
      isGift: r() < 0.1, giftMessage: '', staffNote: held ? 'hold: buyer asked for a change' : '', messages: [], lines });
  }
  return out;
}

/** The history of `orders` orders: sets of 2–4 sheets (12 orders a sheet), every one cut by the laser but the newest `current` sets, with timeline events and one cancelled order in 60. */
function seedHistory(st, { orders = 30000, current = 200, seed = 11 } = {}) {
  const r = rng(seed), PER = 12, sheets = Math.ceil(orders / PER), counts = { sets: 0, sheets: 0, cancelled: 0, events: 0 };
  let s = 0, set = 0, rid = 4100000000;
  const totalSets = Math.ceil(sheets / 3);
  while (s < sheets) {
    const per = 2 + Math.floor(r() * 3), age = Math.floor((totalSets - set) / 40), day = new Date((NOW - age * DAY) * 1000).toISOString().slice(0, 10), done = totalSets - set > current;
    const setId = 'set-' + day + '-' + (set % 40 + 1), ids = [], materials = new Set();
    for (let k = 0; k < per && s < sheets; k++, s++) {
      const [metal, metalLabel] = METALS[k % 2 === 0 ? 0 : 1], id = 'sheet-' + s, ordersOn = Array.from({ length: PER }, () => String(rid++)), pool = ordersOn.map((o, n) => o + '_' + o + n + '_1');
      ids.push(id); materials.add(metal);
      st.put('Charm_Nest_Sheets', id, { id, sheetId: id, metal, metalLabel, setId, setSeq: set % 40 + 1, runId: 'run-' + day, sheetIndex: k + 1, status: 'complete', poolIds: pool, placedCount: PER, charmCount: PER,
        density: 0.6 + r() * 0.3, freePt2: 1200, verification: { ok: true }, orders: ordersOn, day, updatedAt: (NOW - age * DAY) * 1000, createdAt: (NOW - age * DAY) * 1000, folder: `GF_${day}_Set-${set % 40 + 1}_Sheet-${k + 1}`,
        engraving: {}, orderReadiness: Object.fromEntries(ordersOn.map(o => [o, { ready: true }])), ...(done ? { laserDoneAt: (NOW - age * DAY + 3600) * 1000, laserDoneBy: 'Paul K', current: false } : { current: true }) }, false);
      counts.sheets++;
      for (const o of ordersOn) {
        if (r() < 1 / 60) { st.put('Charm_Nest_Cancelled', o, { receiptId: o, cancelledAt: (NOW - age * DAY) * 1000, by: 'Paul K', why: 'buyer cancelled', lines: [{ title: 'Charm', sku: 'SCALE_1-1' }], day }, false); counts.cancelled++; }
      }
      counts.events += PER * 4;   // (pulled, nested, sheet saved, laser done: what the order timeline holds for each; counted, the timeline reads them per order on open)
    }
    st.put('Charm_Nest_Sets', setId + '-' + set, { setId: setId + '-' + set, seq: set % 40 + 1, day, runId: 'run-' + day, sheetIds: ids, materials: [...materials], status: done ? 'done' : 'labelled', updatedAt: (NOW - age * DAY) * 1000,
      ...(done ? { committedAt: (NOW - age * DAY) * 1000, laserDoneAt: (NOW - age * DAY + 3600) * 1000 } : { committedAt: NOW * 1000 }) }, false);
    counts.sets++; set++;
  }
  return counts;
}

module.exports = { openOrders, seedHistory, NOW, DAY };
