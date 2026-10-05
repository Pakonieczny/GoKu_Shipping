/* A shop shaped like Paul's screenshots (image3 / image4 / image8, 5 Oct 2026): Set-1 holds GF Sheet 1 (67 orders) and SS Sheet 1
 * (48 orders, "Waiting on 18 back engravings"); five multi-piece orders are spread over the two sheets (one piece of each on
 * each sheet), and the run still says state 'pooled' for the pieces that are already on SS Sheet 1.
 *
 * What a person sees as true: GF Sheet 1 has every back saved and its QR label, so its only real "order check" trouble is the
 * five orders whose other piece sits on SS Sheet 1, which is not ready (18 engravings still to approve). SS Sheet 1 has
 * nothing wrong with its orders (GF Sheet 1 is ready): its own 18 engravings are shown by its own step, never as orders. */
'use strict';
const { materialize } = require('./issues-shop.cjs');

function paulSpec({ gf = 62, ss = 43, shared = 5, unapproved = 18 } = {}) {
  const sheets = [
    { id: 'gf-sheet-1', metal: 'gold', index: 1, own: 'ok', setId: 'set-1' },
    { id: 'ss-sheet-1', metal: 'silver', index: 1, own: 'ok', setId: 'set-1' }
  ];
  const orders = [];
  let tx = 5000, oid = 4170250000;
  const base = { problems: [], sku: 'SKU', hold: null, change: false, noDesign: false, kind: 'paul' };
  const one = (metal, sheet, engrave, state) => ({ ...base, tx: ++tx, metal, q: 1, copies: [{ sheet, pooled: true }], state, engrave });
  for (let i = 0; i < gf; i++) orders.push({ id: String(++oid), buyer: 'GF buyer ' + i, lines: [one('gold', 'gf-sheet-1', i % 3 ? 'plain' : 'approved', 'written')] });
  for (let i = 0; i < ss; i++) orders.push({ id: String(++oid), buyer: 'SS buyer ' + i, lines: [one('silver', 'ss-sheet-1', i < unapproved ? 'unapproved' : i % 2 ? 'plain' : 'approved', 'written')] });
  for (let i = 0; i < shared; i++) orders.push({ id: String(++oid), buyer: 'Shared buyer ' + i, lines: [one('gold', 'gf-sheet-1', 'approved', 'written'), one('silver', 'ss-sheet-1', 'approved', 'pooled')] });
  return { seed: 7, sandbox: false, sheets, orders, archivedSheets: [] };
}
const paulShop = o => materialize(paulSpec(o));
module.exports = { paulSpec, paulShop };
