/* A shop shaped like Paul's screenshots (image3 / image4 / image8, 5 Oct 2026): Set-1 holds GF Sheet 1 (67 orders) and SS Sheet 1
 * (48 orders, "Waiting on 18 back engravings"); five multi-piece orders are spread over the two sheets (one piece of each on
 * each sheet), and the run still says state 'pooled' for the pieces that are already on SS Sheet 1.
 *
 * What a person sees as true (round 7, Paul: "it's on both sheets and both sheets are in the same set"): NEITHER sheet has an order
 * issue. The five orders whose other piece sits on SS Sheet 1 are fine: a set advances as ONE, and SS Sheet 1 not being ready (18
 * engravings still to approve) is the SET's wait, said once where the Approve buttons are (R7-1's setGate), never an issue of those
 * orders. (Before round 7 they were listed as "Waits on SS Sheet 1" on GF Sheet 1's "!" panel.) SS Sheet 1's own 18 engravings are
 * shown by its own step, never as orders. */
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
