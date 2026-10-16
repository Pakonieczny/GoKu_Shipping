/*  netlify/functions/_charmNestSandboxOrders.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The sandbox's ONE rule for which pulled Etsy receipts play (SBX1000, Paul, 16 Oct 2026: "For the purpose of the sandbox test you can
 *  consider shipped orders as open. I need to be able to pull/test up-to 1000 of the most recent orders from etsy.").
 *
 *    plays(r)      a receipt that is PAID and NOT CANCELLED. Shipped ones play too: the sandbox rehearses a day on the shop's newest orders, and most
 *                  of the newest orders of a busy shop have already shipped, so "open only" left 394 of 500. Cancelled and unpaid ones stay out.
 *    asOpen(r)     a playing receipt as the sandbox SERVES it. A shipped receipt carries is_shipped / was_shipped true, status "Completed" and
 *                  shipments (tracking), and the Design Station closes such an order (orders.detail answers gone, a shipped read is settled as
 *                  gone), so it is served as a normal open order instead: not shipped, status "Paid", no shipments or tracking, no shipped time
 *                  on its lines. Its receipt number, buyer, transactions and dates are kept, and it carries sandbox_shipped_on_etsy: true so a
 *                  reader can tell. Any other receipt is returned as it is (the same object). The STORED pull is never changed: it stays exactly
 *                  as Etsy returned it, and etsySandbox.js applies this when it loads the stored set (the one place every list, read-alone and
 *                  stream goes through).
 *    closedWhy(r)  why a receipt does not play, for the pull's counts: "canceled" or "unpaid" (null when it plays).
 *
 *  Used by etsySandbox.js (what is served and streamed), _charmNestSandboxPull.js (the counts stored with a pull) and charmNestLibrary.js
 *  (how many orders the stream will bring). Pure: no Firestore, no Storage, no network.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";

const OPEN_STATUS = "Paid";   // the status the open receipts of the shop carry (a shipped one reads "Completed")
const TRACKING = ["tracking_code", "tracking_number", "tracking_url", "carrier_name", "shipping_carrier", "shipment_notification_timestamp", "shipped_timestamp"];

const cancelled = r => !!(r.is_canceled || r.was_canceled || /cancel/i.test(r.status || ""));
const unpaid = r => r.is_paid === false || r.was_paid === false;
/** Paid and not cancelled: the receipts the sandbox plays, shipped or not. */
const plays = r => !!r && !unpaid(r) && !cancelled(r);
/** Etsy says the receipt shipped (flags, or its "Completed" status). */
const wasShipped = r => !!r && !!(r.is_shipped || r.was_shipped || /^\s*completed\s*$/i.test(r.status || ""));
/** Why a receipt is left out of the sandbox, for the pull's count only; null for one that plays. */
const closedWhy = r => (!r || cancelled(r) ? "canceled" : unpaid(r) ? "unpaid" : null);

/** A playing receipt as the sandbox serves it: a shipped one becomes a normal open order (a copy: the receipt in the stored set is untouched). */
function asOpen(r) {
  if (!plays(r) || !wasShipped(r)) return r;
  const o = Object.assign({}, r);
  o.is_shipped = false; if ("was_shipped" in o) o.was_shipped = false;
  o.status = OPEN_STATUS;
  if ("shipments" in o) o.shipments = [];
  for (const f of TRACKING) if (f in o) o[f] = null;
  if (Array.isArray(o.transactions)) o.transactions = o.transactions.map(t => (t && typeof t === "object" && (t.shipped_timestamp != null || t.is_shipped) ? Object.assign({}, t, { shipped_timestamp: null }, "is_shipped" in t ? { is_shipped: false } : {}) : t));
  o.sandbox_shipped_on_etsy = true;
  return o;
}

module.exports = { plays, wasShipped, closedWhy, asOpen, OPEN_STATUS };
