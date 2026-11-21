/*  netlify/functions/_charmNestArchiveRecord.js — the Design Station's archive record of a completed order, built in the cloud (cloud plan v2 build step 12,
 *  hard-parts §2.3 item 2, E6). A port of design-1.html §19c (dayKey, pickShip, str, numOr0, money, buildArchiveRecord, ~6552-6665) line for line, with
 *  three differences, each because the cloud has no station page:
 *    · the receipt is NEVER read from Etsy (the station's archiveOrders asks pullEtsyOrderDetails, design-1.html:6684): it comes from the mail system's
 *      copy (EtsyMail_Receipts/<rid>.raw, whole), else the world's own copy (Charm_World_Etsy/<rid>.raw), else the run's line records (the station's own
 *      fall-back when Etsy did not answer, design-1.html:6686-6689). dataFrom() picks; prepareReceipt (charm-nest-etsy-shape.js, the station's shaping,
 *      tested against it) lifts the personalisation and resolves each item's metal exactly as pullEtsyOrderDetails does.
 *    · an item's picture is imageOf(listingId), read by the caller from the stored image cache only (listingPhotos, cache-only: no Etsy call, no image
 *      fetch); the station read its in-memory __imagesCache. The address is kept as it is (designArchive is called with noMirror: nothing is fetched).
 *    · the time is the commit's own (its intent's createdAt), so a replay builds the very same record; the day is Toronto's (the station's own clock).
 *  Pure: no Firestore, no fetch, no DOM. Nothing here calls Etsy, its image CDN or its switch.                                                             */
"use strict";
let Shape = null;
try { Shape = require("../../charm-nest-etsy-shape.js"); } catch (_) { Shape = null; }

const TZ = "America/Toronto";
/** design-1.html dayKey, on the shop's clock (the station runs in Toronto; Netlify runs in UTC). */
const dayKey = ms => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(ms)); } catch (_) { const d = new Date(ms); return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`; } };
const str = v => (v == null ? "" : String(v));
const numOr0 = v => (Number.isFinite(Number(v)) ? Number(v) : 0);
const money = m => (m && typeof m === "object" ? numOr0(m.amount) / (numOr0(m.divisor) || 100) : numOr0(m));

function pickShip(rc) {
  return {
    name        : str(rc.name),
    first_line  : str(rc.first_line),
    second_line : str(rc.second_line),
    city        : str(rc.city),
    state       : str(rc.state),
    zip         : str(rc.zip),
    country     : str(rc.country_iso || rc.country),
    formatted   : str(rc.formatted_address),
  };
}
const metalOf = t => { if (!Shape) return ""; const k = Shape.txMetalResolved(t); return Shape.isRealMetal(k) ? k : ""; };

/**
 * design-1.html buildArchiveRecord: an Etsy receipt { receipt, transactions } → the archive record. `raw` keeps the payload whole.
 * o: { completedBy, now (ms: the commit's own time), imageOf(listingId) → address or "" }.
 */
function buildArchiveRecord(rid, data, o = {}) {
  const rc  = (data && data.receipt) || {};
  const txs = (data && data.transactions) || [];
  const completedAtMs = numOr0(o.now) || Date.now();
  const imageOf = typeof o.imageOf === "function" ? o.imageOf : () => "";

  const items = txs.map(t => {
    const variations = (t.variations || []).map(v => ({
      name : str(v && v.formatted_name), value: str(v && v.formatted_value).replace(/&quot;/g, '"'),
    }));
    const pers = Array.isArray(t.personalization) ? t.personalization.join("\n")
               : str(t.personalization);
    return {
      transactionId : str(t.transaction_id),
      listingId     : str(t.listing_id),
      productId     : str(t.product_id),
      title         : str(t.title),
      sku           : str(t.sku),
      quantity      : numOr0(t.quantity) || 1,
      price         : money(t.price),
      shippingCost  : money(t.shipping_cost),
      metal         : metalOf(t),
      personalization: pers,
      variations,
      expectedShipDate: numOr0(t.expected_ship_date),
      imageUrl      : str(imageOf(str(t.listing_id)) || ""),
      listingImageId: str(t.listing_image_id),
      isDigital     : !!t.is_digital,
    };
  });

  const metals = Shape ? Shape.metalCountsFor(txs) : {};
  const rawReceipt = { ...rc };
  delete rawReceipt.transactions;

  return {
    receiptId    : String(rid),
    orderNumber  : str(rc.order_number || rid),
    completedAtMs,
    completedDay : dayKey(completedAtMs),
    completedBy  : o.completedBy || "Design",
    buyer: {
      name  : str(rc.name),
      userId: str(rc.buyer_user_id),
      email : str(rc.buyer_email),
    },
    ship: pickShip(rc),
    messages: {
      fromBuyer  : str(rc.message_from_buyer),
      fromSeller : str(rc.message_from_seller),
      fromPayment: str(rc.message_from_payment),
      isGift     : !!rc.is_gift,
      giftMessage: str(rc.gift_message),
      giftSender : str(rc.gift_sender),
    },
    money: {
      currency : str((rc.total_price && rc.total_price.currency_code) || (rc.grandtotal && rc.grandtotal.currency_code) || "USD"),
      subtotal : money(rc.subtotal),
      total    : money(rc.total_price),
      shipping : money(rc.total_shipping_cost),
      tax      : money(rc.total_tax_cost),
      vat      : money(rc.total_vat_cost),
      discount : money(rc.discount_amt),
      giftWrap : money(rc.gift_wrap_price),
      grand    : money(rc.grandtotal),
    },
    status: {
      etsyStatus: str(rc.status),
      isPaid    : !!rc.is_paid,
      isShipped : !!rc.is_shipped,
      createdTs : numOr0(rc.create_timestamp || rc.created_timestamp),
      updatedTs : numOr0(rc.update_timestamp || rc.updated_timestamp),
    },
    shipments: (rc.shipments || []).map(s => ({
      carrier          : str(s.carrier_name),
      trackingCode     : str(s.tracking_code),
      buyerNote        : str(s.buyer_note),
      notificationTs   : numOr0(s.shipment_notification_timestamp || s.notification_date),
      receiptShippingId: str(s.receipt_shipping_id),
    })),
    items,
    metals,
    totals: { items: items.length, quantity: items.reduce((n, i) => n + i.quantity, 0) },
    raw: { receipt: rawReceipt, transactions: txs },
  };
}

/** A whole receipt as Etsy's getShopReceipt answers it (_charmNestWorld.js wholeRaw): an id and its items. */
const wholeRaw = r => !!(r && r.receipt_id && Array.isArray(r.transactions) && r.transactions.length);
const DECOR = /^_(shipTS|shipStr|stamp|metalCounts|rowMetal|hydrated|attn)$/;
/** A whole copied receipt → { receipt, transactions } as pullEtsyOrderDetails answers (personalisation lifted, metal resolved; the station's list
    decorations taken off the receipt again, so `raw` holds what Etsy said). Null when it cannot be shaped. */
function detailOf(raw, rid) {
  if (!wholeRaw(raw)) return null;
  if (!Shape) { const rc = JSON.parse(JSON.stringify(raw)), txs = rc.transactions || []; delete rc.transactions; return { receipt: rc, transactions: txs }; }
  let rc; try { rc = Shape.prepareReceipt(raw, { via: "detail", receiptId: rid }); } catch (_) { return null; }
  const txs = Array.isArray(rc.transactions) ? rc.transactions : [];
  const receipt = {}; for (const k of Object.keys(rc)) if (k !== "transactions" && !DECOR.test(k)) receipt[k] = rc[k];
  return { receipt, transactions: txs };
}
/** The station's own fall-back (design-1.html:6686-6689) from the run's line records of the order: what the run kept of each line (its snap). */
function fromLines(rid, lines) {
  const ls = (lines || []).filter(l => l && typeof l === "object");
  if (!ls.length) return null;
  const snap = l => (l.snap && typeof l.snap === "object" ? l.snap : {});
  const first = snap(ls[0]);
  const transactions = ls.map(l => {
    const s = snap(l), vars = (s.vars || []).map(v => { const [name, value] = String(v).split("␟"); return { formatted_name: name || "", formatted_value: value || "" }; });
    return { transaction_id: str(l.transactionId), listing_id: str(s.listingId), title: str(s.title), sku: str(s.bought || l.sku), quantity: numOr0(l.quantity) || 1, variations: vars, personalization: (s.pers || []).map(str) };
  });
  return { receipt: { receipt_id: String(rid), order_number: str(first.orderNumber || rid), name: str(first.buyer), is_gift: !!first.isGift }, transactions, fromLines: true };
}
/** Which copy the record is built from: the mail system's (whole), the world's (whole), else the run's lines. → { data, via } or null */
function dataFrom(rid, { mirror, world, lines } = {}) {
  const m = mirror && mirror.raw ? detailOf(mirror.raw, rid) : null; if (m) return { data: m, via: "mirror" };
  const w = world && world.raw ? detailOf(world.raw, rid) : null; if (w) return { data: w, via: "world" };
  const l = fromLines(rid, lines); return l ? { data: l, via: "lines" } : null;
}
/** design-1.html:6694-6697: a record with no items, no buyer and no address means the read failed outright; it is not written (an empty record would look
    "already archived" and never be retried). */
const usable = rec => !!(rec && ((rec.items || []).length || (rec.ship && rec.ship.name) || (rec.buyer && rec.buyer.name) || (rec.money && rec.money.total)));

module.exports = { buildArchiveRecord, pickShip, dayKey, money, numOr0, str, detailOf, fromLines, dataFrom, usable, wholeRaw };
