/*  netlify/functions/designArchive.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Permanent archive of every design-completed order.
 *
 *  WHY THIS EXISTS
 *  The Design Station used to record a completion as a single boolean in
 *  `Design_Completed Orders`. Once an order shipped it fell out of Etsy's open
 *  list and everything about it — who bought it, where it went, what was
 *  engraved on it, which metal, which SKU — became unreachable without an Etsy
 *  API call, and eventually not even then. This function keeps a full,
 *  searchable copy so the history screen never has to ask Etsy anything except
 *  live shipping status.
 *
 *  COLLECTION  Design_Order_Archive/{receiptId}
 *
 *  NO COMPOSITE INDEXES ARE REQUIRED. Every query below is a single-field
 *  equality or range, which Firestore indexes automatically, so this works the
 *  moment it is deployed with no console setup.
 *
 *  OPS
 *    POST {op:"put", orders:[record,…]}   upsert up to 50, mirror item images
 *    GET  ?op=index&since=<ms>&limit=&cursor=   slim rows for client sync
 *    GET  ?op=day&date=YYYY-MM-DD               full records completed that day
 *    GET  ?op=days&from=YYYY-MM-DD&to=          per-day completion counts
 *    GET  ?op=get&id=<receiptId>                one full record
 *    GET  ?op=stats                             total / oldest / newest
 *  ═══════════════════════════════════════════════════════════════════════ */

const admin = require("./firebaseAdmin");
const db    = admin.firestore();

const COLL       = "Design_Order_Archive";
const MAX_PUT    = 50;
const MAX_INDEX  = 500;
const MAX_MIRROR = 24;          // images mirrored per request, keeps us inside the timeout

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
};
const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });

/* ── helpers ───────────────────────────────────────────────────────────── */

const str  = v => (v == null ? "" : String(v));
const num  = v => (Number.isFinite(Number(v)) ? Number(v) : 0);
const trim = (v, n) => str(v).slice(0, n);

/** Everything a human might type into the search box, lowercased into one blob. */
function buildSearchBlob(r) {
  const parts = [
    r.receiptId, r.orderNumber, r.completedDay, r.completedBy,
    r.buyer?.name, r.buyer?.email,
    r.ship?.name, r.ship?.first_line, r.ship?.second_line,
    r.ship?.city, r.ship?.state, r.ship?.zip, r.ship?.country,
    r.messages?.fromBuyer, r.messages?.giftMessage, r.messages?.giftSender, r.messages?.fromSeller,
    ...(r.items || []).flatMap(i => [
      i.title, i.sku, i.personalization, i.metal,
      ...(i.variations || []).map(v => `${v.name} ${v.value}`),
    ]),
    ...(r.shipments || []).flatMap(s => [s.carrier, s.trackingCode]),
    ...Object.entries(r.metals || {}).filter(([, n]) => n).map(([k]) => k),
  ];
  return parts.filter(Boolean).join(" ").toLowerCase().replace(/\s+/g, " ").slice(0, 12000);
}

/** The slim projection the client keeps in IndexedDB for instant searching. */
function slimRow(d) {
  return {
    receiptId  : d.receiptId,
    orderNumber: d.orderNumber,
    day        : d.completedDay,
    at         : d.completedAt?.toMillis ? d.completedAt.toMillis() : num(d.completedAtMs),
    buyer      : d.buyer?.name || d.ship?.name || "",
    city       : d.ship?.city || "",
    state      : d.ship?.state || "",
    country    : d.ship?.country || "",
    items      : num(d.totals?.items),
    qty        : num(d.totals?.quantity),
    metals     : d.metals || {},
    thumb      : d.items?.[0]?.mirrorUrl || d.items?.[0]?.imageUrl || "",
    shipped    : !!d.status?.isShipped,
    tracked    : (d.shipments || []).length > 0,
    search     : d.search || "",
  };
}

/* ── image mirroring ───────────────────────────────────────────────────── */

let bucket = null;
function getBucket() {
  if (bucket === null) {
    try { bucket = admin.storage().bucket(); }
    catch (e) { console.warn("Storage unavailable, archiving URLs only:", e.message); bucket = false; }
  }
  return bucket;
}

/**
 * Copy a listing image into Firebase Storage so the archive survives Etsy
 * rotating or removing the asset. Best effort — a failure never blocks the
 * write, the record just keeps the original URL.
 */
async function mirrorImage(receiptId, transactionId, url) {
  const b = getBucket();
  if (!b || !url || /^data:/i.test(url)) return "";
  try {
    const path = `design-archive/${receiptId}/${transactionId}.jpg`;
    const file = b.file(path);

    const [exists] = await file.exists();
    if (exists) return file.publicUrl();

    const resp = await fetch(url);
    if (!resp.ok) return "";
    const buf = Buffer.from(await resp.arrayBuffer());
    if (!buf.length || buf.length > 8 * 1024 * 1024) return "";

    await file.save(buf, {
      contentType: resp.headers.get("content-type") || "image/jpeg",
      resumable  : false,
      metadata   : { cacheControl: "public, max-age=31536000, immutable" },
    });
    try { await file.makePublic(); } catch (_) { /* uniform bucket-level access */ }
    return file.publicUrl();
  } catch (e) {
    console.warn("mirrorImage failed", receiptId, transactionId, e.message);
    return "";
  }
}

/* ── handler ───────────────────────────────────────────────────────────── */

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };

  try {
    /* ─────────────────────────── WRITE ─────────────────────────── */
    if (event.httpMethod === "POST") {
      const body = JSON.parse(event.body || "{}");
      const op = body.op || "put";

      if (op !== "put") return json(400, { error: `Unknown op "${op}"` });

      const orders = Array.isArray(body.orders) ? body.orders.slice(0, MAX_PUT) : [];
      if (!orders.length) return json(400, { error: "No orders supplied" });

      let mirrored = 0;
      const now = admin.firestore.FieldValue.serverTimestamp();
      const batch = db.batch();

      for (const o of orders) {
        const receiptId = str(o.receiptId || o.receipt_id).trim();
        if (!receiptId) continue;

        const items = Array.isArray(o.items) ? o.items : [];

        // Mirror the primary image of each item, up to the per-request budget.
        for (const it of items) {
          if (mirrored >= MAX_MIRROR) break;
          if (it.mirrorUrl) continue;
          if (!it.imageUrl) continue;
          const url = await mirrorImage(receiptId, str(it.transactionId), it.imageUrl);
          if (url) { it.mirrorUrl = url; mirrored++; }
        }

        const completedAtMs = num(o.completedAtMs) || Date.now();
        const record = {
          v            : 1,
          receiptId,
          orderNumber  : str(o.orderNumber),
          completedAt  : admin.firestore.Timestamp.fromMillis(completedAtMs),
          completedAtMs,
          completedDay : str(o.completedDay),
          completedBy  : trim(o.completedBy, 120),
          buyer        : o.buyer     || {},
          ship         : o.ship      || {},
          messages     : o.messages  || {},
          money        : o.money     || {},
          status       : o.status    || {},
          shipments    : Array.isArray(o.shipments) ? o.shipments : [],
          items,
          metals       : o.metals    || {},
          totals       : o.totals    || {},
          raw          : o.raw       || null,
          archivedAt   : now,
        };
        record.search = buildSearchBlob(record);

        batch.set(db.collection(COLL).doc(receiptId), record, { merge: true });
      }

      await batch.commit();
      return json(200, { success: true, saved: orders.length, mirrored });
    }

    /* ─────────────────────────── READ ─────────────────────────── */
    const q  = event.queryStringParameters || {};
    const op = q.op || "index";

    /* Slim rows for the client's local index, newest first, cursor-paged. */
    if (op === "index") {
      const since = num(q.since);
      const limit = Math.min(num(q.limit) || 300, MAX_INDEX);
      let ref = db.collection(COLL);
      if (since > 0) ref = ref.where("completedAtMs", ">=", since);
      ref = ref.orderBy("completedAtMs", "asc").limit(limit);
      if (q.cursor) ref = ref.startAfter(num(q.cursor));

      const snap = await ref.get();
      const rows = snap.docs.map(d => slimRow(d.data()));
      const last = rows.length ? rows[rows.length - 1].at : null;
      return json(200, {
        success: true,
        rows,
        nextCursor: rows.length === limit ? last : null,
        now: Date.now(),
      });
    }

    /* Everything completed on one calendar day (the day string is written by
       the client, so it always matches the operator's own timezone). */
    if (op === "day") {
      const date = str(q.date).trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json(400, { error: "date must be YYYY-MM-DD" });
      const snap = await db.collection(COLL).where("completedDay", "==", date).get();
      const rows = snap.docs.map(d => d.data()).sort((a, b) => num(b.completedAtMs) - num(a.completedAtMs));
      return json(200, { success: true, date, count: rows.length, rows });
    }

    /* Per-day counts, for the calendar. */
    if (op === "days") {
      const from = str(q.from).trim() || "0000-00-00";
      const to   = str(q.to).trim()   || "9999-99-99";
      const snap = await db.collection(COLL)
        .where("completedDay", ">=", from)
        .where("completedDay", "<=", to)
        .select("completedDay")
        .get();
      const days = {};
      snap.forEach(d => {
        const k = d.get("completedDay");
        if (k) days[k] = (days[k] || 0) + 1;
      });
      return json(200, { success: true, days, total: snap.size });
    }

    if (op === "get") {
      const id = str(q.id).trim();
      if (!id) return json(400, { error: "id required" });
      const doc = await db.collection(COLL).doc(id).get();
      if (!doc.exists) return json(200, { success: false, notFound: true });
      return json(200, { success: true, row: doc.data() });
    }

    if (op === "stats") {
      const [oldest, newest] = await Promise.all([
        db.collection(COLL).orderBy("completedAtMs", "asc").limit(1).get(),
        db.collection(COLL).orderBy("completedAtMs", "desc").limit(1).get(),
      ]);
      const count = await db.collection(COLL).count().get().catch(() => null);
      return json(200, {
        success: true,
        total : count ? count.data().count : null,
        oldest: oldest.empty ? null : oldest.docs[0].get("completedDay"),
        newest: newest.empty ? null : newest.docs[0].get("completedDay"),
        now   : Date.now(),
      });
    }

    /* Which of these receipt ids are already archived — drives the backfill. */
    if (op === "have") {
      const ids = str(q.ids).split(",").map(s => s.trim()).filter(Boolean).slice(0, 200);
      if (!ids.length) return json(200, { success: true, have: [] });
      const snaps = await db.getAll(...ids.map(id => db.collection(COLL).doc(id)));
      return json(200, { success: true, have: snaps.filter(s => s.exists).map(s => s.id) });
    }

    return json(400, { error: `Unknown op "${op}"` });

  } catch (err) {
    console.error("designArchive error:", err);
    return json(500, { error: err.message });
  }
};
