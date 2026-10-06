/*  netlify/functions/_stationLive.js
 *  The live layer of the Employee efficiency console (Paul, 5 Oct 2026: "all the stations in a list ... showing me the current
 *  order that each station is processing, the thumbnails, the QR code, the person who is working on it, how long since it was
 *  scanned"). Two halves in one file:
 *
 *   WRITE  write(db, FV, live, { prefix })     called by firebaseOrders {live} (the open station door, like {activity} and {session}).
 *          One small document per station + device + person in Station_Live (Sandbox_Station_Live in the sandbox), OVERWRITTEN:
 *          no history, no growth. A station says `work` (an order was scanned or started: the document holds the order), `beat`
 *          (a keep-alive every 30 s: only beatAt moves) and `idle` (the order was completed or closed). A document whose
 *          beatAt is older than STALE_MS is not shown: a closed tab, a crashed browser or a lost network leaves no ghost.
 *   READ   op(ctx, body, H)                    called by employeeEfficiency {op:"live"} (gated by the manager passcode).
 *          One small query (beatAt >= now - STALE_MS, no index needed) kept ~2 s in the instance, plus today's open sessions
 *          (who is signed in; 15 s) and today's rollups (parts and orders per station; 20 s). Thumbnails and the customer are
 *          looked up from data the app already stores (Charm_Master_Index for the vector design, Etsy_Listing_Image_Cache and
 *          EtsyMail_Listings for the listing photo, Design_Order_Archive for a buyer's name and a saved picture), kept in the
 *          instance (15 min; a miss 1 min). Etsy is NEVER called from here.
 *
 *  The person is the employee's NAME, never the PIN: a name with no letter is refused, a PIN-looking id is blanked, a 6-digit
 *  number inside a free-text field becomes "[#]". Real and sandbox are separate stores and never mixed.
 *  Contract: /mnt/project-files/plans/employee-hr/api.md  */
"use strict";
const LIVE = "Station_Live";
const { STATIONS } = require("./_orderTimeline");           // one list of stations for the timeline, the sessions, the activity and this
/* The Welding station (Paul, 6 Oct 2026): two tasks (welding | matching), two people at once, never counted in throughput; see _activityKinds.js */
let KIND = null; try { KIND = require("./_activityKinds"); } catch (_) {}
if (!KIND) KIND = { throughput: () => true, readStationCounters: (st, v) => v, UNATTRIBUTED: "Unattributed", isMatched: () => false };

const KEEPALIVE_MS = 30000;        // what the browser does (station-activity.js); told to the console in the answer
const STALE_MS = 180000;           // a document with no keep-alive for this long is not shown
const COALESCE_MS = 15000;         // a repeat of the same write inside this is not written again
const MAX_BODY_CHARS = 8000, MAX_PIECES = 24, MAX_AGE_MS = 12 * 3600e3, SESSION_GONE_MS = 15 * 60000;
const TTL = { live: 2000, sessions: 15000, today: 20000, found: 15 * 60000, miss: 60000 };
const LIM = { live: 200, sessions: 300, rollups: 200, matched: 400, matchedShown: 30 };

/* what the console lists, in order: key, label (as StationSession shows it), the pages that make up the station */
const CATALOG = [
  { key: "sorting", label: "Sorting", devices: [["sorting-1", "Sorting 1"], ["sorting-2", "Sorting 2"]] },
  { key: "welding", label: "Welding", devices: [["weld-1", "Welding"]] },
  { key: "assembly", label: "Assembly", devices: [["assembly-1", "Assembly 1"], ["assembly-2", "Assembly 2"], ["assembly-3", "Assembly 3"], ["assembly-4", "Assembly 4"]] },
  { key: "shipping", label: "Shipping", devices: [["shipping-1", "Shipping 1"], ["shipping-2", "Shipping 2"], ["shipping-3", "Shipping 3"]] },
  { key: "design", label: "Design", devices: [["design", "Design"], ["design-1", "Design 1"], ["design-message", "Design messages"], ["design-message-1", "Design messages 1"]] },
  { key: "laser", label: "Laser", devices: [] },
  { key: "sorter", label: "Sorter", devices: [["charm-nest-1", "Sorter"]] },
  { key: "qr", label: "QR Printer", devices: [] },
  { key: "inbox", label: "Inbox", devices: [["etsy-mail-1", "Inbox"]] }
];
const LABELS = Object.fromEntries(CATALOG.map(s => [s.key, s.label]));
const DEVICE_LABEL = {}; for (const s of CATALOG) for (const [d, l] of s.devices) DEVICE_LABEL[d] = l;

/* ── small helpers ── */
const str = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
const int = (v, lo, hi) => { const x = Math.round(Number(v)); return Number.isFinite(x) ? Math.max(lo, Math.min(hi, x)) : lo; };
const pinLike = s => /^\d{4,8}$/.test(s);                                // a PIN is digits only: never kept as an id
/** A name never carries a PIN: four or more digits in a name (after it, glued to it, spaced "12 34 56") are a login number that slipped in; they are dropped ("Paul 482915" is "Paul"). */
const noPin = s => (s.match(/\p{Nd}/gu) || []).length >= 4 ? s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim() : s;
const hasLetter = s => /\p{L}/u.test(s);
const idText = v => { const s = str(v, 40); return pinLike(s) ? "" : s.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]"); };            // (an order number of 10 digits stays; a 4 to 8 digit one is a login number, not an order)
const digits = (v, n) => String(v == null ? "" : v).replace(/\D/g, "").slice(0, n);
/** free text: no digits-only text, no lone 6-digit number (a PIN) */
const text = (v, n) => { const s = str(v, n); return /^\d+$/.test(s) ? "" : s.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]"); };
const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;
const isSku = s => /^[A-Za-z0-9][A-Za-z0-9 _.,'&()+\-]{1,60}$/.test(s);
const docId = (station, device, person) => `${station}__${device}__${String(person).replace(/\//g, "-")}`.slice(0, 200);
const idOk = id => !!id && !/^__.*__$/.test(id) && !/^\.+$/.test(id);

/* ════════════════════════════ WRITE ════════════════════════════ */

/** One piece as it is stored (≤ 6 small fields), or null. */
function cleanPiece(p) {
  if (!p || typeof p !== "object" || Array.isArray(p)) return null;
  const id = str(p.id, 40).replace(/[^\w.:-]/g, "_"), label = text(p.label, 60);
  const sku = str(p.sku, 60), listingId = digits(p.listingId, 20), size = str(p.size, 6);
  const out = { id: pinLike(id) ? "" : id, label };
  if (sku && isSku(sku) && !pinLike(sku)) out.sku = sku;
  if (/^\d{3,20}$/.test(listingId) && !pinLike(listingId)) out.listingId = listingId;
  if (/^[A-Za-z0-9]{1,6}$/.test(size)) out.size = size;
  return out.id || out.label || out.sku || out.listingId ? out : null;
}
/** How far this computer's clock is from the server's: the server's now minus the clock the browser stamped on its request (`sentAt`),
 *  in whole seconds. Under 10 s (the request's own travel time, or a clock that is nearly right) it counts as none, and so does a
 *  value that is not a time or is over 36 hours off (a clock that far out is not trusted to be put right by a shift). */
function skewOf(sentAt, now) {
  const t = Number(sentAt);
  if (!Number.isFinite(t) || t < 1e12) return 0;
  const d = now - t;
  return Math.abs(d) < 10000 || Math.abs(d) > 36 * 3600e3 ? 0 : Math.round(d / 1000) * 1000;
}
/** The order a station is working on, as stored; { error } when it is not one. `skew`: the browser clock's error (skewOf), added to its scan time. */
function cleanOrder(o, now, skew) {
  if (!o || typeof o !== "object" || Array.isArray(o)) return { error: "no order" };
  const kind = o.kind === "sheet" ? "sheet" : "order";
  let rid = digits(o.rid, 30); if (pinLike(rid)) rid = "";
  let orderNumber = str(o.orderNumber, 40); if (/^\d+$/.test(orderNumber) && (pinLike(orderNumber) || orderNumber !== rid)) orderNumber = "";
  orderNumber = text(orderNumber, 40) || rid;
  const title = text(o.title, 80);
  if (kind === "order" && !rid) return { error: "an order needs its id" };
  if (kind === "sheet" && !title && !orderNumber) return { error: "a sheet needs a title" };
  let customer = text(o.customer, 60); if (!hasLetter(customer)) customer = "";
  const pieces = (Array.isArray(o.pieces) ? o.pieces : []).slice(0, MAX_PIECES).map(cleanPiece).filter(Boolean);
  const at0 = Number(o.scannedAt) + (skew || 0);
  let scannedAt = Number.isFinite(at0) && at0 > 1e12 ? Math.round(at0) : now;
  if (scannedAt > now) scannedAt = now;
  if (now - scannedAt > MAX_AGE_MS) scannedAt = now;
  const out = { kind, rid, orderNumber, customer, scannedAt, pieces, pieceCount: Math.max(pieces.length, int(o.pieceCount, 0, 500)), note: text(o.note, 80), title };
  return { order: out };
}
const fpOf = o => JSON.stringify([o.kind, o.rid, o.orderNumber, o.customer, o.note, o.title, o.scannedAt, o.pieceCount, o.pieces]);

/* the last write per document in this instance: a repeat of it inside COALESCE_MS is not written again */
const seen = new Map();
const seenKey = (prefix, id) => prefix + "|" + id;
function remember(key, fp, event, now) { seen.delete(key); seen.set(key, { at: now, fp, event }); while (seen.size > 3000) seen.delete(seen.keys().next().value); }

/**
 * live: { v, event: "work" | "beat" | "idle", station, device, computer?, session?, person, startAt?, sandbox?, order?, ended? }
 * Returns [statusCode, body]. Never reads: a write is one set() (work, idle) or one update() (beat).
 */
async function write(db, FV, live, opts) {
  opts = opts || {};
  const prefix = opts.prefix || "", now = opts.now || Date.now();
  if (!live || typeof live !== "object" || Array.isArray(live)) return [400, { error: "not a live event" }];
  if (typeof live.sandbox === "boolean" && live.sandbox !== !!prefix) return [400, { error: "an event keeps the store it was recorded for" }];
  const event = live.event === "work" || live.event === "beat" || live.event === "idle" ? live.event : "";
  const station = typeof live.station === "string" && STATIONS.has(live.station) ? live.station : "";
  const device = str(live.device, 40).replace(/[^\w .:-]/g, "");
  const person = noPin(str(live.person, 200)).slice(0, 80);
  if (!event || !station || !device || !person || !hasLetter(person)) return [400, { error: "not a live event" }];   // no letter (digits, "123 456") = a PIN, never a name
  const id = docId(station, device, person);
  if (!idOk(id)) return [400, { error: "not a live event" }];
  const key = seenKey(prefix, id), last = seen.get(key);
  const ref = db.collection(prefix + LIVE).doc(id);

  if (event === "beat") {
    if (last && now - last.at < COALESCE_MS) return [200, { success: true, coalesced: true }];
    try { await ref.update({ beatAt: now }); }
    catch (e) { if (e && (e.code === 5 || e.code === "not-found" || /NOT_FOUND|no document to update/i.test(String(e.message || "")))) return [200, { success: true, resend: true }]; throw e; }
    remember(key, last ? last.fp : "", "beat", now);
    return [200, { success: true, written: 1 }];
  }

  const computer = typeof live.computer === "string" && /^[\w-]{6,64}$/.test(live.computer) ? live.computer : "";
  const session = typeof live.session === "string" && /^[\w.:-]{8,100}$/.test(live.session) ? live.session : "";
  const skew = skewOf(live.sentAt, now);                    // a computer whose clock is minutes off still shows the true time since the scan
  const sa = Number(live.startAt) + skew, sinceAt = Number.isFinite(sa) && sa > 1e12 && sa <= now ? Math.round(sa) : 0;
  const base = { id, v: 1, station, device, computer, session, person, sinceAt, beatAt: now, eventAt: now };
  if (prefix) base.sandbox = true;

  if (event === "work") {
    const r = cleanOrder(live.order, now, skew);
    if (r.error) return [400, { error: r.error }];
    const fp = fpOf(r.order);
    if (last && last.event === "work" && last.fp === fp && now - last.at < COALESCE_MS) return [200, { success: true, coalesced: true }];
    await ref.set(Object.assign(base, { state: "working" }, r.order));
    remember(key, fp, "work", now);
    return [200, { success: true, written: 1 }];
  }

  // idle: the order was completed or closed; what it was is kept as `last` for the card's "last order" line
  const e = live.ended && typeof live.ended === "object" ? cleanOrder(Object.assign({ kind: live.ended.kind }, live.ended), now, skew) : { error: "none" };
  const doc = Object.assign(base, { state: "idle", idleAt: now });
  if (e.order) doc.last = { kind: e.order.kind, rid: e.order.rid, orderNumber: e.order.orderNumber, title: e.order.title, scannedAt: e.order.scannedAt };
  const fp = doc.last ? JSON.stringify(doc.last) : "";
  if (last && last.event === "idle" && last.fp === fp && now - last.at < COALESCE_MS) return [200, { success: true, coalesced: true }];
  await ref.set(doc);
  remember(key, fp, "idle", now);
  return [200, { success: true, written: 1 }];
}

/* ════════════════════════════ READ ════════════════════════════ */

const safe = (p, label) => p.then(value => ({ ok: true, value }), e => ({ ok: false, label, error: String((e && (e.message || e.code)) || e || "failed").slice(0, 160) }));
const col = (ctx, name) => ctx.db.collection(ctx.prefix + name);

/** a small keyed store per Firestore handle: value or a miss, each with its own life */
function store(ctx, name) {
  const c = ctx.cache.live || (ctx.cache.live = {});
  return c[name] || (c[name] = new Map());
}
function remembered(ctx, name, key) {
  const m = store(ctx, name), e = m.get(key);
  if (!e) return undefined;
  const life = Math.min(e.v ? TTL.found : TTL.miss, ctx.life);
  if (ctx.now - e.at >= life) { m.delete(key); return undefined; }
  return e.v;
}
function keep(ctx, name, key, v) {
  const m = store(ctx, name); m.delete(key); m.set(key, { at: ctx.now, v: v || null });
  while (m.size > 3000) m.delete(m.keys().next().value);
}
/** getAll of refs, with a fallback to single gets */
async function getMany(ctx, refs) {
  if (!refs.length) return [];
  if (typeof ctx.db.getAll === "function") return ctx.db.getAll(...refs);
  return Promise.all(refs.map(r => r.get()));
}
const urlOf = x => { const u = x && typeof x === "object" ? (x.url_570xN || x.url || x.url_fullxfull || x.url_340x270 || "") : ""; return /^https:\/\/[^\s"<>]{8,1900}$/.test(String(u)) ? String(u) : ""; };
const httpsOnly = u => (/^https:\/\/[^\s"<>]{8,1900}$/.test(String(u || "")) ? String(u) : "");

/** the vector-design thumbnail of each SKU (Charm_Master_Index/{SKU}: thumbUrl, per size when it has sizes) → Map sku → { thumbUrl, sizes:{S:url} } */
async function masters(ctx, skus) {
  const need = skus.filter(s => remembered(ctx, "master", s) === undefined);
  if (need.length) {
    const snaps = await getMany(ctx, need.map(s => ctx.db.collection("Charm_Master_Index").doc(s.toUpperCase())));
    need.forEach((s, i) => {
      const d = snaps[i] && snaps[i].exists ? (snaps[i].data() || {}) : null;
      const sizes = {}; if (d && d.sizes && typeof d.sizes === "object") for (const [k, v] of Object.entries(d.sizes)) { const u = httpsOnly(v && v.thumbUrl); if (u) sizes[k.toUpperCase()] = u; }
      const base = d ? httpsOnly(d.thumbUrl) : "";
      keep(ctx, "master", s, base || Object.keys(sizes).length ? { thumbUrl: base, sizes } : null);
    });
  }
  return new Map(skus.map(s => [s, remembered(ctx, "master", s) || null]));
}
/** the listing photo of each listing (the cache the sorter fills, then the Etsy Mail catalog: nothing is asked of Etsy) → Map id → url */
async function listings(ctx, ids) {
  const need = ids.filter(s => remembered(ctx, "listing", s) === undefined);
  if (need.length) {
    const found = new Map();
    const a = await getMany(ctx, need.map(s => ctx.db.collection("Etsy_Listing_Image_Cache").doc(s)));
    need.forEach((s, i) => { const d = a[i] && a[i].exists ? (a[i].data() || {}) : null; const u = d && Array.isArray(d.images) ? urlOf(d.images.slice().sort((x, y) => (Number(x && x.rank) || 99) - (Number(y && y.rank) || 99))[0]) : ""; if (u) found.set(s, u); });
    const rest = need.filter(s => !found.has(s));
    if (rest.length) {
      const b = await getMany(ctx, rest.map(s => ctx.db.collection("EtsyMail_Listings").doc(s)));
      rest.forEach((s, i) => { const d = b[i] && b[i].exists ? (b[i].data() || {}) : null; const u = d && Array.isArray(d.images) ? urlOf(d.images[0]) : ""; if (u) found.set(s, u); });
    }
    for (const s of need) keep(ctx, "listing", s, found.get(s) || null);
  }
  return new Map(ids.map(s => [s, remembered(ctx, "listing", s) || null]));
}
/** the buyer's name and the saved item pictures of an order the Design Station completed (Design_Order_Archive/{rid}) → Map rid → { buyer, items:[{transactionId, sku, url}] } */
async function archives(ctx, rids) {
  const mk = s => ctx.prefix + s;                                  // (a store of its own: the real shop and the Sandbox keep different archives for one order id, and one instance serves both)
  const need = rids.filter(s => remembered(ctx, "archive", mk(s)) === undefined);
  if (need.length) {
    const refs = need.map(s => col(ctx, "Design_Order_Archive").doc(s));
    const snaps = typeof ctx.db.getAll === "function" ? await ctx.db.getAll(...refs, { fieldMask: ["buyer.name", "ship.name", "items"] }) : await Promise.all(refs.map(r => r.get()));
    need.forEach((s, i) => {
      const d = snaps[i] && snaps[i].exists ? (snaps[i].data() || {}) : null;
      if (!d) return keep(ctx, "archive", mk(s), null);
      const items = (Array.isArray(d.items) ? d.items : []).slice(0, 40).map(it => it && typeof it === "object" ? { transactionId: digits(it.transactionId || it.transaction_id, 20), sku: str(it.sku, 60), url: httpsOnly(it.mirrorUrl) || httpsOnly(it.imageUrl) } : null).filter(Boolean);
      const buyer = text(d.buyer && d.buyer.name || d.ship && d.ship.name || "", 60);
      keep(ctx, "archive", mk(s), { buyer: hasLetter(buyer) ? buyer : "", items });
    });
  }
  return new Map(rids.map(s => [s, remembered(ctx, "archive", mk(s)) || null]));
}

/** fills each current order with its thumbnails and customer, from what is already stored */
async function dress(ctx, list) {
  const errors = [];
  // 1 · an order the station said little about (no customer, no pieces): the saved order has its buyer and its items
  const rids = new Set();
  for (const c of list) if (c.kind === "order" && c.rid && (!c.customer || !c.pieces.length)) rids.add(c.rid);
  const a = rids.size ? await safe(archives(ctx, [...rids]), "archive") : { ok: true, value: new Map() };
  if (!a.ok) errors.push(a.label + ": " + a.error);
  const A = a.ok ? a.value : new Map();
  for (const c of list) {
    const arc = c.kind === "order" ? A.get(c.rid) || null : null;
    c.arc = arc;
    if (!c.customer && arc && arc.buyer) c.customer = arc.buyer;
    if (!c.pieces.length && arc && arc.items.length) {                    // no pieces from the station: the saved items
      c.pieces = arc.items.slice(0, MAX_PIECES).map((it, i) => ({ id: it.transactionId ? `${c.rid}_${it.transactionId}_1` : `${c.rid}_${i + 1}`, label: "", sku: it.sku || "", _url: it.url }));
      c.pieceCount = Math.max(c.pieceCount, arc.items.length);
    }
  }
  // 2 · the vector design of each SKU and the listing photo of each listing (one read of each kind, kept 15 minutes)
  const skus = new Set(), lids = new Set();
  for (const c of list) for (const p of c.pieces) { if (p.sku) skus.add(p.sku.toUpperCase()); if (p.listingId) lids.add(p.listingId); }
  const [m, l] = await Promise.all([masters(ctx, [...skus]), listings(ctx, [...lids])].map((p, i) => safe(p, ["designs", "photos"][i])));
  for (const r of [m, l]) if (!r.ok) errors.push(r.label + ": " + r.error);
  const M = m.ok ? m.value : new Map(), L = l.ok ? l.value : new Map();
  for (const c of list) {
    const arc = c.arc; delete c.arc;
    for (const p of c.pieces) {
      const mm = p.sku ? M.get(p.sku.toUpperCase()) : null;
      p.vectorUrl = mm ? (p.size && mm.sizes[p.size.toUpperCase()]) || mm.thumbUrl || Object.values(mm.sizes)[0] || "" : "";
      p.photoUrl = (p.listingId && L.get(p.listingId)) || p._url || "";
      p.thumbUrl = p.vectorUrl || p.photoUrl || "";
      delete p._url; delete p.listingId; delete p.size;
      if (!p.sku) delete p.sku;
    }
    const first = c.pieces.find(p => p.vectorUrl) || c.pieces.find(p => p.photoUrl) || null;
    const photo = (c.pieces.find(p => p.photoUrl) || {}).photoUrl || (arc && (arc.items.find(i => i.url) || {}).url) || "";
    c.vectorUrl = (c.pieces.find(p => p.vectorUrl) || {}).vectorUrl || "";
    c.photoUrl = photo; c.thumbUrl = c.vectorUrl || c.photoUrl || (first && first.thumbUrl) || "";
    c.qr = c.kind === "order" && c.rid ? { text: c.rid } : null;
  }
  return errors;
}

/** the live documents, kept ~2 s */
function readLive(ctx, H) {
  return H.cached(ctx, `live|${ctx.prefix}`, TTL.live, async () => {
    const snap = await col(ctx, LIVE).where("beatAt", ">=", ctx.now - STALE_MS).limit(LIM.live + 1).get();
    const rows = []; for (const d of snap.docs.slice(0, LIM.live)) { const v = d.data() || {}; if (!!v.sandbox === !!ctx.prefix) rows.push(Object.assign({ _id: d.id }, v)); }
    return { rows, capped: snap.docs.length > LIM.live };
  });
}
/** today's sessions that are still open: who is signed in where (15 s) */
function readSessions(ctx, H) {
  return H.cached(ctx, `lsess|${ctx.prefix}`, TTL.sessions, async () => {
    const snap = await col(ctx, "Station_Sessions").where("startAt", ">=", H.nyMidnight(ctx.today)).orderBy("startAt", "desc").limit(LIM.sessions + 1).get();
    const rows = []; for (const d of snap.docs.slice(0, LIM.sessions)) { const v = d.data() || {}; const station = str(v.station, 20); rows.push({ person: str(v.person, 80), station, device: text(v.device, 40), startAt: ms(v.startAt), lastSeenAt: ms(v.lastSeenAt), endAt: ms(v.endAt), task: station === "welding" && (v.task === "welding" || v.task === "matching") ? v.task : "", role: v.role === "laser" || v.role === "design" ? v.role : "", lastInputAt: ms(v.lastInputAt) }); }
    return { rows, capped: snap.docs.length > LIM.sessions };
  });
}
/** today's rollups, only the parts the board shows (20 s) → per station { parts, orders, scans, lastAt } */
function readToday(ctx, H) {
  return H.cached(ctx, `ltoday|${ctx.prefix}`, TTL.today, async () => {
    let q = col(ctx, "Efficiency_Daily").where("day", "==", ctx.today).limit(LIM.rollups + 1);
    if (typeof q.select === "function") q = q.select("day", "person", "stations", "sandbox", "touched");
    const snap = await q.get(), by = {}, touched = {};
    for (const d of snap.docs.slice(0, LIM.rollups)) {
      const v = d.data() || {};
      if (!!v.sandbox !== !!ctx.prefix) continue;                       // (a document of the other store never counts, as in every other reader)
      for (const [st, x0] of Object.entries(v.stations && typeof v.stations === "object" ? v.stations : {})) {
        if (!x0 || typeof x0 !== "object") continue;
        const x = KIND.readStationCounters(st, x0);                         // (the Welding station keeps its scans and matched count, never pieces or orders)
        const t = by[st] || (by[st] = { parts: 0, orders: 0, scans: 0, lastAt: 0, matched: 0, unattributed: 0 });
        t.parts += Math.max(0, (Number(x.parts) || 0) - (Number(x.undoParts) || 0));
        t.orders += Math.max(0, (Number(x.orders) || 0) - (Number(x.undoOrders) || 0));
        t.scans += Math.max(0, Number(x.scans) || 0);
        const mt = Math.max(0, Number(x.matched) || 0); t.matched += mt; if (v.person === KIND.UNATTRIBUTED) t.unattributed += mt;
        t.lastAt = Math.max(t.lastAt, ms(x.lastAt));
      }
      // the orders the person touched today and where (a scan counts the moment it happens; "orders" above counts only the finished ones)
      if (v.touched && typeof v.touched === "object") for (const [oid, sts] of Object.entries(v.touched)) if (sts && typeof sts === "object") for (const st of Object.keys(sts)) if (KIND.throughput(st)) (touched[st] || (touched[st] = new Set())).add(oid);
    }
    // A station's orders today = the orders worked there, as the Overview counts them (an order in hand is one the moment it is scanned), never fewer than the finished ones.
    // Counting only the finished ones made the Stations board say 9 where the Overview, the People cards and the person page said 10 while one order was in hand.
    for (const [st, set] of Object.entries(touched)) { const t = by[st] || (by[st] = { parts: 0, orders: 0, scans: 0, lastAt: 0, matched: 0, unattributed: 0 }); t.orders = Math.max(t.orders, set.size); }
    return { by, capped: snap.docs.length > LIM.rollups };
  });
}

/** Today's matched scans at the Welding station (newest first), read only when today's rollups say there are some (the answer is kept while that count stays the same):
 *  two equalities (day, station), no index; the scans of the person in Matching or of nobody ("Unattributed"). */
function readMatched(ctx, H, count) {
  if (!(count > 0)) return Promise.resolve({ rows: [], capped: false });
  return H.cached(ctx, `lmatched|${ctx.prefix}|${ctx.today}|${count}`, 60000, async () => {
    const snap = await col(ctx, "Station_Activity").where("day", "==", ctx.today).where("station", "==", "welding").limit(LIM.matched + 1).get();
    const rows = [];
    for (const d of snap.docs.slice(0, LIM.matched)) {
      const v = d.data() || {};
      if (!!v.sandbox !== !!ctx.prefix || !KIND.isMatched({ action: v.action, task: v.task, station: v.station })) continue;
      const rid = idText(digits(v.orderId, 30)); if (!rid) continue;
      rows.push({ rid, at: ms(v.at), person: str(v.person, 80), task: "matching", unattributed: v.unattributed === true || v.person === KIND.UNATTRIBUTED });
    }
    rows.sort((a, b) => b.at - a.at || (a.rid < b.rid ? -1 : 1));
    return { rows, capped: snap.docs.length > LIM.matched };
  });
}
/** Milliseconds covered by [s, e] pairs, an overlap counted once. */
function covered(spans) {
  let total = 0, a = -Infinity, z = -Infinity;
  for (const [s, e] of spans.slice().sort((x, y) => x[0] - y[0])) { if (s > z) { if (z > a) total += z - a; a = s; z = e; } else z = Math.max(z, e); }
  if (z > a) total += z - a;
  return total;
}

/**
 * op "live": { op:"live", key, sandbox? } → the board's data. H = { json, nyMidnight, cached, display(rawName) }.
 * Answer: see api.md. Every number is for the New York day `ctx.today`; a station with nothing is { state:"offline" } and still listed.
 */
async function op(ctx, body, H) {
  const now = ctx.now;
  const [lr, sr, tr] = await Promise.all([safe(readLive(ctx, H), "live"), safe(readSessions(ctx, H), "sessions"), safe(readToday(ctx, H), "today")]);
  const mr = await safe(readMatched(ctx, H, tr.ok && tr.value.by.welding ? tr.value.by.welding.matched : 0), "matched");
  if (!lr.ok && !sr.ok) { const e = new Error("both reads failed"); e.unavailable = [lr.error, sr.error]; throw e; }
  const errors = [lr, sr, tr, mr].filter(r => !r.ok).map(r => r.label + ": " + r.error);
  const capped = [lr, sr, tr].filter(r => r.ok && r.value.capped).length > 0;

  // 1 · the current orders (a document that beat inside STALE_MS and says it is working), newest scan first
  const cur = [], idleLive = [];
  for (const v of lr.ok ? lr.value.rows : []) {
    if (!STATIONS.has(v.station) || now - ms(v.beatAt) > STALE_MS) continue;
    if (v.state === "working" && (v.rid || v.title)) {
      const dev = text(v.device, 40);                                    // (a stored device that is a number, or an object, or "constructor" is no device: never shown as it is)
      cur.push({ station: v.station, person: H.display(v.person), device: dev, deviceLabel: Object.prototype.hasOwnProperty.call(DEVICE_LABEL, dev) ? DEVICE_LABEL[dev] : dev, kind: v.kind === "sheet" ? "sheet" : "order",
        rid: idText(v.rid), orderNumber: idText(v.orderNumber) || idText(v.rid), customer: text(v.customer, 60), title: text(v.title, 80), scannedAt: ms(v.scannedAt) || ms(v.eventAt), beatAt: ms(v.beatAt), since: ms(v.sinceAt) || 0,
        pieces: (Array.isArray(v.pieces) ? v.pieces : []).slice(0, MAX_PIECES).map(p => ({ id: str(p && p.id, 40), label: str(p && p.label, 60), sku: str(p && p.sku, 60), listingId: str(p && p.listingId, 20), size: str(p && p.size, 6) })),
        pieceCount: Math.max(0, Number(v.pieceCount) || 0), note: text(v.note, 80) });
    } else if (v.state === "idle") idleLive.push(v);
  }
  cur.sort((a, b) => b.scannedAt - a.scannedAt || (a.device < b.device ? -1 : 1));
  const dressErrors = cur.length ? await dress(ctx, cur) : [];
  errors.push(...dressErrors);

  // 2 · who is signed in (an open session, beat within 15 minutes), one row per person and page
  const signedIn = [], open = new Map();
  for (const s of sr.ok ? sr.value.rows : []) {
    if (!s.person || !hasLetter(s.person) || !STATIONS.has(s.station) || s.endAt) continue;
    const last = Math.max(s.startAt, s.lastSeenAt);
    if (!(s.startAt > 0) || now - last >= SESSION_GONE_MS) continue;
    const name = H.display(s.person), k = `${s.station}|${s.device}|${name}|${s.task}|${s.role}`, had = open.get(k);   // (one row per page, person and task: two people at the Welding station, or one person in both tasks, are separate rows)
    if (had) { had.since = Math.min(had.since, s.startAt); had.lastSeenAt = Math.max(had.lastSeenAt, last); if (s.lastInputAt) had.lastInputAt = Math.max(had.lastInputAt || 0, s.lastInputAt); continue; }
    const row = { name, stationKey: s.station, device: s.device, since: s.startAt, lastSeenAt: last };
    if (s.task) row.task = s.task;
    if (s.role) row.role = s.role;
    if (s.lastInputAt) row.lastInputAt = s.lastInputAt;
    open.set(k, row); signedIn.push(row);
  }
  // a person working at a page whose session is not in today's list (the sorter's laser station has none): shown as signed in there while they work
  for (const c of cur) { const k = `${c.station}|${c.device}|${c.person}||`; if (![...open.keys()].some(x => x.startsWith(`${c.station}|${c.device}|${c.person}|`)) && !open.has(k)) { const r = { name: c.person, stationKey: c.station, device: c.device, since: c.since || c.scannedAt, lastSeenAt: c.beatAt }; open.set(k, r); signedIn.push(r); } }
  signedIn.sort((a, b) => a.since - b.since || (a.name < b.name ? -1 : 1));

  // 3 · the stations
  const today = tr.ok ? tr.value.by : {}, stations = [];
  // the Welding station: today's matched scans (newest first, dressed with the order's thumbnail like a current order) and the time signed in per task today
  const todayStart = H.nyMidnight(ctx.today), matchedRows = mr.ok ? mr.value.rows : [];
  const matched = matchedRows.slice(0, LIM.matchedShown).map(m => ({ kind: "order", rid: m.rid, orderNumber: m.rid, at: m.at, person: m.unattributed ? "" : H.display(m.person), task: "matching", unattributed: m.unattributed, customer: "", pieces: [], pieceCount: 0, note: "" }));
  if (matched.length) { try { errors.push(...await dress(ctx, matched)); } catch (e) { errors.push("matched: " + String((e && e.message) || e).slice(0, 120)); } }
  const lastScan = new Map(); for (const m of matchedRows) if (!m.unattributed) { const n = H.display(m.person); lastScan.set(n, Math.max(lastScan.get(n) || 0, m.at)); }
  const weldRows = (sr.ok ? sr.value.rows : []).filter(r => r.station === "welding" && r.startAt > 0 && r.person && hasLetter(r.person));
  const spanOf = r => [Math.max(r.startAt, todayStart), r.endAt || (now - Math.max(r.startAt, r.lastSeenAt) < SESSION_GONE_MS ? now : Math.max(r.startAt, r.lastSeenAt))];
  const taskMsOf = (match, who) => covered(weldRows.filter(r => (r.task || "unknown") === match && (!who || H.display(r.person) === who)).map(spanOf).filter(x => x[1] > x[0]));
  for (const s of CATALOG) {
    const mine = cur.filter(c => c.station === s.key), folks = signedIn.filter(p => p.stationKey === s.key);
    const people = folks.map(p => { const o = { name: p.name, since: p.since, lastSeenAt: p.lastSeenAt, lastInputAt: p.lastInputAt || null, device: p.device, deviceLabel: Object.prototype.hasOwnProperty.call(DEVICE_LABEL, p.device) ? DEVICE_LABEL[p.device] : p.device }; if (p.task) o.task = p.task; if (p.role) o.role = p.role; return o; });
    for (const c of mine) if (!people.some(p => p.name === c.person)) people.push({ name: c.person, since: c.since || c.scannedAt, lastSeenAt: c.beatAt, lastInputAt: null, device: c.device, deviceLabel: c.deviceLabel });
    const names = [...new Set(people.map(p => p.name))];
    const devs = new Map(s.devices.map(([d, l]) => [d, { device: d, label: l, state: "offline", person: "", since: 0 }]));
    for (const p of folks) { const x = devs.get(p.device) || { device: p.device, label: DEVICE_LABEL[p.device] || p.device, state: "offline", person: "", since: 0 }; devs.set(p.device, x); x.state = "idle"; x.person = x.person && x.person !== p.name && !x.person.split(", ").includes(p.name) ? x.person + ", " + p.name : p.name; x.since = x.since ? Math.min(x.since, p.since) : p.since; }
    for (const c of mine) { const x = devs.get(c.device) || { device: c.device, label: c.deviceLabel, state: "offline", person: "", since: 0 }; devs.set(c.device, x); x.state = "working"; if (!x.person) x.person = c.person; x.since = c.since || x.since; }
    const t = today[s.key] || (tr.ok ? { parts: 0, orders: 0, scans: 0, lastAt: 0, matched: 0, unattributed: 0 } : { parts: null, orders: null, scans: null, lastAt: 0, matched: null, unattributed: null });   // (today's rollups could not be read: the counts are unknown, a dash, never a 0)
    let lastEventAt = t.lastAt;
    for (const c of mine) lastEventAt = Math.max(lastEventAt, c.scannedAt);
    for (const v of idleLive) if (v.station === s.key) lastEventAt = Math.max(lastEventAt, ms(v.idleAt) || ms(v.eventAt));
    const row = { key: s.key, label: s.label, state: mine.length ? "working" : folks.length ? "idle" : "offline", people, names,
      current: mine.map(c => ({ id: `${c.station}__${c.device}__${c.person}`, person: c.person, device: c.device, deviceLabel: c.deviceLabel, kind: c.kind, rid: c.rid, orderNumber: c.orderNumber, customer: c.customer, title: c.title,
        scannedAt: c.scannedAt, beatAt: c.beatAt, thumbUrl: c.thumbUrl, photoUrl: c.photoUrl, vectorUrl: c.vectorUrl, qr: c.qr, pieces: c.pieces, pieceCount: c.pieceCount, note: c.note })),
      devices: [...devs.values()], lastEventAt: lastEventAt || null, counts: { partsToday: t.parts, ordersToday: t.orders, scansToday: t.scans } };
    if (!KIND.throughput(s.key)) {
      // never "pieces / orders today" here: time on task per task and the matched scans stand in their place
      for (const p of people) { if (p.task === "matching") { const n = lastScan.get(p.name); if (n && n > (p.lastInputAt || 0)) p.lastInputAt = n; } p.todayMs = taskMsOf(p.task || "unknown", p.name); }
      row.noThroughput = true;
      row.counts = { partsToday: null, ordersToday: null, scansToday: tr.ok ? t.matched : null };
      row.today = { day: ctx.today, matched: tr.ok ? t.matched : null, unattributed: tr.ok ? t.unattributed : null, taskMs: { welding: taskMsOf("welding"), matching: taskMsOf("matching"), unknown: taskMsOf("unknown") } };
      row.matched = matched.map(m => ({ rid: m.rid, orderNumber: m.orderNumber, at: m.at, person: m.person, task: m.task, unattributed: m.unattributed, customer: m.customer, thumbUrl: m.thumbUrl || "", vectorUrl: m.vectorUrl || "", photoUrl: m.photoUrl || "", pieceCount: m.pieceCount, pieces: m.pieces }));
    }
    stations.push(row);
  }
  const out = { ok: true, at: now, mode: ctx.prefix ? "sandbox" : "real", day: ctx.today, keepAliveMs: KEEPALIVE_MS, staleMs: STALE_MS, stations, signedIn };
  if (errors.length) { out.partial = true; out.errors = errors; }
  else if (capped) out.partial = true;
  return H.json(200, out);
}

module.exports = { LIVE, KEEPALIVE_MS, STALE_MS, COALESCE_MS, MAX_BODY_CHARS, MAX_PIECES, CATALOG, LABELS, cleanOrder, cleanPiece, skewOf, write, op, _t: { seen } };
