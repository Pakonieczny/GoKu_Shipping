/*  order-timeline.js — the order timeline's shared client (Paul, 28 Sep 19:21).
 *  Loaded by the sorter (charm-nest-1.html) and by every production station page. Records what happens to an order
 *  and reads an order's whole timeline back. Server side: netlify/functions/_orderTimeline.js.
 *
 *    OrderTimeline.config({ mode, sandbox, by, station, device, passcode })
 *        mode "sorter"  → writes through charmNestLibrary (op timelineAdd), every event type
 *        mode "station" → writes through firebaseOrders (open door, station event types only)
 *    OrderTimeline.record({ orderId, type, text, lineKey, transactionId, sheetId, sheet, setId, data, id, at, by, station })
 *        never throws, never waits: the event goes to an outbox (kept in localStorage, so a reload or an offline
 *        minute loses nothing) and is sent in batches about a second later, retried with back-off
 *    OrderTimeline.get(orderId)          → Promise<{ events, cancelled, now }>  (sorter mode)
 *    OrderTimeline.cancelCheck(ids)      → Promise<{ cancelled: { id: { at, by, why, source, sheets } } }>
 *    OrderTimeline.TYPES                 → { type: { label, group, milestone } } — the stamp list the timeline draws
 *    OrderTimeline.onRecord(fn)          → fn(event) for every event recorded on this page (a live timeline listens)
 */
(function () {
  "use strict";
  if (window.OrderTimeline) return;
  const TYPES = {
    arrived:         { label: "Order in",            group: "order",    milestone: true },
    pulled:          { label: "Pulled",              group: "order" },
    interpreted:     { label: "Read",                group: "order" },
    needsDecision:   { label: "Needs a decision",    group: "review" },
    decided:         { label: "Approved",            group: "review",   milestone: true },
    skipped:         { label: "Skipped",             group: "review" },
    customRead:      { label: "Custom read",         group: "review" },
    customDecided:   { label: "Custom decided",      group: "review" },
    designSent:      { label: "Sent to sheet",       group: "design" },
    designDropped:   { label: "Design added",        group: "design" },
    engraveNeeded:   { label: "Back engraving",      group: "engrave" },
    engraveApproved: { label: "Engraving approved",  group: "engrave",  milestone: true },
    engraveChanged:  { label: "Engraving changed",   group: "engrave" },
    pooled:          { label: "Ready to nest",       group: "sheet" },
    placed:          { label: "On sheet",            group: "sheet",    milestone: true },
    moved:           { label: "Moved sheet",         group: "sheet" },
    removed:         { label: "Taken off sheet",     group: "sheet" },
    renested:        { label: "Re-nested",           group: "sheet" },
    held:            { label: "On hold",             group: "hold" },
    released:        { label: "Hold released",       group: "hold" },
    restored:        { label: "Restored",            group: "hold" },
    cancelled:       { label: "Cancelled",           group: "cancel" },
    etsyCancelled:   { label: "Cancelled on Etsy",   group: "cancel" },
    cancelRestored:  { label: "Cancel undone",       group: "cancel" },
    sizeChanged:     { label: "Sheet size changed",  group: "sheet" },
    included:        { label: "In current set",      group: "sheet" },
    excluded:        { label: "Out of current set",  group: "sheet" },
    merged:          { label: "Sheets merged",       group: "sheet" },
    roseLine:        { label: "RG green line",       group: "sheet" },
    roseCut:         { label: "RG sheet cut",        group: "laser" },
    qrLabel:         { label: "QR label",            group: "label",    milestone: true },
    setCommitted:    { label: "Set committed",       group: "label",    milestone: true },
    laserDone:       { label: "Laser cut",           group: "laser",    milestone: true },
    recalled:        { label: "Recalled",            group: "laser" },
    sealPrinted:     { label: "Label printed",       group: "label" },
    sealCompleted:   { label: "Order completed",     group: "done",     milestone: true },
    scan:            { label: "Scanned",             group: "station" },
    sorted:          { label: "Sorted",              group: "station",  milestone: true },
    welded:          { label: "Welded",              group: "station",  milestone: true },
    assembled:       { label: "Assembled",           group: "station",  milestone: true },
    packed:          { label: "Packed",              group: "station",  milestone: true },
    labelPrinted:    { label: "Shipping label",      group: "ship",     milestone: true },
    shipped:         { label: "Shipped",             group: "ship",     milestone: true },
    etsyCompleted:   { label: "Completed on Etsy",   group: "done",     milestone: true },
    cancelAlert:     { label: "Cancel alert seen",   group: "cancel" },
    note:            { label: "Note",                group: "note" },
    teamMessage:     { label: "Team message",        group: "note" },
    customerMessage: { label: "Customer message",    group: "note" },
    other:           { label: "Event",               group: "note" }
  };
  const STATION_TYPES = new Set(["scan", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "etsyCompleted", "cancelAlert", "note"]);
  const cfg = { mode: "sorter", sandbox: false, by: "", station: "", device: "", passcode: "" };
  const OUTBOX = "orderTimeline.outbox.v1", listeners = new Set(), KEEP = 2000;   // at most KEEP events wait in memory (500 across a reload)
  let box = [], timer = 0, sending = false, backoff = 1000;
  try { box = JSON.parse(localStorage.getItem(OUTBOX) || "[]"); if (!Array.isArray(box)) box = []; } catch (_) { box = []; }
  const save = () => { try { localStorage.setItem(OUTBOX, JSON.stringify(box.slice(-500))); } catch (_) {} };
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const base = () => (location.protocol === "file:" ? "https://goldenspike.app" : "") + "/.netlify/functions/";
  async function post(fn, body, sandbox = cfg.sandbox) {
    const headers = { "Content-Type": "application/json" };
    if (cfg.passcode) headers["X-Edit-Passcode"] = cfg.passcode;
    const url = base() + fn + (fn === "firebaseOrders" && sandbox ? "?sandbox=1" : "");
    const r = await fetch(url, { method: "POST", headers, body: JSON.stringify(body), keepalive: true });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || j.error) throw new Error(j.error || "HTTP " + r.status);
    return j;
  }
  function schedule(ms) { clearTimeout(timer); timer = setTimeout(flush, ms); }
  async function flush() {
    if (sending || !box.length) return;
    sending = true;
    // one store per batch: an event keeps the store (production or sandbox) it was recorded in
    const sb = !!box[0].sandbox, batch = [];
    for (const ev of box) { if (!!ev.sandbox !== sb) break; batch.push(ev); if (batch.length >= 50) break; }
    try {
      if (cfg.mode === "station") await post("firebaseOrders", { timeline: batch }, sb);
      else await post("charmNestLibrary", { op: "timelineAdd", events: batch, sandbox: sb }, sb);
      box = box.slice(batch.length); save(); backoff = 1000;
      if (box.length) schedule(50);
    } catch (e) {
      backoff = Math.min(backoff * 2, 120000); schedule(backoff);
      try { console.warn("[OrderTimeline] not sent yet, retrying:", e.message); } catch (_) {}
    } finally { sending = false; }
  }
  /** Records one event. Returns the event (as queued) or null when it is not one. */
  function record(e) {
    try {
      const orderId = digits(e && e.orderId); if (!orderId || !TYPES[e.type]) return null;
      if (cfg.mode === "station" && !STATION_TYPES.has(e.type)) return null;
      const at = Number(e.at) > 1e12 ? Number(e.at) : Date.now();
      const ev = Object.assign({}, e, { orderId, at, by: e.by || cfg.by || "", station: e.station || cfg.station || "", device: e.device || cfg.device || "", sandbox: !!cfg.sandbox });
      ev.id = String(e.id || `${at}-${Math.random().toString(36).slice(2, 8)}`);
      box.push(ev); if (box.length > KEEP) box.splice(0, box.length - KEEP);   // (a server refusing for days: memory stays bounded)
      save(); schedule(900);
      for (const fn of listeners) { try { fn(ev); } catch (_) {} }
      return ev;
    } catch (_) { return null; }
  }
  async function get(orderId) {
    const id = digits(orderId); if (!id) throw new Error("no order number");
    const j = await post("charmNestLibrary", { op: "timelineGet", orderId: id, sandbox: cfg.sandbox });
    // events still in this page's outbox are part of the timeline too (they are on their way)
    const known = new Set((j.events || []).map(x => x.id && x.id.split("~").pop()));
    for (const ev of box) if (ev.orderId === id && !!ev.sandbox === !!cfg.sandbox && !known.has(ev.id)) (j.events = j.events || []).push(Object.assign({ pending: true }, ev));
    (j.events || []).sort((a, b) => a.at - b.at);
    return j;
  }
  async function cancelCheck(ids) {
    const list = (Array.isArray(ids) ? ids : [ids]).map(digits).filter(Boolean);
    if (!list.length) return { cancelled: {} };
    if (cfg.mode === "station") {
      const r = await fetch(base() + "firebaseOrders?cancelCheck=" + encodeURIComponent(list.join(",")) + (cfg.sandbox ? "&sandbox=1" : ""));
      const j = await r.json().catch(() => ({})); if (!r.ok || j.error) throw new Error(j.error || "HTTP " + r.status);
      return j;
    }
    return post("charmNestLibrary", { op: "cancelCheck", orderIds: list, sandbox: cfg.sandbox });
  }
  window.addEventListener("online", () => schedule(200));
  document.addEventListener("visibilitychange", () => { if (document.visibilityState === "hidden") flush(); });
  if (box.length) schedule(1500);
  window.OrderTimeline = {
    TYPES, STATION_TYPES,
    config(o) { Object.assign(cfg, o || {}); return Object.assign({}, cfg); },
    record, flush, get, cancelCheck,
    onRecord(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    pending() { return box.length; }
  };
})();
