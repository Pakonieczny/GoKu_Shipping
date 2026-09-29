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
  let box = [], timer = 0, due = 0, sending = false, backoff = 1000;
  /* One outbox for the whole site: the sorter and every station page (one origin), in as many tabs as are open. A save
     keeps what another page queued (only what this page delivered leaves), and a page sends only its own kind of event:
     a station's door keeps station types alone, so a sorter event sent through it was dropped and forgotten. */
  const keyOf = e => `${e && e.orderId}~${e && e.type}~${e && e.id}`, sent = new Set();
  const disk = () => { try { const j = JSON.parse(localStorage.getItem(OUTBOX) || "[]"); return Array.isArray(j) ? j.filter(e => e && typeof e === "object") : []; } catch (_) { return []; } };
  box = disk();
  const mine = ev => (ev.mode ? ev.mode === cfg.mode : cfg.mode !== "station" || STATION_TYPES.has(ev.type));
  // (another page's events stay on the disk alone, for that page: this one neither sends them nor writes them back)
  const save = () => { try { box = box.filter(mine); const have = new Set(box.map(keyOf)); localStorage.setItem(OUTBOX, JSON.stringify(disk().filter(e => !sent.has(keyOf(e)) && !have.has(keyOf(e))).concat(box).slice(-500))); } catch (_) {} };
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const base = () => (location.protocol === "file:" ? "https://goldenspike.app" : "") + "/.netlify/functions/";
  async function post(fn, body, sandbox = cfg.sandbox) {
    const headers = { "Content-Type": "application/json" };
    if (cfg.passcode) headers["X-Edit-Passcode"] = cfg.passcode;
    const url = base() + fn + (fn === "firebaseOrders" && sandbox ? "?sandbox=1" : "");
    // (a keepalive request may carry 64 KB at most: a bigger one is refused outright, and was retried forever. Bytes, not
    //  characters: 45,000 characters of Japanese or emoji are over 120 KB)
    const text = JSON.stringify(body), r = await fetch(url, { method: "POST", headers, body: text, keepalive: new Blob([text]).size < 60000 });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || j.error) throw new Error(j.error || "HTTP " + r.status);
    return j;
  }
  function schedule(ms) { clearTimeout(timer); due = Date.now() + ms; timer = setTimeout(() => { timer = 0; flush(); }, ms); }
  async function flush() {
    const first = sending ? null : box.find(mine); if (!first) return;
    sending = true;
    // one store per batch: an event keeps the store (production or sandbox) it was recorded in; ≤ 50 events, ≤ ~48 KB
    const sb = !!first.sandbox, batch = []; let bytes = 0;
    for (const ev of box) {
      if (!mine(ev) || !!ev.sandbox !== sb) continue;
      const n = JSON.stringify(ev).length; if (batch.length && (batch.length >= 50 || bytes + n > 48000)) break;
      batch.push(ev); bytes += n;
    }
    try {
      if (cfg.mode === "station") await post("firebaseOrders", { timeline: batch }, sb);
      else await post("charmNestLibrary", { op: "timelineAdd", events: batch, sandbox: sb }, sb);
      const done = new Set(batch); box = box.filter(ev => !done.has(ev)); batch.forEach(ev => sent.add(keyOf(ev))); save(); backoff = 1000;
      // (only the newest delivered keys can still be on the disk, which keeps 500: a long session's memory stays bounded)
      if (sent.size > 3000) { let n = sent.size - 2000; for (const k of sent) { if (n-- <= 0) break; sent.delete(k); } }
      if (box.some(mine)) schedule(50);
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
      // (an event that says who, even "" — nobody signed in at the station — keeps it: only an event with no by at all
      //  takes the configured person, so a seal never names someone who was not there)
      let ev = Object.assign({}, e, { orderId, at, by: e.by != null ? e.by : (cfg.by || ""), station: e.station || cfg.station || "", device: e.device || cfg.device || "", sandbox: !!cfg.sandbox, mode: cfg.mode });
      ev.id = String(e.id || `${at}-${Math.random().toString(36).slice(2, 8)}`);
      // kept as plain JSON: details that are not (a circular object) are left out here, where they stopped the outbox for good
      try { ev = JSON.parse(JSON.stringify(ev)); } catch (_) { ev.data = null; ev = JSON.parse(JSON.stringify(ev)); }
      box.push(ev); if (box.length > KEEP) box.splice(0, box.length - KEEP);   // (a server refusing for days: memory stays bounded)
      // (a send already due sooner is not put off: events a scanner records less than a second apart were never sent
      //  while it kept on, and past KEEP the oldest were dropped)
      save(); if (!timer || due > Date.now() + 900) schedule(900);
      for (const fn of listeners) { try { fn(ev); } catch (_) {} }
      return ev;
    } catch (_) { return null; }
  }
  async function get(orderId) {
    const id = digits(orderId); if (!id) throw new Error("no order number");
    const j = await post("charmNestLibrary", { op: "timelineGet", orderId: id, sandbox: cfg.sandbox });
    // events still in this page's outbox are part of the timeline too (they are on their way)
    // (by the server's own key, orderId~type~cleaned id: an id with a space or "/" is stored cleaned, and two types may share one)
    const known = new Set((j.events || []).map(x => x.id && x.id.split("~").slice(1).join("~")));
    const keyIn = ev => `${ev.type}~${String(ev.id).slice(0, 120).replace(/[^\w.:-]/g, "_")}`;
    for (const ev of box) if (ev.orderId === id && !!ev.sandbox === !!cfg.sandbox && !known.has(keyIn(ev))) (j.events = j.events || []).push(Object.assign({ pending: true }, ev));
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
    pending() { return box.length; },
    /** A sandbox reset (sandbox true): that store's events still waiting, this page's and every other page's on the disk,
        go with the records it cleared; sent afterwards they would land on the replay of the same real order numbers */
    discard(sandbox) { const keep = ev => !!ev.sandbox !== !!sandbox; box = box.filter(keep); try { localStorage.setItem(OUTBOX, JSON.stringify(disk().filter(keep))); } catch (_) {} return box.length; }
  };
})();
