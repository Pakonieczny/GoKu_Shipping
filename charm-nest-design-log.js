/*  charm-nest-design-log.js — who did each design step, and when (Design & Engrave, items 34 to 37 and D7; Paul, 28 Oct 2026: "all of this
 *  must be tracked by employee, and I need to know when employees are commissioning custom pieces, especially when they don't belong to an
 *  order that way we can correctly track our rates and who creates the errors and who is creating the solutions").
 *  ONE call every design action point makes (the Charm Editor, the design library, imports, traces, the Send to sheet pop-up):
 *
 *    CNDesignLog.event(kind, data)  → the event as queued (a copy), or null. Never throws, never waits, never touches the network itself.
 *    CNDesignLog.flush()            → Promise: hands what is queued over now (pagehide and a hidden page do it by themselves)
 *    CNDesignLog.pending()          → { events, records }: events not handed over yet, custom-piece records the server has not acknowledged
 *    CNDesignLog.onEvent(fn)        → fn(event) for every event logged on this page; returns the unsubscribe
 *    CNDesignLog.causeOf(orderId, { lineKey }) → Promise<{ suggested, why, candidates: [{ name, step, at }] }>   (D7: who made the original
 *                                     mistake, read from that order's own timeline: who approved its engraving, sent or added its design,
 *                                     approved it, placed or cut it. The pop-up shows the suggestion and lets the person change it.)
 *    CNDesignLog.KINDS · CNDesignLog.options = { quietMs, maxMs, retryMs, maxRetryMs }
 *
 *  kind: editorOpen · editorSave · designOpen · designSave · imageImport · imageTrace · vectorImport · customPieceSent · fixPiece (or another
 *        camelCase word of a workstream's own: counted as a design note, never refused)
 *  data (each optional): { orderId (the Etsy order; a CUSTOM-yyyymmdd-n id is read as customId), designId, version, name (the design's),
 *        sku, poolId, poolIds, faces, file, metal (gold | silver | rose | gold10k | gold14k, or GF SS RG 10K 14K), quantity, customId,
 *        customName, reason, sendId, fixes: { orderId, poolId, lineKey, causedBy, suggested }, by, timeline: false }
 *  `by` is the signed-in person (CNEmployee.name()) unless the caller says; the server stamps the time it is stored.
 *  ONE send of a custom piece is ONE event: customPieceSent, with `fixes` when it fixes a mistake (fixPiece is the same thing said
 *  directly). Both kinds for one send (the same sendId or first pool id) are one record.
 *
 *  Where an event goes. Events are handed over in bursts (1.5 s after the last one, 4 s at most after the first, and at once when the
 *  page is hidden or closed), each part through a door that already batches and keeps its own outbox:
 *    1 · employee efficiency: one `note` per event through CNAct (charm-nest-bridge.js → station-activity.js, sent every 10 s), under the
 *        signed-in person and their station (design, laser or the sorter). Its detail is a fixed phrase, "design: custom piece sent ·
 *        gold · no order", which netlify/functions/_activityKinds.js counts in that person's day (x_dopen, x_dsave, x_dimp, x_cust, x_custNo,
 *        x_fix): the Efficiency page reads them with the rollups it already reads, no extra read.
 *    2 · the order's timeline (OrderTimeline.record, timelineAdd), only when there is an Etsy order: a save is "designDropped" (Design added),
 *        a custom piece "designSent" (Sent to sheet), and a fix piece also on the order it fixes, naming who made the mistake.
 *    3 · custom pieces and fixes: one record each in Design_Log (charmNestLibrary op designLog; netlify/functions/_designLog.js), what the
 *        Efficiency page's Custom pieces view lists (op custom). Kept in localStorage (cn.designLog.outbox, ".sandbox" for the sandbox) until
 *        the server says 200, retried with back-off; the record's id is fixed when it is made, so a retry is stored once.
 *  Nothing here is ever a PIN: a digits-only name is no name. */
(function (root) {
  "use strict";
  if (root.CNDesignLog) return;
  const options = { quietMs: 1500, maxMs: 4000, retryMs: 4000, maxRetryMs: 120000, keepRecords: 200, keepRecent: 60 };
  const KINDS = Object.freeze(["editorOpen", "editorSave", "designOpen", "designSave", "imageImport", "imageTrace", "vectorImport", "customPieceSent", "fixPiece"]);
  const PHRASE = { editorOpen: "editor opened", editorSave: "editor saved", designOpen: "design opened", designSave: "design saved", imageImport: "image imported",
    imageTrace: "image traced", vectorImport: "vector imported", customPieceSent: "custom piece sent", fixPiece: "fix piece sent" };
  const METAL = { gold: "gold", silver: "silver", rose: "rose", gold10k: "gold10k", gold14k: "gold14k", gf: "gold", ss: "silver", rg: "rose", "10k": "gold10k", "14k": "gold14k" };
  const METAL_LABEL = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const OUTBOX = "cn.designLog.outbox";
  const warn = (...a) => { try { console.warn("[CNDesignLog]", ...a); } catch (_) {} };
  const text = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const idish = (v, n) => text(v, n).replace(/[^\w.:-]/g, "_");
  const CUSTOM_RE = /^CUSTOM-\d{8}-\d{1,4}$/;
  /** a person's name, never a PIN (no letter = no name) */
  const nameOf = v => { const s = text(v, 80); return /\p{L}/u.test(s) ? s : ""; };
  const whoNow = () => { try { return nameOf(root.CNEmployee && typeof root.CNEmployee.name === "function" ? root.CNEmployee.name() : ""); } catch (_) { return ""; } };
  /** design | laser | "" (the Admin's sorter): the station a timeline step is written under */
  const roleNow = () => { try { const w = root.StationSession && typeof root.StationSession.who === "function" ? root.StationSession.who() : null; return w && (w.role === "design" || w.role === "laser") ? w.role : ""; } catch (_) { return ""; } };
  const sandboxNow = () => {
    try { if (typeof WORKSPACE_SANDBOX !== "undefined") return !!WORKSPACE_SANDBOX; } catch (_) {}   // (the sorter page's own constant)
    try { if (root.CN && root.CN.S && root.CN.S.settings) return root.CN.S.settings.sandbox === "on"; } catch (_) {}
    try { return /[?&]sandbox=1\b/.test(root.location.search); } catch (_) { return false; }
  };
  const ls = {
    get(k) { try { const v = JSON.parse(root.localStorage.getItem(k) || "null"); return Array.isArray(v) ? v : []; } catch (_) { return []; } },
    set(k, list) { try { if (list.length) root.localStorage.setItem(k, JSON.stringify(list)); else root.localStorage.removeItem(k); } catch (_) {} }
  };
  const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 8);

  /* ── one event, as kept ── */
  function norm(kind, data) {
    const d = data && typeof data === "object" ? data : {};
    const k = typeof kind === "string" && /^[a-z][A-Za-z0-9]{1,39}$/.test(kind) ? kind : "";
    if (!k) return null;
    const rawOrder = text(d.orderId, 40), at = Date.now();
    const ev = { kind: k, at, by: nameOf(d.by) || whoNow(), station: roleNow() || "sorter", sandbox: sandboxNow() };
    if (CUSTOM_RE.test(rawOrder)) ev.customId = rawOrder; else if (digits(rawOrder).length >= 9) ev.orderId = digits(rawOrder);   // (an Etsy receipt is 9+ digits; 4 to 8 could be a PIN)
    if (!ev.customId && CUSTOM_RE.test(text(d.customId, 24))) ev.customId = text(d.customId, 24);
    for (const [key, n] of [["designId", 80], ["poolId", 60], ["sendId", 60]]) { const v = idish(d[key], n); if (v) ev[key] = v; }
    const v = Math.floor(Number(d.version)); if (v > 0 && v < 1e6) ev.version = v;
    for (const [key, n] of [["name", 80], ["sku", 60], ["file", 120], ["customName", 80], ["reason", 200]]) { const s = text(d[key], n); if (s) ev[key] = s; }
    const m = METAL[String(d.metal == null ? "" : d.metal).trim().toLowerCase()]; if (m) ev.metal = m;
    const q = Math.floor(Number(d.quantity)); if (q > 0) ev.quantity = Math.min(q, 500);
    if (Array.isArray(d.poolIds)) { const p = d.poolIds.map(x => idish(x, 60)).filter(Boolean).slice(0, 50); if (p.length) ev.poolIds = p; }
    if (Array.isArray(d.faces)) { const f = d.faces.filter(x => x === "front" || x === "back"); if (f.length) ev.faces = [...new Set(f)]; }
    const fx = d.fixes && typeof d.fixes === "object" ? d.fixes : null;
    if (fx || k === "fixPiece") {
      const f = {}, fo = text(fx && fx.orderId, 40);
      if (CUSTOM_RE.test(fo)) f.customId = fo; else if (digits(fo).length >= 9) f.orderId = digits(fo);
      for (const [key, n] of [["poolId", 60], ["lineKey", 80]]) { const s = idish(fx && fx[key], n); if (s) f[key] = s; }
      const cb = nameOf(fx && fx.causedBy), sg = nameOf(fx && fx.suggested); if (cb) f.causedBy = cb; if (sg) f.suggested = sg;
      const why = text(fx && fx.why, 120); if (why) f.why = why;
      ev.fix = f;
    }
    if (d.timeline === false) ev.timeline = false;
    ev.custom = k === "customPieceSent" || k === "fixPiece";
    ev.id = `${k}-${uid()}`;
    // ONE record per send: both kinds of one send (its sendId, else its first pool id) land on the same document
    if (ev.custom) ev.rid = "dl-" + (ev.sendId || (ev.poolIds && ev.poolIds[0]) || ev.poolId || uid()).slice(0, 90);
    return ev;
  }
  /** "design: custom piece sent · gold · no order": the fixed words _activityKinds.js counts (never a number: a detail is scrubbed of PIN-like digits) */
  function phraseOf(ev) {
    const base = PHRASE[ev.kind] || ev.kind.replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase();
    return ("design: " + base + (ev.custom && ev.metal ? " · " + ev.metal : "") + (ev.custom && !ev.orderId ? " · no order" : "") + (ev.kind === "customPieceSent" && ev.fix ? " · fix" : "")).slice(0, 120);
  }
  const pieceWords = ev => `${ev.name || ev.customName || ev.sku || "custom piece"}${ev.version ? " v" + ev.version : ""}${ev.quantity > 1 ? " × " + ev.quantity : ""}`;

  /* ── the queue: events wait here for their burst to end ── */
  let queue = [], timer = 0, firstAt = 0;
  const listeners = new Set(), recent = [];
  function schedule() {
    const now = Date.now(); if (!firstAt) firstAt = now;
    const wait = Math.max(0, Math.min(options.quietMs, firstAt + options.maxMs - now));
    clearTimeout(timer); timer = setTimeout(() => { timer = 0; handOver(false); }, wait);
  }
  function event(kind, data) {
    try {
      const ev = norm(kind, data); if (!ev) return null;
      queue.push(ev);
      recent.push(ev); if (recent.length > options.keepRecent) recent.shift();
      schedule();
      const copy = JSON.parse(JSON.stringify(ev));
      for (const fn of listeners) { try { fn(copy); } catch (_) {} }
      return copy;
    } catch (e) { warn("event:", e && e.message); return null; }
  }

  /* ── the hand-over: efficiency, the timeline, the records ── */
  function toActivity(ev) {
    try {
      const A = root.CNAct; if (typeof A !== "function") return false;
      const o = { detail: phraseOf(ev) };
      if (ev.orderId) o.orderId = ev.orderId;
      if (ev.designId) o.line = ev.designId.slice(0, 40);
      if (ev.sku) o.sku = ev.sku;
      return !!A("note", o);
    } catch (_) { return false; }
  }
  function toTimeline(list) {
    const T = root.OrderTimeline; if (!T || typeof T.record !== "function") return;
    const out = new Map();     // (one step per order and kind of step in a burst: the editor's own save and the library's version of it are ONE "Design added")
    for (const ev of list) {
      if (ev.timeline === false) continue;
      const base = { by: ev.by, station: ev.station, at: ev.at };
      if (ev.orderId && (ev.kind === "editorSave" || ev.kind === "designSave")) {
        const key = ev.orderId + "|save", had = out.get(key);
        if (had && had.src === "designSave" && ev.kind === "editorSave") continue;
        const faces = ev.faces ? ev.faces.join(" and ") : "";
        out.set(key, { src: ev.kind, ev: Object.assign({}, base, { orderId: ev.orderId, type: "designDropped", id: "dl." + ev.id,
          text: (ev.designId ? `Design saved in the Charm Editor: ${pieceWords(ev)}` : `Artwork edited in the Charm Editor${faces ? " (" + faces + ")" : ""}${ev.sku ? ": " + ev.sku : ""}`).slice(0, 200),
          data: { designLog: ev.kind, designId: ev.designId || null, version: ev.version || null, faces: ev.faces || null, poolId: ev.poolId || null } }) });
      }
      if (ev.custom && ev.orderId) {
        out.set(ev.orderId + "|sent|" + ev.rid, { ev: Object.assign({}, base, { orderId: ev.orderId, type: "designSent", id: "dl." + ev.rid,
          text: `Custom piece sent to the ${METAL_LABEL[ev.metal] || "next"} sheet by ${ev.by || "someone"}: ${pieceWords(ev)}${ev.fix ? " · a fix" : ""}`.slice(0, 200),
          data: { designLog: ev.kind, designId: ev.designId || null, version: ev.version || null, metal: ev.metal || null, quantity: ev.quantity || 1, poolIds: ev.poolIds || null, handmadeCustom: true } }) });
      }
      // the order a fix piece fixes: who sent the fix, and who the mistake was put down to (D7)
      if (ev.custom && ev.fix && ev.fix.orderId && ev.fix.orderId !== ev.orderId) {
        const f = ev.fix, piece = ev.orderId ? `order ${ev.orderId}` : `${ev.customId || "no order"}${ev.customName ? ` “${ev.customName}”` : ""}`;
        out.set(f.orderId + "|fix|" + ev.rid, { ev: Object.assign({}, base, { orderId: f.orderId, type: "designSent", id: "dl.fix." + ev.rid, lineKey: f.lineKey || undefined,
          text: `Fix piece sent to the ${METAL_LABEL[ev.metal] || "next"} sheet by ${ev.by || "someone"} (${piece})${f.causedBy ? ` · mistake by ${f.causedBy}` : ""}`.slice(0, 200),
          data: { designLog: "fixPiece", fix: true, fixedBy: ev.by || null, causedBy: f.causedBy || null, suggested: f.suggested || null, poolId: f.poolId || null, customId: ev.customId || null, customName: ev.customName || null, reason: ev.reason || null, metal: ev.metal || null } }) });
      }
    }
    for (const x of out.values()) { try { T.record(x.ev); } catch (_) {} }
  }
  /** the Design_Log record of a custom piece (what the Custom pieces view lists); the server stamps its time */
  function recordOf(ev) {
    const r = { id: ev.rid, kind: ev.fix ? "fixPiece" : "customPieceSent", by: ev.by, station: ev.station, clientAt: ev.at, sandbox: ev.sandbox };
    for (const k of ["orderId", "customId", "customName", "reason", "designId", "version", "name", "sku", "metal", "quantity", "poolIds", "poolId", "sendId"]) if (ev[k] != null) r[k] = ev[k];
    if (ev.fix) r.fix = ev.fix;
    return r;
  }
  const boxKey = sb => OUTBOX + (sb ? ".sandbox" : "");      // (".sandbox" is the sandbox's mark: a sandbox wipe clears it, charm-nest-sandbox-families.js)
  let records = ls.get(boxKey(sandboxNow()));
  function keep(list) {
    for (const r of list) { const i = records.findIndex(x => x.id === r.id); if (i < 0) records.push(r); else records[i] = Object.assign({}, records[i], r, r.fix || records[i].fix ? { fix: Object.assign({}, records[i].fix, r.fix), kind: "fixPiece" } : {}); }
    if (records.length > options.keepRecords) records = records.slice(-options.keepRecords);
    ls.set(boxKey(sandboxNow()), records);
  }
  function handOver(final) {
    clearTimeout(timer); timer = 0; firstAt = 0;
    const list = queue; queue = [];
    if (list.length) {
      for (const ev of list) toActivity(ev);
      toTimeline(list);
      const recs = list.filter(ev => ev.custom).map(recordOf);
      if (recs.length) keep(recs);
    }
    if (final) {   // (the page is going: the two doors' own outboxes hold what was just handed over; ask them to send it now)
      try { root.StationActivity && typeof root.StationActivity.flush === "function" && root.StationActivity.flush(true); } catch (_) {}
      try { root.OrderTimeline && typeof root.OrderTimeline.flush === "function" && root.OrderTimeline.flush(); } catch (_) {}
    }
    return send(final);
  }

  /* ── the records' door: charmNestLibrary op designLog ── */
  let sending = null, backoff = options.retryMs, retryTimer = 0;
  function post(body, final) {
    const sb = !!body.sandbox;
    if (!final && root.CN && typeof root.CN.api === "function" && sb === sandboxNow()) return root.CN.api("charmNestLibrary", body, { quiet: true });   // (the page's own door: its passcode, the sandbox tag, the reset's guard)
    const headers = { "Content-Type": "application/json" };
    try { const p = root.CN && root.CN.S && root.CN.S.passcode; if (p) headers["X-Edit-Passcode"] = p; } catch (_) {}
    const base = (root.location && root.location.protocol === "file:" ? "https://goldenspike.app" : "") + "/.netlify/functions/charmNestLibrary";
    const t = JSON.stringify(body);
    return fetch(base, { method: "POST", headers, body: t, keepalive: final && t.length < 60000 }).then(r => r.json().catch(() => ({})).then(j => { if (!r.ok || (j && j.error)) throw new Error((j && j.error) || "HTTP " + r.status); return j; }));
  }
  function send(final) {
    if (sending) return sending;
    try { if (root.CNWipe && root.CNWipe.active) return Promise.resolve(); } catch (_) {}
    const sb = sandboxNow(), batch = records.filter(r => !!r.sandbox === sb).slice(0, 50);
    if (!batch.length) return Promise.resolve();
    clearTimeout(retryTimer); retryTimer = 0;
    sending = Promise.resolve().then(() => post({ op: "designLog", records: batch, sandbox: sb }, final)).then(() => {
      const done = new Set(batch.map(r => r.id + "|" + JSON.stringify(r)));
      records = records.filter(r => !done.has(r.id + "|" + JSON.stringify(r)));      // (a record changed while it was on its way, a fix merged into it, stays for the next send)
      ls.set(boxKey(sb), records); backoff = options.retryMs;
      if (records.some(r => !!r.sandbox === sb)) retryTimer = setTimeout(() => send(false), 50);
    }, e => {
      if (!final) { retryTimer = setTimeout(() => send(false), backoff); backoff = Math.min(backoff * 2, options.maxRetryMs); }
      warn("custom piece records not sent yet, retrying:", e && e.message);
    }).finally(() => { sending = null; });
    return sending;
  }

  /* ── D7: who made the mistake a fix piece fixes, from the order's own timeline ── */
  const BLAME = [   // (the step, in the order a mistake is most likely made there: the engraving or design a person approved first)
    ["engraveApproved", "approved the engraving"], ["monogramApproved", "approved the monogram"], ["engraveChanged", "changed the engraving"], ["designSent", "sent the design to the sheet"],
    ["designDropped", "added the design"], ["customDecided", "decided the custom order"], ["decided", "approved the order"], ["placed", "placed it on the sheet"], ["laserDone", "cut it on the laser"],
    ["sorted", "sorted it"], ["welded", "welded it"], ["assembled", "assembled it"], ["packed", "packed it"]];
  const NOBODY = /^(|system|etsy|operator|someone|unknown|unattributed)$/i;
  async function causeOf(orderId, o) {
    o = o || {};
    const id = digits(orderId), T = root.OrderTimeline;
    if (!id || !T || typeof T.get !== "function") return { suggested: "", why: "", candidates: [] };
    const j = await T.get(id);
    const rank = new Map(BLAME.map(([t], i) => [t, i])), said = new Map(BLAME);
    const evs = (j && Array.isArray(j.events) ? j.events : []).filter(e => e && rank.has(e.type) && nameOf(e.by) && !NOBODY.test(String(e.by).trim()) && !(e.data && e.data.fix) && (!o.lineKey || !e.lineKey || e.lineKey === o.lineKey));
    evs.sort((a, b) => rank.get(a.type) - rank.get(b.type) || (Number(b.at) || 0) - (Number(a.at) || 0));
    const seen = new Set(), candidates = [];
    for (const e of evs) { const k = nameOf(e.by) + "|" + e.type; if (seen.has(k)) continue; seen.add(k); candidates.push({ name: nameOf(e.by), step: said.get(e.type), type: e.type, at: Number(e.at) || 0 }); }
    const top = candidates[0];
    return { suggested: top ? top.name : "", why: top ? `${top.name} ${top.step}` : "", candidates: candidates.slice(0, 12) };
  }

  /* ── the page going away: what is queued is handed over and sent at once ── */
  try {
    root.addEventListener("pagehide", () => { handOver(true); });
    root.document && root.document.addEventListener("visibilitychange", () => { if (root.document.visibilityState === "hidden") handOver(true); });
    root.addEventListener("online", () => send(false));
    // what an earlier visit left unsent goes a few seconds after the page has settled (never during its load)
    if (records.length) retryTimer = setTimeout(() => send(false), 5000);
  } catch (_) {}

  root.CNDesignLog = {
    KINDS, options, event,
    flush: () => handOver(false),
    pending: () => ({ events: queue.length, records: records.length }),
    onEvent(fn) { if (typeof fn === "function") listeners.add(fn); return () => listeners.delete(fn); },
    recent: () => recent.map(e => JSON.parse(JSON.stringify(e))),
    causeOf,
    _t: { norm, phraseOf, recordOf, toTimeline, boxKey }
  };
})(typeof window !== "undefined" ? window : globalThis);
