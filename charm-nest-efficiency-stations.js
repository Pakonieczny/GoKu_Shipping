/*  charm-nest-efficiency-stations.js — the live stations board and the shared "current order" card of the Employee efficiency console
 *  (Paul, 5 Oct 2026: "I should be able to see all the stations in a list with visuals, showing me the current order that each station is
 *  processing, including the thumbnails for that order and the QR code for that order, the person who is working on it, how long it has been
 *  since the order got scanned ... a real time order list ... a barcode thumbnail for the order image/vector image file, and one thumbnail
 *  for each piece in a multi piece order ... animations where warranted, beautiful transitions, real time data, full hover states and zooms
 *  into relevant places"; plans/employee-hr/plan.md, request 3, 6 and 7).
 *
 *    EfficiencyStations.mount(el, opts?)   the board: every station as a row with its state light, its people, today's counts, a last-hour
 *                                          sparkline when the answer carries one, and one order card per person at work. → { unmount, refresh }
 *    EfficiencyStations.orderCard(current) ONE order card, the same component on the board, in the Overview and on the employee page:
 *                                          order number, customer, person, station, a ticking "since scanned", the order's QR, its picture and
 *                                          one picture per piece of a multi-piece order; hovering a picture or the QR grows it in place (never
 *                                          full screen), a press opens the order the way every other list does (openOrderFrom). → element
 *                                          with .update(next), .finish(text)
 *    EfficiencyStations.options            { pollMs, zoomDelay, doneMs, source, ... }      EfficiencyStations.norm(answer)  the view model
 *
 *  What it reads: employeeEfficiency op "live" (plans/employee-hr/api.md, E2 section 4), POST { op:"live", key, sandbox? }; key = the console's
 *  manager passcode, held in sessionStorage (cn.eff.key) by the shell, never in a URL or a log. Answer: { ok, at, mode, stations:[{ key, label,
 *  state: working | idle | offline, people:[name | { name, ... }], devices:[{ device, label, state, person, since }], current:[{ id, person,
 *  device, deviceLabel, kind: order | sheet, rid, orderNumber, customer, title, scannedAt, beatAt, note, thumbUrl, vectorUrl, photoUrl, qr:{ text }
 *  | null, pieces:[{ id, label, sku, thumbUrl, vectorUrl, photoUrl }], pieceCount }], lastEventAt, counts:{ partsToday, ordersToday, scansToday },
 *  spark? (optional: parts per 5 minutes of the last hour, oldest first; drawn only when present) }], signedIn:[{ name, stationKey, since,
 *  lastSeenAt }] }. A missing field is simply not shown: nothing here is invented. A person's hover card also asks op "person" (range day,
 *  compare false; E4's kpis with their own label, unit and one-line definition) once the pointer has rested on it, and keeps the answer a minute.
 *  A laser sheet (kind "sheet") has a title and no QR: it is shown with its title, "since started", and nothing to open.
 *
 *  Live: one request about every 3 s, only while the board is on screen (in a tab that is shown, in a page that is in sight), none while
 *  hidden; the next one starts after the last one ended, backs off after a failure and says "Reconnecting". The timers are local: a card's
 *  "since scanned" ticks once a second from the order's scan time (server-corrected) without any request (the clock looks four times a
 *  second so a digit changes on its second, not up to a second late; it only writes when the text changes).
 *  Motion: a new order flies in from its station's light (Motion.flyIn), a finished one says "Done in 4 m 12 s" and lifts away (Motion.shut),
 *  people fade in and out, rows rise in once. Only transform, opacity and clip-path move; prefers-reduced-motion leaves everything still.  */
(function (root) {
  "use strict";
  const doc = root.document;
  if (!doc || root.EfficiencyStations) return;
  const TZ = "America/New_York", KEY_STORE = "cn.eff.key", EASE = "cubic-bezier(.2,.8,.2,1)";
  const options = { pollMs: 3000, maxBackoffMs: 15000, timeoutMs: 10000, tickMs: 250, zoomDelay: 500, zoomMs: 300, tipDwell: 250, flyMs: 760, doneMs: 2800, quietAfterMs: 20000, enterMs: 460, growMs: 480, endpoint: "", source: null };
  const N = v => { v = +v; return Number.isFinite(v) ? v : 0; };
  const T = v => { v = +v; return Number.isFinite(v) && v > 0 ? v : null; };
  const has = v => v !== null && v !== undefined && v !== "" && Number.isFinite(+v);
  const pad = n => String(n).padStart(2, "0");
  const low = s => String(s || "").trim().toLowerCase();
  const h = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
  const setText = (e, s) => { if (e && e.textContent !== s) e.textContent = s; };
  const still = () => { try { return root.Motion && root.Motion.reduced ? !!root.Motion.reduced() : !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches); } catch (_) { return false; } };
  const raf = f => (root.requestAnimationFrame ? root.requestAnimationFrame(f) : setTimeout(() => f(Date.now()), 16));
  const warn = (what, e) => { try { console.warn("Stations board: " + what, e); } catch (_) {} };
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));

  /* ── time: the server's clock, the New York shop clock, plain durations ── */
  const clk = { off: 0 };
  /** The server's clock now (inside the console, the console's own: one truth for every timer on the screen). */
  const now = () => { const E = root.Efficiency; try { if (E && E.api && typeof E.api.now === "function") { const t = E.api.now(); if (t > 1e12) return t; } } catch (_) {} return Date.now() + clk.off; };
  /** The server said it was `at` somewhere between the request leaving and the answer arriving: its middle is the best guess. */
  const sync = (at, mid) => { at = N(at); if (at > 1e12) clk.off = at - (mid || Date.now()); };
  const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hour: "numeric", minute: "2-digit" });
  const clock = t => clockFmt.format(new Date(t));
  /** The ticking timer: mm:ss under an hour, then "1 h 12 m". */
  function since(ms) { const s = Math.max(0, Math.floor(N(ms) / 1000)); if (s < 3600) return `${pad(Math.floor(s / 60))}:${pad(s % 60)}`; const m = Math.floor(s / 60); return `${Math.floor(m / 60)} h ${m % 60} m`; }
  /** Plain words: 42 s, 4 m 12 s, 1 h 12 m. */
  function words(ms) { const s = Math.max(0, Math.round(N(ms) / 1000)); if (s < 60) return `${s} s`; if (s < 3600) { const m = Math.floor(s / 60), r = s % 60; return r ? `${m} m ${r} s` : `${m} m`; } const m = Math.floor(s / 60), hh = Math.floor(m / 60); return m % 60 ? `${hh} h ${m % 60} m` : `${hh} h`; }
  const ago = s => (s < 2 ? "just now" : s < 60 ? `${Math.floor(s)}s ago` : s < 3600 ? `${Math.floor(s / 60)} m ago` : `${Math.floor(s / 3600)} h ago`);
  const nf = n => Math.round(N(n)).toLocaleString("en-US");
  const initials = name => { const w = String(name || "").trim().split(/[\s._-]+/).filter(Boolean); return w.length ? (w[0].charAt(0) + (w[1] ? w[1].charAt(0) : "")).toUpperCase() : "?"; };
  const tone = name => { let x = 0; for (const c of low(name)) x = (x * 31 + c.charCodeAt(0)) >>> 0; return x % 6; };
  const cap = s => (s ? s.charAt(0).toUpperCase() + s.slice(1) : "Station");

  /* ── the answer, normalised (a missing field is empty, never a crash, never a guess) ── */
  const str = v => (v == null ? "" : String(v));
  /** One order (or laser sheet) in somebody's hand. Inside the console E1's model leaves some fields out; its `raw` is the server's own entry, read first. */
  function normCurrent(c, st) {
    if (c && c._n) return c;   // already normalised
    c = c || {}; const r = c.raw && typeof c.raw === "object" ? c.raw : c;
    const rid = str(r.rid != null && r.rid !== "" ? r.rid : r.orderNumber), num = str(r.orderNumber || r.rid), kind = r.kind === "sheet" ? "sheet" : "order", title = str(r.title);
    const pieces = (Array.isArray(r.pieces) ? r.pieces : []).filter(Boolean).map((p, i) => ({ id: str(p.id), label: str(p.label), sku: str(p.sku), thumbUrl: str(p.thumbUrl), vectorUrl: str(p.vectorUrl), photoUrl: str(p.photoUrl), n: i + 1 }));
    const qr = kind === "sheet" ? "" : typeof r.qr === "string" ? r.qr : str((r.qr && r.qr.text) || num || rid);
    const person = str(c.person || r.person), sid = str(r.id);
    return { _n: 1, id: `${low(sid || person)}|${low(rid || title)}`, sid, kind, title, person, device: str(r.device), deviceLabel: str(r.deviceLabel), rid, orderNumber: num, customer: str(r.customer), scannedAt: T(r.scannedAt), beatAt: T(r.beatAt),
      thumbUrl: str(r.thumbUrl), photoUrl: str(r.photoUrl), vectorUrl: str(r.vectorUrl), qr, pieces, pieceCount: Math.max(pieces.length, Math.round(N(r.pieceCount))), note: r.note ? str(r.note) : "",
      station: str(c.station || r.station || (st && st.key)), stationLabel: str(c.stationLabel || r.stationLabel || (st && st.label)) };
  }
  const sparkOf = s => {
    const a = s.spark || s.lastHour || s.sparkline || s.hourly;
    const v = Array.isArray(a) ? a : a && Array.isArray(a.bins) ? a.bins : a && Array.isArray(a.parts) ? a.parts : null;
    const out = v ? v.map(x => N(x && typeof x === "object" ? (x.parts != null ? x.parts : x.n != null ? x.n : x.v) : x)) : null;
    return out && out.length >= 2 ? out : null;
  };
  function normPerson(p, sign) {
    const o = typeof p === "string" ? { name: p } : (p || {});
    const name = String(o.name || ""), g = sign.get(low(name)) || {};
    const pick = (...ks) => { for (const k of ks) if (has(o[k])) return N(o[k]); return null; };
    return { name, since: T(o.since != null ? o.since : g.since), lastSeenAt: T(o.lastSeenAt != null ? o.lastSeenAt : g.lastSeenAt),
      parts: pick("partsToday", "parts"), orders: pick("ordersToday", "orders"), medianMs: pick("medianOrderMs", "medianMs", "medianPerOrderMs"), longestIdleMs: pick("longestIdleMs", "maxIdleMs") };
  }
  function norm(r) {
    r = r || {};
    const signedIn = (Array.isArray(r.signedIn) ? r.signedIn : []).filter(x => x && x.name).map(x => ({ name: String(x.name), stationKey: String(x.stationKey || ""), since: T(x.since), lastSeenAt: T(x.lastSeenAt) }));
    const sign = new Map(signedIn.map(x => [low(x.name), x]));
    const stations = (Array.isArray(r.stations) ? r.stations : []).filter(s => s && (s.key || s.label)).map(s => {
      const key = String(s.key || low(s.label)), label = String(s.label || cap(key));
      const current = (Array.isArray(s.current) ? s.current : []).filter(c => c && (c.rid || c.orderNumber || (c.kind === "sheet" && c.title))).map(c => normCurrent(c, { key, label }));
      const people = (Array.isArray(s.people) ? s.people : []).map(p => normPerson(p, sign)).filter(p => p.name);
      for (const c of current) if (c.person && !people.some(p => low(p.name) === low(c.person))) people.push(normPerson(c.person, sign));
      const k = s.counts || {}, cnt = v => (has(v) ? N(v) : null);
      const devices = (Array.isArray(s.devices) ? s.devices : []).filter(d => d && (d.device || d.label)).map(d => ({ device: str(d.device), label: str(d.label || d.device), state: ["working", "idle", "offline"].includes(d.state) ? d.state : "offline", person: str(d.person), since: T(d.since) }));
      const state = ["working", "idle", "offline"].includes(s.state) ? s.state : current.length ? "working" : people.length ? "idle" : "offline";
      return { key, label, state, people, current, lastEventAt: T(s.lastEventAt), counts: { parts: cnt(k.partsToday != null ? k.partsToday : k.parts), orders: cnt(k.ordersToday != null ? k.ordersToday : k.orders), scans: cnt(k.scansToday != null ? k.scansToday : k.scans) }, devices, spark: sparkOf(s) };
    });
    return { at: T(r.at), mode: r.mode === "sandbox" ? "sandbox" : "real", stations, signedIn };
  }

  /* ── the QR code of an order: the app's own generator (lib/qrcode.min.js, the one the QR labels use), drawn once per text with a quiet zone ── */
  const qrCache = new Map();
  function qrUrl(text) {
    text = String(text || ""); if (!text) return "";
    if (qrCache.has(text)) return qrCache.get(text);
    let url = "";
    const Q = root.QRCode;
    if (Q && doc.body) {
      const holder = h("div"); holder.style.cssText = "position:fixed;left:-9999px;top:0;width:0;height:0;overflow:hidden;opacity:0;pointer-events:none"; holder.setAttribute("aria-hidden", "true"); doc.body.appendChild(holder);
      try {
        const make = lvl => { holder.textContent = ""; return new Q(holder, { text, width: 176, height: 176, correctLevel: lvl }); };
        try { make(Q.CorrectLevel.M); } catch (_) { make(Q.CorrectLevel.L); }
        const cv = holder.querySelector("canvas"), im = holder.querySelector("img");
        if (cv && cv.width) {
          const out = h("canvas"), padPx = Math.round(cv.width * .09); out.width = out.height = cv.width + padPx * 2;
          const g = out.getContext("2d"); g.fillStyle = "#fff"; g.fillRect(0, 0, out.width, out.height); g.drawImage(cv, padPx, padPx); url = out.toDataURL("image/png");
        } else if (im && /^data:/.test(im.src || "")) url = im.src;
      } catch (e) { warn("QR", e); } finally { holder.remove(); }
    }
    if (url) { if (qrCache.size > 300) qrCache.delete(qrCache.keys().next().value); qrCache.set(text, url); }
    return url;
  }

  /* ── pictures: the order's stored image address, else the piece's vector design (PieceMedia, the order window's own renderer), else a calm placeholder ── */
  const vecCache = new Map();   // design key → Promise<address>
  function rowFor(rid, pool) {   // the same lookup as the piece dots' (charm-nest-piece-dots.js), kept small
    const line = String(pool || "").replace(/_\d+$/, "");
    let rows = null; try { rows = root.Orders && typeof root.Orders.rows === "function" ? root.Orders.rows() : null; } catch (_) {}
    if (Array.isArray(rows)) { const r = rows.find(x => x && ((line && String(x.key) === line) || (pool && (x.poolIds || []).includes(pool)))); if (r) return r; }
    const OP = root.OrderPieces; if (!OP || typeof OP.of !== "function") return null;
    let ps = []; try { ps = OP.of(rid) || []; } catch (_) {}
    const p = pool ? ps.find(x => x.key === pool) || ps.find(x => x.lineKey === line) : ps[0];
    if (!p || p.hand || p.noDesign) return null;
    let pr = null; try { pr = root.B && root.B.pool && root.B.pool.rows && root.B.pool.rows.get(p.key); } catch (_) {}
    return { key: p.lineKey, order: { receiptId: rid }, line: { sku: p.sku, listingId: p.listingId }, spec: { designSku: p.sku, size: (pr && pr.size) || null, noDesign: false }, poolIds: [p.key] };
  }
  function vectorOf(rid, pool) {
    const PM = root.PieceMedia; if (!PM || typeof PM.vectorThumb !== "function") return Promise.resolve("");
    let row = null; try { row = rowFor(String(rid || "").replace(/\D/g, ""), pool); } catch (_) {}
    if (!row) return Promise.resolve("");
    let key = ""; try { key = PM.vectorKey ? PM.vectorKey(row) : ""; } catch (_) {}
    if (!key) return Promise.resolve("");
    if (!vecCache.has(key)) { if (vecCache.size > 200) vecCache.delete(vecCache.keys().next().value); vecCache.set(key, Promise.resolve().then(() => PM.vectorThumb(row)).then(u => String(u || ""), () => "")); }
    return vecCache.get(key).then(u => { if (!u) vecCache.delete(key); return u; });
  }
  const PH_ICON = '<svg viewBox="0 0 24 24" width="40%" height="40%" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3.5" y="4.5" width="17" height="15" rx="2.5"/><circle cx="9" cy="10" r="1.6"/><path d="M4 17l5-4.5 3.5 3L16 12l4 4"/></svg>';
  const PH_SHEET = '<svg viewBox="0 0 24 24" width="44%" height="44%" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3.5" y="3.5" width="17" height="17" rx="2"/><circle cx="9" cy="9" r="2"/><path d="M13 8h4M7 14h10M7 17.5h6"/></svg>';
  /** The addresses to try for one picture, in order, each with how it sits in its box (a vector design is drawn whole, a photo fills the box). */
  function urlsOf(o, piece) {
    const out = [], add = (u, fit) => { if (u && !out.some(x => x.u === u)) out.push({ u, fit }); };
    add(o.thumbUrl, o.thumbUrl && o.thumbUrl === o.vectorUrl ? "contain" : "cover");
    if (piece) { add(o.vectorUrl, "contain"); add(o.photoUrl, "cover"); } else { add(o.photoUrl, "cover"); add(o.vectorUrl, "contain"); }
    return out;
  }
  /** One picture box: set({ urls | url, rid, pool, fit, label, icon }) shows the first address that loads, then the vector design (PieceMedia), then a placeholder; a stale answer never lands. */
  function pictureBox(cls, zoomSize) {
    const box = h("div", cls); box.dataset.zoom = zoomSize; box.tabIndex = 0; box.setAttribute("role", "img");
    let tok = 0, sig = "", ctx = null;
    const ph = why => { const p = h("span", "esPh"); p.innerHTML = ctx && ctx.icon === "sheet" ? PH_SHEET : PH_ICON; p.title = why || (ctx && ctx.icon === "sheet" ? "A laser sheet" : "No picture yet"); p.dataset.ph = "1"; box.replaceChildren(p); box.dataset.state = "none"; };
    const show = (i, my) => {
      const it = ctx.urls[i], im = new root.Image(); im.alt = ""; im.decoding = "async"; im.draggable = false; im.referrerPolicy = "no-referrer"; im.className = "esImg " + (it.fit || "cover");
      im.onload = () => { if (my !== tok) return; box.replaceChildren(im); box.dataset.state = "ready"; raf(() => im.classList.add("on")); };
      im.onerror = () => { if (my !== tok) return; if (i + 1 < ctx.urls.length) show(i + 1, my); else vector(my, true); };
      im.src = it.u;
    };
    const vector = (my, afterFail) => {
      if (my !== tok) return;
      if (ctx.vector) {
        vectorOf(ctx.rid, ctx.pool).then(u => { if (my !== tok) return; if (!u) return ph(afterFail ? "The picture could not be loaded" : "No picture yet"); const im = new root.Image(); im.alt = ""; im.decoding = "async"; im.draggable = false; im.className = "esImg contain"; im.onload = () => { if (my !== tok) return; box.replaceChildren(im); box.dataset.state = "ready"; raf(() => im.classList.add("on")); }; im.onerror = () => { if (my === tok) ph("The picture could not be loaded"); }; im.src = u; });
      } else ph(afterFail ? "The picture could not be loaded" : "No picture yet");
    };
    box.set = o => {
      const urls = (o.urls || (o.url ? [{ u: o.url, fit: o.fit }] : [])).filter(x => x && x.u);
      const s = JSON.stringify([urls.map(x => x.u + "|" + x.fit), o.rid || "", o.pool || "", o.vector !== false, o.icon || ""]); if (s === sig) return; sig = s; const my = ++tok;
      ctx = { urls, rid: o.rid, pool: o.pool, vector: o.vector !== false && !o.icon, icon: o.icon || "" };
      if (o.label) box.setAttribute("aria-label", o.label);
      if (!box.firstChild) box.dataset.state = "wait";
      if (urls.length) show(0, my); else vector(my, false);
    };
    return box;
  }

  /* ── zoom in place (the platform's seal zoom, for pictures): a resting pointer grows it where it stands, lifted on a soft shadow, kept in view; never full screen ── */
  const Z = { node: null, timer: 0 };
  function bounds() {
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, bar = doc.querySelector(".topbar"), q = bar && bar.getClientRects().length ? bar.getBoundingClientRect() : null;
    return { l: 8, t: Math.max(8, q && q.bottom < vh / 2 ? q.bottom + 8 : 0), r: vw - 8, b: vh - 8 };
  }
  function boundsOf(node) {   // inside the screen, under the top bar, and inside the scrolling box that holds it
    const B = bounds();
    for (let a = node.parentElement; a && a !== doc.body; a = a.parentElement) {
      const o = getComputedStyle(a).overflowY; if (o === "auto" || o === "scroll") { const r = a.getBoundingClientRect(); B.l = Math.max(B.l, r.left + 4); B.t = Math.max(B.t, r.top + 4); B.r = Math.min(B.r, r.right - 4); B.b = Math.min(B.b, r.bottom - 4); break; }
    }
    return B;
  }
  function zoomIn(node) {
    clearTimeout(Z.timer); Z.timer = 0;
    if (!node || !node.isConnected || Z.node === node) return;
    if (Z.node) zoomOut(Z.node, true);
    const r = node.getBoundingClientRect(); if (!r.width || !r.height) return;
    Z.node = node; node.classList.add("esUp", "esLit");
    if (still() || !node.animate) return;
    const size = +node.dataset.zoom || 168, k = clamp(size / r.width, 1, 3.6), w = r.width * k, hh = r.height * k, cx = r.left + r.width / 2, cy = r.top + r.height / 2, B = boundsOf(node);
    const L = clamp(cx - w / 2, B.l, Math.max(B.l, B.r - w)), Tp = clamp(cy - hh / 2, B.t, Math.max(B.t, B.b - hh));
    node._zTo = `translate(${(L + w / 2 - cx).toFixed(1)}px,${(Tp + hh / 2 - cy).toFixed(1)}px) scale(${k.toFixed(3)})`;
    node._zAnim = node.animate([{ transform: "none" }, { transform: node._zTo }], { duration: options.zoomMs, easing: "cubic-bezier(.2,.9,.25,1.1)", fill: "forwards" });
  }
  function zoomOut(node, instant) {
    if (!node) return; clearTimeout(Z.timer); Z.timer = 0;
    if (Z.node === node) Z.node = null;
    node.classList.remove("esLit");
    const a = node._zAnim; node._zAnim = null;
    const done = () => { if (Z.node !== node) node.classList.remove("esUp"); };
    if (!a || instant || still() || !node.animate) { if (a) { try { a.cancel(); } catch (_) {} } return done(); }
    let cur = "none"; try { cur = getComputedStyle(node).transform; } catch (_) {}
    try { a.cancel(); } catch (_) {}
    const b = node.animate([{ transform: cur && cur !== "none" ? cur : node._zTo || "none" }, { transform: "none" }], { duration: 230, easing: "cubic-bezier(.3,0,.2,1)" });
    b.finished.then(done, done);
  }
  function zoomBind(node) {
    node.addEventListener("pointerenter", ev => { if (ev.pointerType === "touch") return; clearTimeout(Z.timer); Z.timer = setTimeout(() => zoomIn(node), options.zoomDelay); });
    node.addEventListener("pointerleave", ev => { if (ev.pointerType === "touch") return; if (Z.node === node) zoomOut(node); else { clearTimeout(Z.timer); Z.timer = 0; } });   // (a lifted finger "leaves": the grown picture stays until a tap elsewhere)
    node.addEventListener("focus", () => { let fv = true; try { fv = node.matches(":focus-visible"); } catch (_) {} if (fv) zoomIn(node); });
    node.addEventListener("blur", () => zoomOut(node));
  }

  /* ── the hover card: one floating card for every station light and every person ── */
  const Tp = { el: null, node: null, timer: 0, lazy: 0, wired: false };
  function tipSpec(spec) {
    const t = Tp.el; t.textContent = "";
    const head = t.appendChild(h("div", "esTipH"));
    if (spec.avatar) { const a = head.appendChild(h("span", "esAv big", initials(spec.avatar))); a.dataset.t = tone(spec.avatar); } else if (spec.state) { const l = head.appendChild(h("i", "esLight")); l.dataset.state = spec.state; }
    const ti = head.appendChild(h("div", "esTipT")); ti.appendChild(h("b", "", spec.title)); if (spec.sub) ti.appendChild(h("span", "", spec.sub));
    if (spec.rows && spec.rows.length) {
      const dl = t.appendChild(h("dl", "esTipR"));
      for (const r of spec.rows) { const row = dl.appendChild(h("div")); row.appendChild(h("dt", "", r.k)); row.appendChild(h("dd", "", r.v)); if (r.d) row.appendChild(h("small", "", r.d)); }
    }
    if (spec.wait) { const w = t.appendChild(h("p", "esTipW")); w.setAttribute("role", "status"); w.append(h("span", "esSpin"), h("span", "", spec.wait)); }
    if (spec.note) t.appendChild(h("p", "esTipN", spec.note));
    if (spec.spark) { const w = t.appendChild(h("div", "esTipS")); w.appendChild(sparkSvg(spec.spark, 220, 38)); w.appendChild(h("small", "", spec.sparkNote || "Last hour")); }
    if (spec.foot) t.appendChild(h("p", "esTipF", spec.foot));
  }
  function tipPlace(node) {
    const t = Tp.el, b = node.getBoundingClientRect(), vw = root.innerWidth || 1200, B = bounds();
    t.style.left = "0px"; t.style.top = "0px";
    const w = t.offsetWidth, hh = t.offsetHeight, cx = (b.left + b.right) / 2, above = b.top - 10 - B.t, below = B.b - (b.bottom + 10), up = above >= hh || (below < hh && above >= below);
    const y = clamp(up ? b.top - 10 - hh : b.bottom + 10, B.t, Math.max(B.t, B.b - hh)), x = clamp(cx - w / 2, 8, Math.max(8, vw - w - 8));
    t.style.left = Math.round(x) + "px"; t.style.top = Math.round(y) + "px"; t.style.setProperty("--ax", clamp(cx - x, 16, Math.max(16, w - 16)) + "px"); t.classList.toggle("below", !up);
  }
  function tipShow(node) {
    let spec = null; try { spec = node._esTip && node._esTip(); } catch (e) { warn("hover card", e); }
    if (!spec) return;
    clearTimeout(Tp.timer);
    if (!Tp.el) { Tp.el = h("div", "esTip"); Tp.el.setAttribute("aria-hidden", "true"); }
    const layer = node.closest("dialog[open]") || doc.body; if (Tp.el.parentNode !== layer) layer.appendChild(Tp.el);
    const was = Tp.node; Tp.node = node; tipSpec(spec); tipPlace(node);
    Tp.el.setAttribute("data-on", "");
    // a card that needs a read (what a person did today) waits a moment for the pointer to rest, so a pointer passing over asks for nothing
    clearTimeout(Tp.lazy); Tp.lazy = 0;
    if (spec.lazy) { const nd = node, again = () => { if (Tp.node === nd) tipRefresh(); }; Tp.lazy = setTimeout(() => { Tp.lazy = 0; if (Tp.node !== nd) return; let p = null; try { p = spec.lazy(); } catch (_) {} Promise.resolve(p).then(again, again); }, options.tipDwell); }
    if (!was && !still() && Tp.el.animate) Tp.el.animate([{ transform: "translateY(3px)", opacity: 0 }, { transform: "none", opacity: 1 }], { duration: 140, easing: "ease-out" });
  }
  function tipHide(wait) {
    clearTimeout(Tp.timer);
    clearTimeout(Tp.lazy); Tp.lazy = 0;   // a pointer that has gone asks for nothing
    const go = () => { Tp.node = null; if (Tp.el) Tp.el.removeAttribute("data-on"); };
    if (wait) Tp.timer = setTimeout(go, 70); else go();
  }
  /** After an update: the card that is showing reads the new numbers. */
  function tipRefresh() { if (!Tp.node) return; if (!Tp.node.isConnected) return tipHide(); let spec = null; try { spec = Tp.node._esTip(); } catch (_) {} if (spec) { tipSpec(spec); tipPlace(Tp.node); } else tipHide(); }
  const tipNode = n => (n && n.closest ? n.closest("[data-es-tip]") : null);
  function wire() {
    if (Tp.wired) return; Tp.wired = true;
    doc.addEventListener("pointerover", ev => { if (ev.pointerType === "touch") return; const n = tipNode(ev.target); if (n && n !== Tp.node) tipShow(n); else if (n) clearTimeout(Tp.timer); }, true);
    doc.addEventListener("pointerout", ev => { if (!Tp.node || ev.pointerType === "touch") return; const n = tipNode(ev.target); if (!n || n !== Tp.node) return; if (tipNode(ev.relatedTarget) === n) return; tipHide(true); }, true);
    doc.addEventListener("focusin", ev => { const n = tipNode(ev.target); if (n) tipShow(n); }, true);
    doc.addEventListener("focusout", ev => { if (Tp.node && tipNode(ev.target) === Tp.node) tipHide(); }, true);
    doc.addEventListener("pointerdown", ev => {
      const n = tipNode(ev.target);
      if (ev.pointerType === "touch" && n) { if (n === Tp.node) tipHide(); else tipShow(n); } else if (Tp.node && !n) tipHide();
      if (Z.node && !Z.node.contains(ev.target)) zoomOut(Z.node);
    }, true);
    doc.addEventListener("keydown", ev => { if (ev.key !== "Escape") return; if (Z.node) zoomOut(Z.node); if (Tp.node) tipHide(); }, true);
    doc.addEventListener("scroll", () => { if (Tp.node) tipHide(); if (Z.node) zoomOut(Z.node, true); }, { capture: true, passive: true });
    root.addEventListener("blur", () => { tipHide(); if (Z.node) zoomOut(Z.node, true); });
  }

  /* ── one ticker for every "since scanned" on screen (and the board's own "updated Ns ago"): a local clock, no request ── */
  const tk = { set: new Set(), timer: 0 };
  function tickRun() {
    const t = now();
    for (const e of tk.set) { if (!e.node.isConnected) { if (++e.miss > 120) tk.set.delete(e); continue; } e.miss = 0; try { e.run(t); } catch (_) {} }
    if (!tk.set.size) { clearInterval(tk.timer); tk.timer = 0; }
  }
  function tickStart() { if (!tk.timer && doc.visibilityState !== "hidden") tk.timer = setInterval(tickRun, options.tickMs); }
  function track(node, run) { const e = { node, run, miss: 0 }; tk.set.add(e); tickStart(); return () => tk.set.delete(e); }
  doc.addEventListener("visibilitychange", () => { if (doc.visibilityState === "hidden") { clearInterval(tk.timer); tk.timer = 0; } else if (tk.set.size) { tickRun(); tickStart(); } });

  /* ── a small line over a run of numbers (the last hour of a station), its own scale, a dot on the last point ── */
  const SVGNS = "http://www.w3.org/2000/svg";
  function sparkSvg(vals, w, hh) {
    const s = doc.createElementNS(SVGNS, "svg"); s.setAttribute("viewBox", `0 0 ${w} ${hh}`); s.setAttribute("width", w); s.setAttribute("height", hh); s.setAttribute("class", "esSpark"); s.setAttribute("aria-hidden", "true");
    const n = vals.length, max = Math.max(1, ...vals), x = i => 2 + (n < 2 ? 0 : i * (w - 4) / (n - 1)), y = a => hh - 3 - (a / max) * (hh - 7);
    let d = ""; vals.forEach((v, i) => { d += (i ? "L" : "M") + x(i).toFixed(1) + "," + y(v).toFixed(1); });
    const mk = (name, at) => { const e = doc.createElementNS(SVGNS, name); for (const k in at) e.setAttribute(k, at[k]); s.appendChild(e); return e; };
    mk("path", { class: "esSA", d: `${d}L${x(n - 1).toFixed(1)},${hh - 2}L${x(0).toFixed(1)},${hh - 2}Z` }); mk("path", { class: "esSL", d }); mk("circle", { class: "esSD", r: 2.6, cx: x(n - 1).toFixed(1), cy: y(vals[n - 1]).toFixed(1) });
    return s;
  }

  /* ── opening an order: the way every other list does (openOrderFrom: the order window, never a pop-up over a pop-up) ── */
  function openOrder(node, c) {
    const rid = String((c && (c.rid || c.orderNumber)) || "").replace(/\D/g, ""); if (!rid) return false;
    try { if (typeof root.openOrderFrom === "function") { const r = root.openOrderFrom(node, rid); if (r !== false) return true; } } catch (e) { warn("open order", e); }
    try { if (root.OrderWin && typeof root.OrderWin.openOrder === "function") { root.OrderWin.openOrder(rid, { from: node }); return true; } } catch (e) { warn("open order", e); }
    return false;
  }

  /* ══ THE ORDER CARD ══ */
  const CHECK = '<svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 8.5l3.2 3L13 4.8"/></svg>';
  /** Where it happens, in words: the page of the station when it has its own name ("Assembly 2"), else the station; on the board the row says the station already. */
  function whereOf(c, hide) {
    const st = c.stationLabel, dv = c.deviceLabel;
    if (!dv || low(dv) === low(st)) return hide ? "" : st;
    return hide || !st || low(dv).includes(low(st)) ? dv : `${st} · ${dv}`;
  }
  /** One card. `cur` is an entry of the live answer's `current`; o: { stationKey, stationLabel, hideStation, onPerson (false: the name is not a link), onOpen, personTip }. */
  function orderCard(cur, o) {
    o = o || {}; css(); wire();
    let c = normCurrent(cur, { key: o.stationKey, label: o.stationLabel });
    const card = h("article", "esCard"); card.setAttribute("role", "group"); card.dataset.rid = c.rid;
    const th = pictureBox("esTh", 168), main = h("div", "esMain"), qr = pictureBox("esQr", 176), pcs = h("div", "esPieces");
    th.dataset.kind = "order"; qr.dataset.kind = "qr";
    const l1 = h("div", "esL1"), oid = h("button", "esOid"); oid.type = "button"; const cust = h("span", "esCust"); l1.append(oid, cust);
    const who = h("div", "esWho"), av = h("span", "esAv"), pn = h("span", "esPn"), sn = h("span", "esSn"); who.append(av, pn, sn);
    const tm = h("div", "esTime"), tv = h("time", "esT"), tl = h("span", "esTl", "since scanned"), dn = h("span", "esDn"); tv.setAttribute("role", "timer"); dn.hidden = true; tm.append(tv, tl, dn);
    const nt = h("div", "esNote"); main.append(l1, who, tm, nt);
    card.append(th, main, qr, pcs);
    for (const z of [th, qr]) zoomBind(z);
    const goPerson = o.onPerson === false ? null : o.onPerson || (n => { const a = sharedApi(); return a && typeof a.openPerson === "function" ? a.openPerson(n) : undefined; });
    let stopTick = null, pieceSig = "";
    const timer = t => { if (card.dataset.done) return; const s = c.scannedAt ? since(t - c.scannedAt) : "—"; setText(tv, s); };
    function paint(first) {
      const sheet = c.kind === "sheet", name = sheet ? c.title || "Laser sheet" : c.orderNumber || c.rid;
      card.dataset.rid = c.rid; card.dataset.person = c.person; card.dataset.kind = c.kind;
      setText(oid, name); oid.disabled = sheet; oid.setAttribute("aria-label", sheet ? `Laser sheet ${name}` : `Open order ${name}`); oid.title = sheet ? "A laser sheet being cut" : "Open this order";
      setText(cust, c.customer); cust.hidden = !c.customer; cust.title = c.customer;
      const where = whereOf(c, !!o.hideStation);
      av.dataset.t = tone(c.person); setText(av, initials(c.person)); av.hidden = !c.person; setText(pn, c.person); pn.hidden = !c.person; setText(sn, where); sn.hidden = !where;
      who.hidden = !c.person && !where;
      setText(tl, sheet ? "since started" : "since scanned");
      setText(nt, c.note); nt.hidden = !c.note;
      const total = Math.max(c.pieceCount, c.pieces.length), multi = !sheet && total > 1, first1 = c.pieces[0];
      let urls = urlsOf(c, false); if (!urls.length && first1) urls = urlsOf(first1, true);
      th.set({ urls, rid: c.rid, pool: first1 && first1.id, label: sheet ? `Laser sheet ${name}` : `Picture of order ${name}`, icon: sheet ? "sheet" : "", vector: !sheet });
      qr.hidden = sheet; if (!sheet) qr.set({ url: qrUrl(c.qr), vector: false, fit: "contain", label: `QR code of order ${name}` });
      card.classList.toggle("sheet", sheet); card.classList.toggle("lp", !!goPerson);
      const sig = JSON.stringify([total, c.pieces.map(p => [p.id, p.label, p.thumbUrl, p.vectorUrl, p.photoUrl])]);
      if (multi && sig !== pieceSig) {
        pieceSig = sig; pcs.textContent = ""; pcs.hidden = false; pcs.setAttribute("aria-label", `${total} pieces`);
        pcs.appendChild(h("span", "esPcL", `${total} pieces`));
        c.pieces.forEach((p, i) => {
          const fig = h("figure", "esPc"), box = pictureBox("esPcTh", 136); box.dataset.kind = "piece"; box.dataset.n = p.n; box.tabIndex = i === 0 ? 0 : -1;
          const label = p.label || `Piece ${p.n}`; box.title = label; box.set({ urls: urlsOf(p, true), rid: c.rid, pool: p.id, label: `${label}` });
          zoomBind(box);
          box.addEventListener("keydown", ev => { if (ev.key !== "ArrowRight" && ev.key !== "ArrowLeft") return; const all = [...pcs.querySelectorAll(".esPcTh")], to = all[clamp(all.indexOf(box) + (ev.key === "ArrowRight" ? 1 : -1), 0, all.length - 1)]; if (to && to !== box) { ev.preventDefault(); all.forEach(x => { x.tabIndex = -1; }); to.tabIndex = 0; to.focus({ preventScroll: true }); } });
          fig.append(box, h("figcaption", "", String(p.n))); pcs.appendChild(fig);
        });
        if (total > c.pieces.length) {   // the answer lists the first few; the rest are counted, not pictured
          if (c.pieces.length) { const more = h("figure", "esPc"), m = h("span", "esMore", `+${total - c.pieces.length}`); m.title = `${total - c.pieces.length} more pieces are not shown`; more.append(m, h("figcaption", "", "more")); pcs.appendChild(more); }
          else pcs.appendChild(h("span", "esPcNone", "No pictures are stored for these pieces yet"));
        }
      } else if (!multi) { pieceSig = ""; pcs.textContent = ""; pcs.hidden = true; }
      card.dataset.pieces = String(total);
      card.setAttribute("aria-label", sheet ? `Laser sheet ${name}${c.person ? `, ${c.person}` : ""}${where ? ` at ${where}` : ""}` : `Order ${name}${c.customer ? ` for ${c.customer}` : ""}${c.person ? `, ${c.person}` : ""}${where ? ` at ${where}` : ""}${total > 1 ? `, ${total} pieces` : ""}`);
      tv.title = c.scannedAt ? `${sheet ? "Started" : "Scanned"} at ${clock(c.scannedAt)}` : sheet ? "No start time yet" : "No scan time yet"; timer(now());
      if (first) tip();
    }
    const tip = () => {
      av.dataset.esTip = "";
      av._esTip = () => {
        if (o.personTip) return o.personTip(c);
        const a = sharedApi(), sheet = c.kind === "sheet", head = [c.scannedAt ? { k: sheet ? "Started" : "Scanned", v: clock(c.scannedAt), d: sheet ? "When this sheet was started" : "When this order was scanned at the station" } : null, sheet ? { k: "Sheet", v: c.title } : { k: "Order", v: c.orderNumber }].filter(Boolean), at = whereOf(c, false);
        return addFacts({ avatar: c.person, title: c.person || "Unknown", sub: at ? `At ${at}` : "", foot: "Logged activity, not effort." }, head, [], c.person, a ? String(a.view && a.view()) : "real", a && c.person ? body => a.call(body) : null);
      };
    };
    card.addEventListener("pointerdown", ev => { card._pt = ev.pointerType; }, true);
    card.addEventListener("click", ev => {
      const z = ev.target.closest && ev.target.closest("[data-zoom]");
      if (z && card._pt === "touch" && Z.node !== z) { ev.preventDefault(); ev.stopPropagation(); zoomIn(z); return; }   // a tap grows a picture; a tap on the grown one, or anywhere else, opens the order
      if (goPerson && ev.target.closest(".esWho") && c.person) { ev.preventDefault(); goPerson(c.person, c); return; }
      if (c.kind === "sheet") return;   // a laser sheet is not an order: nothing to open
      let taken = false; try { taken = o.onOpen ? o.onOpen(c, card) === true : false; } catch (e) { warn("open", e); }
      if (!taken) openOrder(card, c);
    });
    paint(true);
    stopTick = track(tv, timer);
    card.update = next => { const was = c.id; c = normCurrent(next, { key: c.station, label: c.stationLabel }); paint(false); card.dataset.id = c.id; return was; };
    card.finish = text => {
      card.dataset.done = "1"; if (stopTick) { stopTick(); stopTick = null; }
      dn.innerHTML = CHECK; dn.appendChild(doc.createTextNode(" " + text)); dn.hidden = false; tv.hidden = true; tl.hidden = true; card.classList.add("done");
      card.setAttribute("aria-label", card.getAttribute("aria-label") + `, ${text.toLowerCase()}`); if (Z.node && card.contains(Z.node)) zoomOut(Z.node, true);
    };
    card.data = () => c; card.dataset.id = c.id;
    return card;
  }

  /* ══ THE LIVE FEED: one poll, shared by everyone who watches ══ */
  const endpoint = () => options.endpoint || root.location.origin + "/.netlify/functions/employeeEfficiency";
  const keyOf = sub => { const k = sub.key ? (typeof sub.key === "function" ? sub.key() : sub.key) : ""; if (k) return String(k); try { return root.sessionStorage.getItem(KEY_STORE) || ""; } catch (_) { return ""; } };
  function failure(status, j, net) {
    const srv = j && j.error ? String(j.error).replace(/\s+/g, " ").slice(0, 100) : "";
    const e = new Error(net ? "The service cannot be reached from here." : status === 401 ? "That passcode was not accepted." : status === 403 ? "No manager passcode is set up yet." : status === 404 ? "The efficiency service is not published yet (404)." : status === 429 ? "Too many requests. Paused for a moment." : `The stations could not be read${srv ? ` (${srv})` : status ? ` (${status})` : ""}.`);
    e.status = status || 0; e.auth = status === 401 || status === 403; e.net = !!net; return e;
  }
  async function request(sub, signal, ask) {
    const body = Object.assign({}, sub.params ? sub.params() : {}, ask || { op: "live" });
    if (options.source) return options.source(body, signal);
    const key = keyOf(sub); if (!key) { const e = new Error("Locked: the manager passcode is asked in the console."); e.locked = true; throw e; }
    body.key = key;
    let res; try { res = await fetch(endpoint(), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), cache: "no-store", signal }); } catch (e) { if (e && e.name === "AbortError") throw failure(0, null, true); throw failure(0, null, true); }
    const txt = await res.text(); let j = null; try { j = JSON.parse(txt); } catch (_) {}
    if (!res.ok || !j || j.ok === false) throw failure(res.status, j);
    return j;
  }
  function askOwn(sub, body) {
    const ctl = root.AbortController ? new AbortController() : null, to = ctl ? setTimeout(() => ctl.abort(), options.timeoutMs) : 0;
    return request(sub, ctl && ctl.signal, body).finally(() => clearTimeout(to));
  }
  const Feed = (() => {
    const subs = new Set(); let timer = 0, busy = false, fails = 0, okAt = 0, nCalls = 0, inflight = null;
    const live = () => [...subs].filter(s => { try { return s.active(); } catch (_) { return false; } });
    const next = () => (fails ? Math.min(options.maxBackoffMs, options.pollMs * Math.pow(2, fails)) : options.pollMs);
    function plan(ms) { clearTimeout(timer); timer = 0; if (live().length) timer = setTimeout(poll, ms); }
    async function poll() {
      clearTimeout(timer); timer = 0;
      const act = live(); if (!act.length || busy) return;
      busy = true; nCalls++; for (const s of act) s.busy(true);
      let done = null; inflight = new Promise(r => { done = r; });
      const ctl = root.AbortController ? new AbortController() : null, to = ctl ? setTimeout(() => ctl.abort(), options.timeoutMs) : 0;
      try {
        const sent = Date.now(), j = await request(act[0], ctl && ctl.signal);
        fails = 0; okAt = Date.now(); sync(j && j.at, (sent + okAt) / 2);
        for (const s of subs) s.data(j, okAt);
      } catch (e) {
        if (!e.locked) fails++;
        for (const s of subs) s.fail(e, okAt);
      } finally { clearTimeout(to); busy = false; for (const s of subs) s.busy(false); plan(next()); done(); }
    }
    return {
      /** A new watcher has nothing drawn yet: it reads at once. */
      add(s) { subs.add(s); this.wake(true); },
      remove(s) { subs.delete(s); if (!live().length) { clearTimeout(timer); timer = 0; } },
      /** Something may have become visible (or hidden): poll at once if the data is not fresh, and keep the clock only while someone watches. */
      wake(force) { if (!live().length) { clearTimeout(timer); timer = 0; return; } if (busy) return; if (force || Date.now() - okAt > 900) { clearTimeout(timer); timer = 0; Promise.resolve().then(poll); } else if (!timer) plan(next()); },
      now() { clearTimeout(timer); timer = 0; return busy && inflight ? inflight : poll(); },
      get calls() { return nCalls; }, get busy() { return busy; }, get fails() { return fails; }
    };
  })();
  doc.addEventListener("visibilitychange", () => Feed.wake());
  root.addEventListener("online", () => Feed.wake());
  const shown = el => !!(el && el.isConnected && doc.visibilityState !== "hidden" && el.getClientRects().length > 0);

  /** The console's own door to its data (charm-nest-efficiency.js, Efficiency.api), when this board lives in it. */
  function sharedApi() {
    const E = root.Efficiency, a = E && E.api;
    return a && typeof a.onLive === "function" && typeof a.live === "function" && typeof a.call === "function" && typeof a.state === "function" ? a : null;
  }
  /** On its own page: the Real | Sandbox choice the console keeps for the tab (sessionStorage cn.eff.view), or the caller's. */
  function viewParams(opts) {
    let v = typeof opts.mode === "function" ? opts.mode() : opts.mode;
    if (!v) { try { v = root.sessionStorage.getItem("cn.eff.view"); } catch (_) {} }
    return v === "sandbox" ? { sandbox: true } : {};
  }

  /* ── what a person did today, for their hover card: op person (the employee page's own read: one day, no comparison), asked only when a card
        is about to open, kept a minute (half a minute after a failure); the service's own label, unit and one-line definition of every number are shown ── */
  const PF = new Map();
  const pfGet = (mode, name) => { const e = PF.get(mode + "|" + low(name)); return e && (e.pending || Date.now() - e.at < (e.err ? 30000 : 60000)) ? e : null; };
  const round1 = v => Math.round(v * 10) / 10;
  function pfValue(m) {
    const v = +m.value, u = String(m.unit || "");
    return u === "seconds" ? words(v * 1000) : u === "hours" ? words(v * 3600000) : u === "percent" ? `${round1(v)}%` : /\/hour$/.test(u) ? `${round1(v)} an hour` : /\/day$/.test(u) ? `${round1(v)} a day` : nf(v);
  }
  function pfRows(j) {
    const K = (j && j.kpis) || null, rows = [];
    if (!K) return rows;
    for (const k of ["parts", "orders", "secPerOrderMedian", "activeHours", "idleHours", "partsPerActiveHour"]) {
      const m = K[k]; if (!m || m.value == null || m.value === "" || !Number.isFinite(+m.value)) continue;
      rows.push({ k: str(m.label || k), v: pfValue(m), d: str(m.def) + (m.estimated ? ` (estimated${m.why ? ": " + str(m.why).slice(0, 90) : ""})` : "") });
    }
    return rows;
  }
  function pfLoad(mode, name, ask) {
    const got = pfGet(mode, name); if (got) return got.pending || Promise.resolve(got);
    const ent = { at: Date.now(), pending: null, err: null, rows: null, found: true };
    PF.set(mode + "|" + low(name), ent);
    ent.pending = Promise.resolve().then(() => ask({ op: "person", name, range: "day", compare: false })).then(j => { ent.rows = pfRows(j); ent.found = !(j && j.found === false); ent.known = !!(j && j.kpis); }, e => { ent.err = e || true; }).then(() => { ent.pending = null; ent.at = Date.now(); if (PF.size > 120) PF.delete(PF.keys().next().value); return ent; });
    return ent.pending;
  }
  /** Adds the day's numbers to a person's hover card: the rows when they are in, a labelled wait line while they are on their way, a quiet note when they cannot be had. */
  function addFacts(spec, head, live, name, mode, ask) {
    spec.rows = head.concat(live);
    if (!ask) return spec;
    const e = pfGet(mode, name);
    if (e && !e.pending && e.rows && e.rows.length) spec.rows = head.concat(e.rows, live.filter(r => r.k === "Longest idle"));
    else if (!e || e.pending) { spec.wait = "Reading today's numbers…"; spec.lazy = () => pfLoad(mode, name, ask); }
    else if (e.err) spec.note = "Today's numbers could not be read just now.";
    else if (!e.found) spec.note = "Nothing is logged for this person today yet.";
    else if (!live.length) spec.note = "Today's numbers are not available from the service yet.";
    return spec;
  }

  /* ══ THE BOARD ══ */
  const stateWord = { working: "Working", idle: "Idle", offline: "Offline" };
  function mount(el, opts) {
    opts = opts || {}; css(); wire();
    if (!el) return { unmount() {}, refresh() { return Promise.resolve(); } };
    const R = h("div", "es"); R.dataset.es = "";
    R.innerHTML = `<div class="esHead"><span class="esSum" aria-live="polite"></span><span class="esLive" data-s="load" role="status"><i class="esDot"></i><span class="esSpin" aria-hidden="true"></span><span class="esLiveT">Connecting…</span></span></div>
<p class="esNone" hidden>No one is working on an order right now</p>
<div class="esWait" role="status"><span class="esSpin" aria-hidden="true"></span><span class="esWaitT">Reading the stations…</span><button type="button" class="esRetry" hidden>Try now</button></div>
<div class="esList" hidden></div>`;
    el.appendChild(R);
    const E = { sum: R.querySelector(".esSum"), live: R.querySelector(".esLive"), liveT: R.querySelector(".esLiveT"), none: R.querySelector(".esNone"), wait: R.querySelector(".esWait"), waitT: R.querySelector(".esWaitT"), retry: R.querySelector(".esRetry"), list: R.querySelector(".esList") };
    const S = { rows: new Map(), data: null, okAt: 0, lastApply: 0, busy: false, err: null, dead: false, timers: new Set() };
    const E1 = !opts.own && !options.source ? sharedApi() : null;   // inside the console: its door to the data (one read, its passcode, its Real | Sandbox choice)
    let shared = null;
    const goPerson = opts.onPerson === false ? null : opts.onPerson || (E1 && typeof E1.openPerson === "function" ? n => E1.openPerson(n) : null);
    const later = (fn, ms) => { const t = setTimeout(() => { S.timers.delete(t); fn(); }, ms); S.timers.add(t); return t; };

    /* the status line, honest: live, updating, reconnecting, locked */
    const errNow = () => {   // inside the console the shell knows whether its reads are getting through
      if (S.err) return S.err;
      if (shared) { let c = null; try { c = shared.state(); } catch (_) {} if (c && c.liveSupported === false) return { unsupported: true }; if (c && S.data && c.connected === false) return { net: true }; }
      return null;
    };
    function paintLive() {
      let s, t; const age = S.okAt ? (Date.now() - S.okAt) / 1000 : 0, err = errNow();
      if (err && err.unsupported && !S.data) { s = "off"; t = "Not available yet"; setText(E.waitT, "The stations feed is not available from the service yet."); E.wait.classList.add("quiet"); E.retry.hidden = true; }
      else if (err && err.unsupported) { s = "off"; t = "Not available yet"; }
      else if (err && (err.locked || err.auth)) { s = "off"; t = err.locked ? "Locked" : "Passcode not accepted"; }
      else if (err && S.data) { s = "slow"; t = `Reconnecting · last update ${ago(age)}`; }
      else if (err) { s = "slow"; t = "Reconnecting…"; }
      else if (!S.data) { s = "load"; t = "Connecting…"; }
      else if (S.busy && age > 8) { s = "load"; t = "Updating…"; }
      else { s = "live"; t = `Live · updated ${ago(age)}`; }
      if (E.live.dataset.s !== s) E.live.dataset.s = s; E.live.classList.toggle("esQ", !S.data); setText(E.liveT, t);   // until the first answer the wait below carries the one spinner
    }
    const stopLive = track(E.live, paintLive);

    function stationRow(s) {
      const e = h("section", "esSt"); e.dataset.key = s.key;
      e.innerHTML = `<div class="esStHead"><span class="esStId" tabindex="0" data-es-tip><i class="esLight"></i><h3 class="esStName"></h3><span class="esStState"></span></span><span class="esPeople"></span><span class="esGrow"></span><span class="esCnt"><span class="esCntL">Today</span><span><b data-n="parts">0</b> pieces</span><span><b data-n="orders">0</b> orders</span></span><span class="esSparkW"></span></div><div class="esStBody"></div>`;
      const X = { key: s.key, el: e, id: e.querySelector(".esStId"), light: e.querySelector(".esLight"), name: e.querySelector(".esStName"), state: e.querySelector(".esStState"), people: e.querySelector(".esPeople"), cnt: e.querySelector(".esCnt"),
        parts: e.querySelector('[data-n="parts"]'), orders: e.querySelector('[data-n="orders"]'), sparkW: e.querySelector(".esSparkW"), body: e.querySelector(".esStBody"), chips: new Map(), cards: new Map(), leaving: 0, idle: null, data: s, sparkSig: "" };
      X.id._esTip = () => stationTip(X);
      return X;
    }
    function stationTip(X) {
      const s = X.data, rows = [];
      if (s.counts.parts != null) rows.push({ k: "Pieces today", v: nf(s.counts.parts), d: "Pieces scanned or completed here today" });
      if (s.counts.orders != null) rows.push({ k: "Orders today", v: nf(s.counts.orders), d: "Different orders handled here today" });
      if (s.counts.scans != null) rows.push({ k: "Scans today", v: nf(s.counts.scans), d: "Scans logged at this station today" });
      if (s.lastEventAt) rows.push({ k: "Last event", v: `${clock(s.lastEventAt)} · ${ago((now() - s.lastEventAt) / 1000)}`, d: "The last scan or action logged at this station" });
      if (s.people.length) rows.push({ k: s.people.length === 1 ? "Person" : "People", v: s.people.map(p => p.name).join(", ") });
      if (s.devices.length > 1 || (s.devices.length === 1 && s.devices[0].state !== "offline" && low(s.devices[0].label) !== low(s.label))) {   // the pages of the station and who is on each
        const on = s.devices.filter(d => d.state !== "offline").length;
        s.devices.slice(0, 6).forEach((d, i) => rows.push({ k: d.label, v: d.state === "offline" ? "Offline" : `${d.person ? d.person + " · " : ""}${stateWord[d.state]}`, d: i === 0 ? `Pages of this station: ${on} of ${s.devices.length} in use` : "" }));
        if (s.devices.length > 6) rows.push({ k: "", v: `+${s.devices.length - 6} more pages` });
      }
      return { state: s.state, title: s.label, sub: `${stateWord[s.state]}${s.state === "working" && s.current.length ? ` · ${s.current.length} ${s.current.length === 1 ? "order" : "orders"}` : ""}`, rows, spark: s.spark, sparkNote: "Pieces, last hour", foot: "Logged activity only: it shows what was scanned, not effort." };
    }
    const ask = body => (shared ? shared.call(body) : askOwn(sub, body));
    const modeKey = () => { if (shared) { let v = ""; try { v = shared.view(); } catch (_) {} return String(v || "real"); } return viewParams(opts).sandbox ? "sandbox" : "real"; };
    function personTip(X, p) {
      const s = X.data, cur = s.current.find(c => low(c.person) === low(p.name)), head = [], live = [];
      if (p.since) head.push({ k: "Signed in since", v: `${clock(p.since)} · ${words(now() - p.since)}`, d: "When this person signed in at a station today" });
      if (p.lastSeenAt) head.push({ k: "Last seen", v: ago((now() - p.lastSeenAt) / 1000), d: "The last sign of life from the station" });
      if (p.parts != null) live.push({ k: "Pieces today", v: nf(p.parts), d: "Pieces scanned or completed today" });
      if (p.orders != null) live.push({ k: "Orders today", v: nf(p.orders), d: "Different orders handled today" });
      if (p.medianMs != null) live.push({ k: "Median per order", v: words(p.medianMs), d: "The middle time from scan to done" });
      if (p.longestIdleMs != null) live.push({ k: "Longest idle", v: words(p.longestIdleMs), d: "The longest gap with nothing logged today" });
      const spec = { avatar: p.name, title: p.name, sub: `${s.label}${cur ? ` · ${cur.kind === "sheet" ? "on sheet " + cur.title : "on order " + cur.orderNumber}` : s.state === "working" ? " · between orders" : ""}`, foot: "Logged activity, not effort: a phone scan counts for the desktop's signed-in person." };
      return addFacts(spec, head, live, p.name, modeKey(), p.name ? ask : null);
    }
    function chip(X, p) {
      const c = h("span", "esPer"); c.dataset.name = p.name; c.setAttribute("tabindex", "0"); c.dataset.esTip = "";
      const a = h("span", "esAv", initials(p.name)); a.dataset.t = tone(p.name); c.append(a, h("span", "esPn", p.name));
      c._esTip = () => personTip(X, c._p); c._p = p;
      if (goPerson) { c.addEventListener("click", () => goPerson(c._p.name)); c.dataset.link = ""; }
      return c;
    }
    const cardFor = (X, c) => orderCard(c, { hideStation: true, onPerson: goPerson || false, onOpen: opts.onOpen, personTip: cc => personTip(X, X.data.people.find(p => low(p.name) === low(cc.person)) || { name: cc.person }) });

    /* a thing arrives or leaves */
    const fade = (n, from, to, ms, fill) => { if (still() || !n.animate) return null; return n.animate([from, to], { duration: ms || 220, easing: EASE, fill: fill || "backwards" }); };
    function enterCard(X, card, quiet) {
      if (quiet || still()) { if (!still() && card.animate) card.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 200, easing: "ease-out" }); return; }
      const M = root.Motion, vh = root.innerHeight || 800, inView = r => r.width > 0 && r.height > 0 && r.bottom > 0 && r.top < vh, seen = inView(X.light.getBoundingClientRect()) && inView(card.getBoundingClientRect());
      if (M && M.flyIn && seen) { try { if (M.grow) M.grow(card, { ms: options.flyMs }); M.flyIn(X.light, card, { ms: options.flyMs }); return; } catch (e) { warn("fly in", e); } }
      if (M && M.grow) { try { M.grow(card, { ms: options.enterMs }); return; } catch (_) {} }
      fade(card, { opacity: 0, transform: "translateY(-6px)" }, { opacity: 1, transform: "none" }, options.enterMs);
    }
    function lift(card, gone) {
      const M = root.Motion;
      if (still() || !card.animate) return gone();
      if (M && M.shut) { try { M.shut(card, { ms: 560, done: gone, frames: [{ opacity: 1, transform: "none", clipPath: "inset(0 0 0 0)" }, { opacity: 0, transform: "translateY(-10px)", clipPath: "inset(0 0 100% 0)" }] }); return; } catch (e) { warn("lift", e); } }
      const a = card.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateY(-10px)" }], { duration: 420, easing: "ease-in", fill: "forwards" }); a.finished.then(gone, gone);
    }
    function finishCard(X, card, c, ctx) {
      X.leaving++;
      const on = ctx.names.has(low(c.person)), ms = c.scannedAt ? ctx.at - c.scannedAt : 0;
      const text = c.scannedAt && ms >= 0 && ms < 864e5 ? `${on ? "Done in" : "Left after"} ${words(ms)}` : on ? "Done" : "Left";
      card.finish(text);
      const gone = () => { if (card.isConnected) card.remove(); X.leaving = Math.max(0, X.leaving - 1); idleLine(X); };
      if (ctx.quiet || S.dead) { gone(); return; }
      later(() => { if (!S.dead) lift(card, gone); else gone(); }, options.doneMs);
    }
    function idleLine(X) {
      const s = X.data, none = !X.cards.size && !X.leaving;
      let t = "";
      if (none) {
        const last = s.lastEventAt ? `last event ${clock(s.lastEventAt)} (${ago((now() - s.lastEventAt) / 1000)})` : "";
        t = s.state === "offline" ? `Offline · nobody is signed in${last ? " · " + last : ""}` : s.state === "working" ? `Working · no order open right now${last ? " · " + last : ""}` : `Idle · ${last || "no activity yet today"}`;
      }
      if (!t) { if (X.idle) { X.idle.remove(); X.idle = null; } return; }
      if (!X.idle) { X.idle = h("p", "esIdle"); X.body.appendChild(X.idle); if (!still() && X.idle.animate && S.lastApply) X.idle.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 300, easing: "ease-out" }); }
      X.idle.dataset.at = s.lastEventAt || ""; X.idle.dataset.state = s.state; setText(X.idle, t);
    }
    // the idle lines' "12 m ago" follows the clock
    const stopIdle = track(R, () => { for (const X of S.rows.values()) if (X.idle && X.data.lastEventAt && X.idle.dataset.at) idleLine(X); });

    function update(X, s, ctx) {
      X.data = s; const e = X.el, was = e.dataset.state;
      if (was !== s.state) { e.dataset.state = s.state; if (was && !ctx.quiet && !still() && X.light.animate) X.light.animate([{ transform: "scale(1)" }, { transform: "scale(1.5)", offset: .4 }, { transform: "scale(1)" }], { duration: 520, easing: EASE }); }
      setText(X.name, s.label); setText(X.state, stateWord[s.state] + (s.state === "working" && s.people.length > 1 ? ` · ${s.people.length} people` : ""));
      X.id.setAttribute("aria-label", `${s.label}, ${stateWord[s.state].toLowerCase()}`);
      X.cnt.hidden = s.counts.parts == null && s.counts.orders == null;
      for (const [k, n] of [["parts", X.parts], ["orders", X.orders]]) { const v = s.counts[k]; n.parentNode.hidden = v == null; if (v != null) setNum(n, v, ctx.quiet); }
      const sig = s.spark ? s.spark.join() : ""; if (sig !== X.sparkSig) { X.sparkSig = sig; X.sparkW.textContent = ""; if (s.spark) { X.sparkW.appendChild(sparkSvg(s.spark, 84, 22)); X.sparkW.title = "Pieces in the last hour"; } }
      // people: arrive and leave softly
      const keep = new Set();
      for (const p of s.people) {
        const k = low(p.name); keep.add(k); let c = X.chips.get(k);
        if (!c) { c = chip(X, p); X.chips.set(k, c); X.people.appendChild(c); if (!ctx.quiet) fade(c, { opacity: 0, transform: "scale(.86)" }, { opacity: 1, transform: "none" }, 300); } else c._p = p;
      }
      for (const [k, c] of X.chips) if (!keep.has(k)) { X.chips.delete(k); const a = ctx.quiet ? null : fade(c, { opacity: 1, transform: "none" }, { opacity: 0, transform: "scale(.86)" }, 240, "forwards"); if (a) a.finished.then(() => c.remove(), () => c.remove()); else c.remove(); }
      // one card per person at work
      const seen = new Set();
      for (const c of s.current) {
        seen.add(c.id); let card = X.cards.get(c.id);
        if (card) { card.update(c); continue; }
        card = cardFor(X, c); X.cards.set(c.id, card);
        if (X.idle) { X.idle.remove(); X.idle = null; }
        X.body.appendChild(card); enterCard(X, card, ctx.quiet);
      }
      for (const [id, card] of X.cards) if (!seen.has(id)) { X.cards.delete(id); finishCard(X, card, card.data(), ctx); }
      idleLine(X);
    }
    function setNum(n, to, quiet) {
      to = N(to); if (n._v === to) return; const was = n._v == null ? to : (n._cur != null ? n._cur : n._v);
      if (n._stop) n._stop(); n._v = to; n.dataset.v = to;
      if (quiet || still() || was === to) { n._cur = null; setText(n, nf(to)); return; }
      let t0 = null, dead = false; n._stop = () => { dead = true; };
      const f = ts => { if (dead) return; if (t0 == null) t0 = ts; const k = Math.min(1, (ts - t0) / options.growMs); n._cur = was + (to - was) * (1 - Math.pow(1 - k, 3)); setText(n, nf(n._cur)); if (k < 1) raf(f); else n._cur = null; };
      raf(f);
    }

    function apply(raw, okAt) {
      const M = norm(raw), quiet = !S.data || Date.now() - S.lastApply > options.quietAfterMs, at = M.at || now(), first = !S.data;
      S.data = M; S.okAt = okAt; S.err = null; S.lastApply = Date.now();
      const names = new Set(M.signedIn.map(x => low(x.name))); for (const s of M.stations) for (const p of s.people) names.add(low(p.name));
      const ctx = { quiet, at, names };
      E.wait.hidden = true; E.list.hidden = false;
      const want = new Set(); let i = 0, prev = null;
      for (const s of M.stations) {
        want.add(s.key); let X = S.rows.get(s.key), made = false;
        if (!X) { X = stationRow(s); S.rows.set(s.key, X); made = true; }
        if (made) { E.list.insertBefore(X.el, prev ? prev.nextSibling : E.list.firstChild); } else if (prev ? prev.nextSibling !== X.el : E.list.firstChild !== X.el) E.list.insertBefore(X.el, prev ? prev.nextSibling : E.list.firstChild);
        update(X, s, made && first ? { quiet: true, at, names } : ctx);
        if (made && !still() && X.el.animate) X.el.animate([{ opacity: 0, transform: "translateY(8px)" }, { opacity: 1, transform: "none" }], { duration: options.enterMs, delay: first ? Math.min(i, 8) * 45 : 0, easing: EASE, fill: "backwards" });
        prev = X.el; i++;
      }
      for (const [k, X] of S.rows) if (!want.has(k)) { S.rows.delete(k); const a = quiet ? null : fade(X.el, { opacity: 1 }, { opacity: 0 }, 260, "forwards"); if (a) a.finished.then(() => X.el.remove(), () => X.el.remove()); else X.el.remove(); }
      const working = M.stations.filter(s => s.state === "working").length, on = M.signedIn.length || names.size, orders = M.stations.reduce((n, s) => n + s.current.length, 0);
      setText(E.sum, M.stations.length ? `${working} of ${M.stations.length} ${M.stations.length === 1 ? "station" : "stations"} working · ${on} ${on === 1 ? "person" : "people"} on` : "No stations reported yet");
      E.none.hidden = !(M.stations.length && !orders);
      R.dataset.mode = M.mode; R.dataset.orders = String(orders);
      paintLive(); tipRefresh();
    }
    function fail(e, okAt) {
      S.err = e; S.okAt = okAt || S.okAt;
      if (!S.data) {
        E.wait.hidden = false; E.list.hidden = true; E.waitT.textContent = e.locked ? "The console is locked. Enter the manager passcode to see the stations." : e.auth ? "The passcode was not accepted. Enter it again in the console." : `${e.message} Trying again…`;
        E.wait.classList.toggle("quiet", !!(e.locked || e.auth)); E.retry.hidden = !!(e.locked || e.auth);
      }
      paintLive();
    }
    // Inside the console the board rides the shell's own read of op live (Efficiency.api.onLive: ONE request for the Overview and this board, the
    // shell's passcode, its Real | Sandbox choice and its visibility rules). On its own page it polls op live itself.
    const sub = { active: () => !S.dead && shown(R), params: () => Object.assign(viewParams(opts), typeof opts.params === "function" ? opts.params() : opts.params || {}), key: opts.key, busy: b => { S.busy = b; paintLive(); },
      data: (j, at) => { if (S.dead) return; try { apply(j, at); } catch (e) { warn("draw", e); fail(Object.assign(new Error("The answer could not be drawn."), { status: 0 }), at); } }, fail: (e, at) => { if (!S.dead) fail(e, at); } };
    let io = null, off = null;
    const fromShell = L => { if (S.dead || !L || L.derived) return; const age = (E1.state() || {}).age; try { apply(L.raw || L, Date.now() - (age > 0 ? age : 0)); } catch (e) { warn("draw", e); } };
    if (E1) { shared = E1; off = E1.onLive(fromShell); const L0 = E1.live(); if (L0) fromShell(L0); }
    else { E.retry.addEventListener("click", () => { E.retry.hidden = true; Feed.now(); }); if (root.IntersectionObserver) { io = new IntersectionObserver(() => Feed.wake()); io.observe(R); } Feed.add(sub); }
    return {
      /** The console has shown this board (or wants it fresh now): read at once. */
      refresh() { return E1 ? E1.call({ op: "live" }).then(r => { if (!S.dead) apply(r, Date.now()); }, () => {}) : Feed.now(); },
      destroy() { this.unmount(); },
      unmount() {
        if (S.dead) return; S.dead = true; if (E1) { if (off) off(); } else Feed.remove(sub); if (io) io.disconnect(); stopLive(); stopIdle();
        for (const t of S.timers) clearTimeout(t); S.timers.clear();
        if (Z.node && R.contains(Z.node)) zoomOut(Z.node, true); if (Tp.node && R.contains(Tp.node)) tipHide();
        R.remove();
      },
      get data() { return S.data; }
    };
  }

  /* ── the look: the platform's tokens (paper, ink, gold, sage), thin lines, calm ── */
  function css() {
    if (doc.getElementById("esStyle")) return;
    const s = doc.createElement("style"); s.id = "esStyle";
    s.textContent = `
.es{container-type:inline-size;container-name:esb;display:grid;gap:12px;min-width:0;color:var(--ink,#1c1a17);font-size:12.5px}
.es *,.esCard *,.esTip *{box-sizing:border-box}
.esHead{display:flex;align-items:center;gap:6px 16px;flex-wrap:wrap;min-height:24px;padding:0 2px}
.esSum{color:var(--ink70,#5b554c);font-size:12.5px}
.esLive{margin-left:auto;display:inline-flex;align-items:center;gap:7px;font-size:11.5px;color:var(--ink45,#938c80);white-space:nowrap;min-width:0}
.esDot{position:relative;width:7px;height:7px;border-radius:50%;background:var(--ink25,#c4bdb0);flex:0 0 7px}
.esLive[data-s=live] .esDot{background:var(--sage,#5f7a5b)}
.esLive[data-s=live] .esDot:after{content:"";position:absolute;inset:0;border-radius:50%;background:var(--sage,#5f7a5b);animation:esPulse 2.4s ease-out infinite}
.esLive[data-s=slow] .esDot{background:var(--gold2,#caa861)}
.esLive[data-s=load] .esDot{display:none}.esLive .esSpin{display:none}.esLive[data-s=load] .esSpin{display:inline-block}.esLive.esQ[data-s=load] .esSpin{visibility:hidden}
.esSpin{width:11px;height:11px;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:esSpin .7s linear infinite;flex:0 0 11px;display:inline-block}
@keyframes esSpin{to{transform:rotate(360deg)}}
@keyframes esPulse{0%{transform:scale(1);opacity:.45}70%,100%{transform:scale(3.1);opacity:0}}
.esNone{margin:0;padding:12px 16px;color:var(--ink45,#938c80);background:var(--card,#fffefb);border:1px dashed var(--line,#e4ddd0);border-radius:12px;text-align:center}
.esWait{display:flex;align-items:center;justify-content:center;gap:9px;padding:56px 0;color:var(--ink70,#5b554c);flex-wrap:wrap}
.esWait.quiet .esSpin{display:none}
.esRetry{border:1px solid var(--line,#e4ddd0);background:var(--card,#fffefb);border-radius:999px;padding:4px 12px;font-size:11.5px;font-weight:650;color:var(--ink70,#5b554c)}.esRetry:hover{background:var(--paper2,#ebe5d9);color:var(--ink,#1c1a17)}
.es [hidden],.esCard [hidden],.esTip [hidden]{display:none!important}
.esList{display:grid;gap:12px;min-width:0}
.esSt{background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-radius:12px;min-width:0;transition:border-color .4s}
.esSt[data-state=working]{border-color:#d2dac8}
.esStHead{display:flex;align-items:center;gap:6px 16px;flex-wrap:wrap;padding:11px 16px;min-width:0}
.esStId{display:inline-flex;align-items:center;gap:9px;border-radius:8px;padding:3px 8px;margin:-3px -8px;min-width:0;cursor:default;outline-offset:0}
.esStId:hover,.esStId:focus-visible{background:var(--paper2,#ebe5d9)}
.esLight{position:relative;display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--ink25,#c4bdb0);flex:0 0 10px;box-sizing:border-box}
.esSt[data-state=working] .esLight,.esLight[data-state=working]{background:var(--sage,#5f7a5b)}
.esSt[data-state=working] .esLight:after,.esLight[data-state=working]:after{content:"";position:absolute;inset:0;border-radius:50%;background:var(--sage,#5f7a5b);animation:esPulse 2.6s ease-out infinite}
.esSt[data-state=idle] .esLight,.esLight[data-state=idle]{background:transparent;border:2px solid var(--gold2,#caa861)}
.esSt[data-state=offline] .esLight,.esLight[data-state=offline]{background:var(--ink25,#c4bdb0)}
.esStName{margin:0;font:700 13.5px var(--sans,system-ui,sans-serif);white-space:nowrap}
.esSt[data-state=offline] .esStName{color:var(--ink45,#938c80);font-weight:600}
.esStState{font-size:11.5px;color:var(--ink45,#938c80);white-space:nowrap}
.esPeople{display:flex;flex-wrap:wrap;gap:4px 6px;min-width:0}
.esPer{display:inline-flex;align-items:center;gap:6px;padding:2px 10px 2px 2px;border-radius:999px;border:1px solid var(--line,#e4ddd0);background:var(--card2,#faf7f1);font-size:11.5px;color:var(--ink70,#5b554c);min-width:0;max-width:100%;transition:border-color .2s,background .2s;cursor:default}
.esPer[data-link]{cursor:pointer}
.esPer:hover,.esPer:focus-visible{border-color:var(--ink25,#c4bdb0);background:var(--card,#fffefb);color:var(--ink,#1c1a17)}
.esPer .esPn{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.esAv{display:inline-grid;place-items:center;width:22px;height:22px;border-radius:50%;font:700 9.5px/1 var(--sans,system-ui,sans-serif);letter-spacing:.02em;flex:0 0 22px;background:var(--sageSoft,#e7eddf);color:#46603f;cursor:default}
.esAv[data-t="1"]{background:var(--goldSoft,#f0e6cd);color:#7a5a1d}.esAv[data-t="2"]{background:var(--claySoft,#f4e3dc);color:#8a3a26}.esAv[data-t="3"]{background:var(--slateSoft,#e1ebee);color:#35525e}.esAv[data-t="4"]{background:#ece6f0;color:#5a4a68}.esAv[data-t="5"]{background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c)}
.esAv.big{width:30px;height:30px;flex-basis:30px;font-size:11.5px}
.esGrow{flex:1 1 0}
.esCnt{display:inline-flex;align-items:baseline;gap:4px 14px;font-size:11px;color:var(--ink45,#938c80);font-variant-numeric:tabular-nums;white-space:nowrap}
.esCnt b{color:var(--ink,#1c1a17);font-weight:650;font-size:14px;margin-right:2px}.esCntL{text-transform:uppercase;letter-spacing:.08em;font-size:9.5px;font-weight:700}
.esSt[data-state=offline] .esCnt b{color:var(--ink45,#938c80);font-weight:500}
.esSparkW{display:inline-flex;align-items:center;min-width:0}.esSparkW:empty{display:none}
.esSpark{display:block;overflow:visible}.esSL{fill:none;stroke:#6f6a62;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}.esSA{fill:rgba(93,90,82,.08);stroke:none}.esSD{fill:var(--gold,#a9823f);stroke:var(--card,#fffefb);stroke-width:1.5}
.esStBody{padding:0 16px 14px;display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(min(100%,380px),1fr));align-items:start;min-width:0}
.esStBody:empty{display:none}
.esIdle{grid-column:1/-1;margin:-2px 0 0;font-size:12px;color:var(--ink45,#938c80)}.esIdle:before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--gold2,#caa861);margin-right:8px;vertical-align:1px;opacity:.8}
.esIdle[data-state=offline]:before{background:var(--ink25,#c4bdb0)}.esIdle[data-state=working]:before{background:var(--sage,#5f7a5b)}
/* the order card */
.esCard{position:relative;display:grid;grid-template-columns:72px minmax(0,1fr) 72px;gap:0 12px;align-items:center;background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);border-radius:12px;padding:10px;min-width:0;cursor:pointer;transition:border-color .2s,transform .25s}
.esCard:hover{border-color:var(--goldLine,#e3d3a6);transform:translateY(-1px)}
.esCard.sheet{grid-template-columns:72px minmax(0,1fr);cursor:default}.esCard.sheet:hover{transform:none}
.esCard.lp .esWho{cursor:pointer}.esCard.lp .esWho:hover .esPn{text-decoration:underline;text-decoration-color:var(--gold2,#caa861);text-underline-offset:3px}
.esOid:disabled{cursor:default;color:var(--ink,#1c1a17);opacity:1}.esOid:disabled:hover{background:transparent;text-decoration:none}
.esCard.done{cursor:default}.esCard.done:hover{transform:none;border-color:var(--line,#e4ddd0)}
.esCard.done .esTh,.esCard.done .esQr,.esCard.done .esPieces,.esCard.done .esL1,.esCard.done .esWho{opacity:.55}
.esTh,.esQr,.esPcTh{position:relative;display:grid;place-items:center;border-radius:9px;background:#fff;border:1px solid var(--line2,#efe9dd);isolation:isolate;outline-offset:2px;cursor:pointer;flex:none}
.esTh,.esQr{width:72px;height:72px}
.esTh:after,.esQr:after,.esPcTh:after{content:"";position:absolute;inset:-1px;border-radius:inherit;box-shadow:0 12px 30px rgba(30,26,20,.3),0 2px 6px rgba(30,26,20,.14);opacity:0;transition:opacity .25s;pointer-events:none;z-index:-1}
.esLit:after{opacity:1}.esUp{z-index:60}
.esTh:hover,.esQr:hover,.esPcTh:hover{border-color:var(--goldLine,#e3d3a6)}
.esImg{display:block;width:100%;height:100%;border-radius:inherit;opacity:0;transition:opacity .3s}.esImg.on{opacity:1}.esImg.cover{object-fit:cover}.esImg.contain{object-fit:contain}
.esQr .esImg{image-rendering:auto}
.esTh[data-state=wait],.esQr[data-state=wait],.esPcTh[data-state=wait]{background:var(--paper2,#ebe5d9)}
.esPh{display:grid;place-items:center;width:100%;height:100%;border-radius:inherit;color:var(--ink25,#c4bdb0);background:var(--paper2,#ebe5d9)}
.esMain{display:grid;gap:5px;min-width:0;align-content:center}
.esL1{display:flex;align-items:baseline;gap:4px 10px;min-width:0;flex-wrap:wrap}
.esOid{border:0;background:transparent;padding:1px 6px;margin:-1px -6px;border-radius:6px;font:650 12.5px var(--mono,ui-monospace,monospace);color:var(--ink,#1c1a17);white-space:nowrap}
.esOid:hover{background:var(--goldSoft,#f0e6cd);text-decoration:underline;text-decoration-color:var(--gold2,#caa861);text-underline-offset:3px}
.esCust{color:var(--ink70,#5b554c);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;max-width:100%}
.esWho{display:flex;align-items:center;gap:6px;min-width:0;font-size:11.5px;color:var(--ink70,#5b554c)}
.esWho .esAv{width:20px;height:20px;flex-basis:20px;font-size:9px}.esWho .esPn{font-weight:650;color:var(--ink,#1c1a17);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.esSn{color:var(--ink45,#938c80);white-space:nowrap}.esSn:before{content:"·";margin-right:6px}
.esTime{display:flex;align-items:baseline;gap:7px;min-height:20px}
.esT{font:650 16px var(--mono,ui-monospace,monospace);font-variant-numeric:tabular-nums;letter-spacing:-.01em;color:var(--ink,#1c1a17)}
.esTl{font-size:11px;color:var(--ink45,#938c80)}
.esDn{display:inline-flex;align-items:center;gap:5px;color:var(--sage,#5f7a5b);font-weight:650;font-size:12.5px}
.esNote{font-size:11.5px;color:var(--ink45,#938c80);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.esPieces{grid-column:1/-1;display:flex;flex-wrap:wrap;align-items:flex-start;gap:8px 9px;margin-top:10px;padding-top:10px;border-top:1px solid var(--line2,#efe9dd);min-width:0}
.esPcL{align-self:center;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--ink45,#938c80);font-weight:700;margin-right:4px;white-space:nowrap}
.esPc{margin:0;display:grid;justify-items:center;gap:3px}.esPc figcaption{font-size:10px;color:var(--ink45,#938c80);font-variant-numeric:tabular-nums}
.esPcTh{width:44px;height:44px;border-radius:8px}
.esMore{display:grid;place-items:center;width:44px;height:44px;border-radius:8px;border:1px dashed var(--line,#e4ddd0);color:var(--ink45,#938c80);font:650 12px var(--sans,system-ui,sans-serif);font-variant-numeric:tabular-nums}
.esPcNone{align-self:center;font-size:11.5px;color:var(--ink45,#938c80)}
/* the hover card */
.esTip{position:fixed;z-index:2147483200;left:0;top:0;width:max-content;max-width:min(300px,calc(100vw - 16px));padding:12px 14px 11px;border-radius:12px;background:var(--card,#fffefb);color:var(--ink70,#5b554c);border:1px solid var(--line,#e4ddd0);box-shadow:0 12px 34px rgba(30,26,20,.18),0 2px 6px rgba(30,26,20,.08);visibility:hidden;opacity:0;pointer-events:none;font-size:12px;line-height:1.35;display:grid;gap:9px}
.esTip[data-on]{visibility:visible;opacity:1}
.esTip:after{content:"";position:absolute;left:var(--ax,50%);bottom:-4px;width:8px;height:8px;margin-left:-4px;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-top:0;border-left:0;border-radius:0 0 2px 0;transform:rotate(45deg)}
.esTip.below:after{bottom:auto;top:-4px;transform:rotate(225deg)}
.esTipH{display:flex;align-items:center;gap:10px}.esTipH .esLight{margin:0 2px}
.esTipT{display:grid;min-width:0}.esTipT b{color:var(--ink,#1c1a17);font-size:13.5px}.esTipT span{font-size:11.5px;color:var(--ink45,#938c80)}
.esTipR{margin:0;display:grid;gap:7px}.esTipR>div{display:grid;grid-template-columns:1fr auto;column-gap:16px;align-items:baseline}
.esTipR dt{font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45,#938c80);font-weight:700}.esTipR dd{margin:0;color:var(--ink,#1c1a17);font-weight:650;font-variant-numeric:tabular-nums;text-align:right}
.esTipR small{grid-column:1/-1;color:var(--ink45,#938c80);font-size:10.5px;margin-top:1px}
.esTipW{margin:0;display:flex;align-items:center;gap:8px;font-size:11.5px;color:var(--ink45,#938c80)}
.esTipN{margin:0;font-size:11.5px;color:var(--ink45,#938c80)}
.esTipS{display:grid;gap:2px}.esTipS small{color:var(--ink45,#938c80);font-size:10.5px}
.esTipF{margin:0;padding-top:8px;border-top:1px solid var(--line2,#efe9dd);font-size:10.5px;color:var(--ink45,#938c80)}
@container (max-width:560px){
 .esStHead{padding:10px 14px}.esStBody{padding:0 12px 12px}.esCntL{display:none}.esCnt{gap:4px 12px}.esSparkW{display:none}.esGrow{display:none}.esPeople{flex:1 1 100%;order:3}.esCnt{margin-left:auto}
 .esLive .esLiveT{max-width:100%}
}
@container (max-width:420px){
 .esCard{grid-template-columns:56px minmax(0,1fr) 56px;gap:0 10px;padding:9px}.esCard.sheet{grid-template-columns:56px minmax(0,1fr)}.esTh,.esQr{width:56px;height:56px}.esT{font-size:15px}
}
@media (prefers-reduced-motion:reduce){.es *,.esCard *,.esTip *{transition:none!important;animation:none!important}.esCard{transition:none}.esLive[data-s=live] .esDot:after,.esSt[data-state=working] .esLight:after{animation:none}}`;
    (doc.head || doc.documentElement).appendChild(s);
  }

  /** The board's light hover card on ANY element (the Overview's numbers, the people list): spec() is asked each time the card opens or refreshes and
   *  returns { title, sub, avatar | state, rows:[{ k, v, d }], note, foot } (or null for no card). The element gets the platform's pointer, focus and touch
   *  behaviour of the board's own cards; one card is shared by all of them. */
  function hoverCard(node, spec) { if (!node || typeof spec !== "function") return node; css(); wire(); node.dataset.esTip = ""; node._esTip = spec; return node; }

  root.EfficiencyStations = { mount, orderCard, norm, options, qr: qrUrl, fmt: { since, words, ago, initials }, openOrder, hoverCard,
    /* for the checks */
    feed: { get calls() { return Feed.calls; }, get busy() { return Feed.busy; }, get fails() { return Feed.fails; }, wake: () => Feed.wake(), now: () => Feed.now() }, zoomed: () => Z.node, tip: () => Tp.el };
})(typeof self !== "undefined" ? self : this);
