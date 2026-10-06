/*  charm-nest-efficiency-orders.js — the real-time order search list of one employee (Paul, 5 Oct 2026: "real-time search of all the orders
 *  that employee has done ... a real time order list ... a barcode thumbnail for the order image/vector image file, and one thumbnail for
 *  each piece in a multi piece order ... full hover states and zooms into relevant places").
 *  It lists every order a person handled, newest first, and keeps itself current: a search box that filters as you type, paging as you
 *  scroll, new orders arriving at the top while the list is on screen. Plans: /mnt/project-files/plans/employee-hr/plan.md (worker E8).
 *
 *  API
 *    const h = EfficiencyOrders.mount(el, opts)      → { unmount(), refresh(), setQuery(q), setRange(range), setStation(key), setSort(key), setName(name), state() }
 *      el                the box to fill (any block element; it keeps its own height, the list grows with the page, no inner scroll)
 *      opts.name         the person (required; any spelling of the name, the server merges aliases)
 *      opts.onOpen       (rid, button, order) → opens the order. Default: Efficiency.api.openOrder(button, rid), else the sorter's openOrderFrom(button, rid), else OrderWin.openOrder(rid)
 *      opts.query        the search shown first ('' by default)
 *      opts.range        { from, to } New York days 'YYYY-MM-DD' (either may be '') or null: the days the list covers. Sent as `from` / `to`.
 *      opts.station      a station key ('welding' ...) or '' for all
 *      opts.sort         'newest' (default) | 'slowest' | 'fastest'   (see "Sort" below)
 *      opts.filters      false hides the station chips, the date fields and the sort switch (the host drives them with setStation / setRange / setSort)
 *      opts.dateFields   false hides only the From / To fields (the host owns the date chips); an active range then shows as a quiet note
 *      opts.sandbox      true | false | () => boolean — only for a list mounted WITHOUT the console shell: which copy the server reads (default Real).
 *                        Inside the shell, Real | Sandbox is the shell's own switch (Efficiency.api.view()), which adds it to every request.
 *      opts.call         (body, signal) → Promise<answer>: how a request is made. Default: Efficiency.api.call(body, { signal }) (the shell adds the
 *                        manager passcode and the Real | Sandbox choice), else a built-in POST to /.netlify/functions/employeeEfficiency with the passcode
 *                        the console holds for this tab (sessionStorage 'cn.eff.key', sent as `key`, never shown, logged or stored by this file). An
 *                        error it throws is shown as its message; one with .auth or .locked stops the live polling.
 *      opts.onRange      ({from,to} | null) → called when the list's own date fields or its "Clear" change the range (so the host's chips can follow)
 *      opts.limit        orders per page (25)    opts.pollMs   milliseconds between live checks (5000; 0 = no live checks)
 *      opts.prefer       'photo' (default): a piece shows its stored picture, else its vector design; 'vector': the design first
 *      opts.onState      (info) → called when the list changes: { total, scanned, rows, loading, error, query }
 *    h.setQuery(q)       puts q in the search box and searches at once (E5: the person's page can seed or clear the search)
 *    h.setRange(r)       { from, to } | null  ·  h.setStation(key)  ·  h.setSort(key)  ·  h.setName(name)  ·  h.refresh() reloads the first page
 *    h.state()           a plain snapshot for scripts and tests: { name, query, station, from, to, sort, rids:[...], total, scanned, next, loading, more,
 *                        error, polling, away, buffered, live }
 *    h.unmount()         stops every request, timer and listener and empties el. Mounting again into the same el unmounts the old list first.
 *
 *  What it asks (op personOrders, employeeEfficiency; E4's, newest first by the time of the last action): { op, name, q, cursor, limit, from?, to?, station?,
 *  sort?, sandbox? } and reads
 *    answer.{ ok, now, mode, found, total, scanned, searched:{orders,withDetails}, orders, next, notes[], partial, sort? }
 *    order.{ rid, number, at, day, station, stations[], durationMs, spanMs, scans, completes, prints, parts, undone, rejected, errors,
 *            steps[{station,firstAt,lastAt,durationMs,scans,completes,prints,parts}], issues[{kind,label,at,note}], customer, info, thumbUrl,
 *            qr:{text}, pieces[{id,label,sku,thumbUrl}], piecesCount }
 *  Anything missing is left out of the row (never invented): no pieces → one calm picture tile; no thumbUrl → a placeholder; no qr → the receipt id.
 *
 *  What a row shows: order number, customer, the date and time (New York), the station chip, the logged working time ("4 m 12 s"), a status hint
 *    (Completed · Reopened · Issue · Handled), ONE picture tile per piece (at most three, then "+N": hover it for all of them, press it to open
 *    them in place; tiles stop at 24 and the card says "and N more", the server lists at most 12 pieces and gives the true piecesCount), and a
 *    small QR of the order (qr.text). Thumbnails and the QR grow in place on a resting pointer (the platform's own zoom,
 *    data-zoom-dot of charm-nest-motion.js: never full screen); the rest of the row shows a hover card with the timing facts. Words you type
 *    are marked in the text. A press anywhere on the row opens the order (onOpen); a pop-up is never opened over a pop-up by this file.
 *  Live: while the list is in sight (tab shown, the box laid out and on screen) the first page is read every pollMs. An order that is new
 *    slides in at the top; an order that changed (its working time still counting) is updated where it stands. When you have scrolled
 *    away from the top, new orders wait behind an "N new" pill instead of moving the list under you. Hidden or out of sight: no requests.
 *  States: loading (labelled spinner), "This person has not handled an order yet", "No orders match", an error with Retry (rows already on
 *    screen are kept when only a live check fails: the Live light says "Reconnecting…"). Stale answers are dropped and their requests aborted.
 *  Sort: the server lists newest first and ignores `sort` today. The list keeps the server's order for Newest (never re-sorting by the first action;
 *    each order remembers its place in that order, so going back to Newest restores it);
 *    'slowest' and 'fastest' reorder the orders loaded so far (more load as you scroll) and the list says so, unless the answer carries
 *    sort:'<the same key>' (the server sorted the whole range). The server's own `notes` (how far the customer / SKU search reached, no receipt
 *    stored...) are shown quietly under the filters.
 *  Motion: opacity and transform only; prefers-reduced-motion removes it. Layout: wide (about 980 px and up) one line per order, medium two lines,
 *    narrow (a phone) stacked; never a sideways scroll.
 *  A box that was taken out of the page without unmount() is let go after options.orphanMs (20 s) of being detached.
 *    EfficiencyOrders.options = { pollMs, debounceMs, pageSize, capThumbs, hoverMs, zoom, orphanMs }   EfficiencyOrders.norm(answer)   EfficiencyOrders.fmtDur(ms)  */
(function (root) {
  "use strict";
  if (root.EfficiencyOrders) return;
  const doc = root.document, TZ = "America/New_York";
  const options = { pollMs: 5000, debounceMs: 200, pageSize: 25, capThumbs: 3, hoverMs: 260, maxBackoffMs: 60000, freshMs: 2400, orphanMs: 20000, zoom: 1.9, vectorConc: 2, vectorCap: 240, qrCap: 600, awayPx: 48 };
  const KEY_STORE = "cn.eff.key";
  const NAMES = { shipping: "Shipping", assembly: "Assembly", welding: "Welding", sorting: "Sorting", design: "Design", laser: "Laser", inbox: "Inbox" };
  const CORE = ["sorting", "welding", "assembly", "shipping", "design", "laser"];   // (Laser and Design are two stations of their own)
  /* ONE Sorting station: a stored "sorter" or "qr" key is shown as Sorting (EfficiencyStations.displayStation, the same rule as the server's) */
  const dispSt = k => { const f = root.EfficiencyStations && root.EfficiencyStations.displayStation; return typeof f === "function" ? f(k) : (k === "sorter" || k === "qr" ? "sorting" : k); };
  const SORTS = [["newest", "Newest"], ["slowest", "Slowest"], ["fastest", "Fastest"]];
  const ACTIVE_MS = 120000, TILES = 24;   // (the server lists at most 12 pieces of an order and says the true count; tiles are drawn for up to 24)
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const S_ = v => (v == null ? "" : String(v));
  const N = v => { v = +v; return Number.isFinite(v) ? v : 0; };
  const T = v => { v = +v; return Number.isFinite(v) && v > 0 ? v : null; };
  const arr = v => (Array.isArray(v) ? v : []);
  const nf = n => Math.round(N(n)).toLocaleString("en-US");
  const still = () => !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches);
  const stName = s => NAMES[dispSt(s)] || (s ? s.charAt(0).toUpperCase() + s.slice(1) : "Station");
  const aborted = e => !!(e && e.name === "AbortError");
  const el = (tag, cls, html) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const setText = (e, s) => { if (e && e.textContent !== s) e.textContent = s; };
  const setHtml = (e, h) => { if (e && e._h !== h) { e._h = h; e.innerHTML = h; return true; } return false; };
  const safeUrl = u => { u = S_(u).trim(); return /^(https?:\/\/|data:image\/|blob:|\/(?!\/))/i.test(u) ? u : ""; };

  /* ── New York time ── */
  const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hour: "numeric", minute: "2-digit" });
  const dateFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, weekday: "short", month: "short", day: "numeric" });
  const yearFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, year: "numeric" });
  const fullFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, weekday: "short", month: "short", day: "numeric", year: "numeric" });
  const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
  /** { d: "Mon, Oct 5" (with the year when it is not this one), t: "3:12 PM" } of a time in New York. */
  function when(at, now) {
    const a = new Date(at), same = yearFmt.format(a) === yearFmt.format(new Date(now || Date.now()));
    return { d: (same ? dateFmt : fullFmt).format(a), t: clockFmt.format(a) };
  }
  /** "4 m 12 s": the logged working time. Under a second the number is not known well enough to print. */
  function fmtDur(ms) {
    ms = N(ms); if (ms < 1000) return "—";
    const s = Math.round(ms / 1000); if (s < 60) return `${s} s`;
    const m = Math.floor(s / 60), r = s % 60; if (m < 60) return r ? `${m} m ${r} s` : `${m} m`;
    const h = Math.floor(m / 60), mm = m % 60; if (h < 48) return mm ? `${h} h ${mm} m` : `${h} h`;
    const d = Math.floor(h / 24); return `${d} d${h % 24 ? ` ${h % 24} h` : ""}`;
  }

  /* ── the answer, normalised: a missing field is empty, never a crash ── */
  const ISSUE_KINDS = new Set(["refused", "rejected", "cancelAlert", "heldOrSkipped", "failed", "lookupFailed"]);
  /** completed · reopened · issue · handled, with the plain reason. Priority: issue, reopened, completed. A reprint or a rescan is a signal, never a verdict. */
  function statusOf(o) {
    const kinds = new Set(o.issues.map(i => i.kind));
    const bad = o.rejected > 0 || o.errors > 0 || [...kinds].some(k => ISSUE_KINDS.has(k));
    if (bad) {
      const why = o.issues.filter(i => ISSUE_KINDS.has(i.kind)).map(i => i.label || i.kind);
      return { s: "issue", label: "Issue", why: why.length ? why.join(", ") : o.rejected ? "A piece was rejected" : "An error was logged on this order" };
    }
    if (o.undone > 0 || kinds.has("undone")) return { s: "reopened", label: "Reopened", why: "A completion of this order was undone, so it was opened again" };
    if (o.completes > 0) return { s: "completed", label: "Completed", why: "Completed here at least once" };
    return { s: "handled", label: "Handled", why: "Scanned or printed here; no completion by this person" };
  }
  function normOrder(o) {
    if (!o || typeof o !== "object") return null;
    const rid = S_(o.rid != null ? o.rid : o.orderId).trim(); if (!rid) return null;
    const steps = arr(o.steps).filter(s => s && s.station).map(s => ({ station: S_(s.station), firstAt: T(s.firstAt), lastAt: T(s.lastAt), durationMs: s.durationMs == null ? null : N(s.durationMs), scans: N(s.scans), completes: N(s.completes), prints: N(s.prints), parts: N(s.parts) }));
    const pieces = arr(o.pieces).filter(p => p && typeof p === "object").map((p, i) => ({ id: S_(p.id != null ? p.id : i + 1), label: S_(p.label), sku: S_(p.sku), thumbUrl: safeUrl(p.thumbUrl) }));
    const issues = arr(o.issues).filter(i => i && (i.kind || i.label)).map(i => ({ kind: S_(i.kind), label: S_(i.label), at: T(i.at), note: S_(i.note) }));
    const stations = arr(o.stations).map(S_).filter(Boolean), station = S_(o.station) || stations[0] || "";
    if (station && !stations.includes(station)) stations.unshift(station);
    const at = T(o.at) || steps.reduce((m, s) => (s.firstAt && (!m || s.firstAt < m) ? s.firstAt : m), 0) || 0;
    const lastAt = steps.reduce((m, s) => Math.max(m, s.lastAt || 0), 0) || at;
    const q = o.qr && typeof o.qr === "object" ? S_(o.qr.text) : S_(o.qr);
    const out = { rid, number: S_(o.number).replace(/^#/, "") || rid, at, lastAt, day: S_(o.day), station, stations, durationMs: o.durationMs == null || !Number.isFinite(+o.durationMs) ? null : Math.max(0, +o.durationMs), spanMs: o.spanMs == null || !Number.isFinite(+o.spanMs) ? null : Math.max(0, +o.spanMs),
      scans: N(o.scans), completes: N(o.completes), prints: N(o.prints), parts: N(o.parts), undone: N(o.undone), rejected: N(o.rejected), errors: N(o.errors), steps, issues, customer: S_(o.customer).trim(), info: o.info !== false,
      thumbUrl: safeUrl(o.thumbUrl), qr: q || rid, pieces, piecesCount: Math.max(pieces.length, N(o.piecesCount)) };
    out.status = statusOf(out);
    return out;
  }
  function norm(a) {
    a = a || {};
    const orders = arr(a.orders).map(normOrder).filter(Boolean), se = a.searched && typeof a.searched === "object" ? { orders: N(a.searched.orders), withDetails: N(a.searched.withDetails) } : null;
    return { ok: a.ok !== false, now: T(a.now) || 0, mode: S_(a.mode), found: a.found !== false, total: a.total == null ? orders.length : N(a.total), scanned: N(a.scanned), searched: se, orders, next: a.next ? S_(a.next) : "", notes: arr(a.notes).map(S_).filter(Boolean), partial: !!a.partial, sort: S_(a.sort) };
  }
  /** What makes a row look different: any change redraws that row's regions (and no other). */
  const sigOf = o => JSON.stringify([o.number, o.customer, o.at, o.lastAt, o.station, o.stations, o.durationMs, o.spanMs, o.scans, o.completes, o.prints, o.parts, o.undone, o.rejected, o.errors, o.issues.map(i => i.kind + i.at), o.thumbUrl, o.qr, o.piecesCount, o.pieces.map(p => [p.id, p.label, p.sku, p.thumbUrl]), o.steps.map(s => [s.station, s.lastAt, s.durationMs])]);

  /* ── words typed, marked in the text ── */
  const words = q => S_(q).toLowerCase().split(/\s+/).filter(Boolean);
  const reEsc = w => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  function hl(text, ws) {
    text = S_(text); ws = ws.filter(w => w.length >= 2); if (!ws.length || !text) return esc(text);
    const re = new RegExp("(" + ws.map(reEsc).sort((a, b) => b.length - a.length).join("|") + ")", "gi");
    let out = "", last = 0, m; re.lastIndex = 0;
    while ((m = re.exec(text))) { if (!m[0]) { re.lastIndex++; continue; } out += esc(text.slice(last, m.index)) + `<mark class="efoMk">${esc(m[0])}</mark>`; last = m.index + m[0].length; }
    return out + esc(text.slice(last));
  }
  const WEEKDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];
  const mark = (t, on) => (on ? `<mark class="efoMk">${esc(t)}</mark>` : esc(t));
  /** The date text ("Mon, Oct 5"), with what the typed words say about the date marked: a weekday, a month, a day beside a month,
   *  a whole date (2026-10-03, 10/3) or a whole month (2026-10): the forms the server reads. */
  function hlDate(o, d, ws) {
    if (!ws.length) return esc(d);
    const m = /^(\w{3}), (\w{3}) (\d{1,2})(, \d{4})?$/.exec(d), p = /^(\d{4})-(\d{2})-(\d{2})$/.exec(o.day || "");
    if (!m || !p) return hl(d, ws);
    const y = +p[1], mo = +p[2], da = +p[3];
    const whole = w => { if (w === o.day) return true; if (/^\d{4}-\d{2}$/.test(w)) return o.day.startsWith(w + "-"); const x = /^(\d{1,2})\/(\d{1,2})(?:\/\d{2,4})?$/.exec(w); return !!x && +x[1] === mo && +x[2] === da; };
    if (ws.some(whole)) return mark(d, true);
    const wd = WEEKDAYS[new Date(Date.UTC(y, mo - 1, da)).getUTCDay()], mon = MONTHS[mo - 1];
    const hasWd = ws.some(w => w.length >= 3 && /^[a-z]+$/.test(w) && wd.startsWith(w)), hasMon = ws.some(w => w.length >= 3 && /^[a-z]+$/.test(w) && mon.startsWith(w));
    const hasDay = hasMon && ws.some(w => /^\d{1,2}$/.test(w) && +w === da);
    return `${mark(m[1], hasWd)}, ${mark(m[2], hasMon)} ${mark(m[3], hasDay)}${esc(m[4] || "")}`;
  }

  /* ── the QR of an order: the app's own generator (lib/qrcode.min.js, as the sheet labels use it), drawn as a sharp vector ── */
  const QRC = new Map(), mounts = new Set();
  let qrLoading = false;
  function qrLib() {
    if (root.QRCode) return root.QRCode;
    if (!qrLoading && doc.head) {
      qrLoading = true; const s = doc.createElement("script"); s.src = "lib/qrcode.min.js";
      s.onload = () => { QRC.clear(); for (const m of mounts) m.repaintQr(); }; s.onerror = () => {}; doc.head.appendChild(s);
    }
    return null;
  }
  function qrUrl(text) {
    text = S_(text); if (!text) return "";
    if (QRC.has(text)) { const v = QRC.get(text); QRC.delete(text); QRC.set(text, v); return v; }
    const Q = qrLib(); if (!Q || !doc.body) return "";
    const holder = doc.createElement("div"); holder.style.cssText = "position:absolute;left:-9999px;top:0;width:0;height:0;overflow:hidden"; doc.body.appendChild(holder);
    let url = "";
    try {
      let qr; try { qr = new Q(holder, { text, width: 96, height: 96, correctLevel: Q.CorrectLevel.M }); } catch (e) { holder.innerHTML = ""; qr = new Q(holder, { text, width: 96, height: 96, correctLevel: Q.CorrectLevel.L }); }
      const m = qr && qr._oQRCode;
      if (m && typeof m.getModuleCount === "function" && typeof m.isDark === "function") {
        const n = m.getModuleCount(), q = 2; let d = "";
        for (let r = 0; r < n; r++) for (let c = 0; c < n; c++) if (m.isDark(r, c)) d += `M${c + q} ${r + q}h1v1h-1z`;
        const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${n + 2 * q} ${n + 2 * q}" shape-rendering="crispEdges"><rect width="100%" height="100%" fill="#fff"/><path d="${d}" fill="#111"/></svg>`;
        url = "data:image/svg+xml," + encodeURIComponent(svg);
      } else { const cv = holder.querySelector("canvas"), im = holder.querySelector("img"); url = cv && cv.width ? cv.toDataURL("image/png") : im ? im.src : ""; }
    } catch (e) { url = ""; } finally { holder.remove(); }
    if (url) { QRC.set(text, url); while (QRC.size > options.qrCap) QRC.delete(QRC.keys().next().value); }
    return url;
  }

  /* ── the vector design of a piece, from the sorter's own renderer (window.PieceMedia): a small queue, a bounded cache, no Etsy ── */
  const VC = new Map(), vq = []; let vrun = 0;
  function vpump() {
    while (vrun < options.vectorConc && vq.length) {
      const j = vq.shift(); vrun++;
      Promise.resolve().then(() => root.PieceMedia.vectorThumb(j.row)).then(u => j.done(typeof u === "string" && u ? u : null), () => j.done(null)).then(() => { vrun--; vpump(); });
    }
  }
  function vecThumb(sku) {
    sku = S_(sku).trim(); const PM = root.PieceMedia;
    if (!sku || !PM || typeof PM.vectorThumb !== "function") return Promise.resolve(null);
    const row = { spec: { designSku: sku, size: null, noDesign: false }, line: { sku }, poolIds: [] };
    let key = "sku:" + sku.toUpperCase(); try { if (typeof PM.vectorKey === "function") key = PM.vectorKey(row) || key; } catch (_) {}
    const had = VC.get(key); if (had && (had.url || Date.now() - had.at < 60000)) { VC.delete(key); VC.set(key, had); return had.p; }
    const e = { at: Date.now(), url: "", p: null };
    e.p = new Promise(res => vq.push({ row, done: u => { e.url = u || ""; e.at = Date.now(); res(u); } }));
    VC.set(key, e); while (VC.size > options.vectorCap) VC.delete(VC.keys().next().value);
    vpump(); return e.p;
  }

  const PH = '<svg viewBox="0 0 24 24" width="55%" height="55%" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round" aria-hidden="true"><path d="M12 3.8 18.4 8v8L12 20.2 5.6 16V8z"/><path d="M12 3.8v8.4m0 0L5.6 8m6.4 4.2L18.4 8" opacity=".55"/></svg>';
  const QRPH = '<svg viewBox="0 0 24 24" width="60%" height="60%" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true"><rect x="4" y="4" width="6" height="6" rx="1"/><rect x="14" y="4" width="6" height="6" rx="1"/><rect x="4" y="14" width="6" height="6" rx="1"/><path d="M14 14h2.5v2.5M20 14v6h-3.5M14 20v-1"/></svg>';
  const MAG = '<svg class="efoMag" viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6"/><path d="m15 15 5 5"/></svg>';

  let styled = false;
  function style() {
    if (styled || doc.getElementById("efoStyle")) { styled = true; return; }
    styled = true;
    const s = doc.createElement("style"); s.id = "efoStyle";
    s.textContent = `
.efo{container-type:inline-size;container-name:efo;display:grid;gap:10px;min-width:0;color:var(--ink,#1c1a17);font:12.5px/1.4 var(--sans,-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif);--efoT:48px;--efoTW:204px}
.efo *,.efoTip *{box-sizing:border-box}
.efo [hidden],.efoTip [hidden]{display:none!important}
.efo button,.efo input{font:inherit;color:inherit}
.efoBar{display:flex;align-items:center;gap:8px 14px;flex-wrap:wrap;min-width:0}
.efoSearch{position:relative;flex:1 1 300px;min-width:0;max-width:560px;display:flex;align-items:center;margin:0}
.efoSearch input{width:100%;height:36px;border:1px solid var(--line,#e4ddd0);background:var(--card,#fffefb);border-radius:999px;padding:0 36px;font-size:13px;font-weight:500;letter-spacing:0;transition:border-color .2s,box-shadow .2s,background .2s}
.efoSearch input{text-overflow:ellipsis}.efoSearch input::placeholder{color:var(--ink45,#938c80);font-weight:400;text-overflow:ellipsis}
.efoSearch input:focus{outline:none;border-color:var(--gold,#a9823f);background:#fff;box-shadow:0 0 0 3px var(--goldSoft,#f0e6cd)}
.efoMag{position:absolute;left:13px;color:var(--ink45,#938c80);pointer-events:none}
.efoClear{position:absolute;right:6px;width:26px;height:26px;border:0;background:transparent;border-radius:50%;color:var(--ink45,#938c80);font-size:17px;line-height:1;padding:0;display:grid;place-items:center}
.efoClear:hover{background:var(--paper2,#ebe5d9);color:var(--ink,#1c1a17)}
.efoBusy{display:inline-flex;align-items:center;gap:7px;color:var(--ink70,#5b554c);font-size:11.5px;white-space:nowrap}
.efo .spin{width:12px;height:12px;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:efoSpin .7s linear infinite;flex:0 0 12px;display:inline-block}
@keyframes efoSpin{to{transform:rotate(360deg)}}
.efoCount{margin-left:auto;color:var(--ink45,#938c80);font-size:11.5px;font-variant-numeric:tabular-nums;white-space:nowrap}
.efoLive{display:inline-flex;align-items:center;gap:6px;color:var(--ink45,#938c80);font-size:11.5px;white-space:nowrap}
.efoLive i{width:7px;height:7px;border-radius:50%;background:var(--ink25,#c4bdb0);flex:0 0 7px}
.efoLive[data-s=live] i{background:var(--sage,#5f7a5b);animation:efoPulse 2.4s ease-out infinite}
.efoLive[data-s=slow] i{background:var(--gold2,#caa861)}
@keyframes efoPulse{0%{box-shadow:0 0 0 0 rgba(95,122,91,.4)}70%,100%{box-shadow:0 0 0 6px rgba(95,122,91,0)}}
.efoFilters{display:flex;align-items:center;gap:8px 16px;flex-wrap:wrap;min-width:0}
.efoChips{display:flex;flex-wrap:wrap;gap:5px;min-width:0}
.efoFc{border:1px solid var(--line,#e4ddd0);background:var(--card2,#faf7f1);border-radius:999px;padding:3px 11px;font-size:11.5px;font-weight:600;color:var(--ink70,#5b554c);white-space:nowrap;transition:background .2s,color .2s,border-color .2s}
.efoFc:hover{border-color:var(--ink25,#c4bdb0);color:var(--ink,#1c1a17)}
.efoFc[aria-pressed=true]{background:var(--velvet,#221f1b);border-color:var(--velvet,#221f1b);color:#fff}
.efoDates{display:inline-flex;align-items:center;gap:6px;color:var(--ink45,#938c80);font-size:11.5px}
.efoDates label{display:inline-flex;align-items:center;gap:5px}
.efoDates input{border:1px solid var(--line,#e4ddd0);background:var(--card,#fffefb);border-radius:8px;padding:3px 7px;font-size:11.5px;min-width:0;width:122px}
.efoDates input:focus{outline:none;border-color:var(--gold,#a9823f)}
.efoDx{border:0;background:transparent;color:var(--ink45,#938c80);font-size:11.5px;padding:3px 6px;border-radius:6px}.efoDx:hover{background:var(--paper2,#ebe5d9);color:var(--ink,#1c1a17)}
.efoSeg{display:inline-flex;border:1px solid var(--line,#e4ddd0);border-radius:9px;overflow:hidden;margin-left:auto}
.efoSeg button{border:0;background:var(--card,#fffefb);padding:4px 11px;font-size:11px;font-weight:650;color:var(--ink70,#5b554c)}
.efoSeg button[aria-pressed=true]{background:var(--velvet,#221f1b);color:#fff}
.efoNote{margin:0;display:grid;gap:2px;color:var(--ink70,#5b554c);font-size:12px;padding:0 2px}
.efoNote span:before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--gold2,#caa861);margin-right:8px;vertical-align:1px}
.efoListW{position:relative;min-width:0}
.efoPillW{position:sticky;top:10px;height:0;z-index:5;display:flex;justify-content:center;pointer-events:none;overflow:visible}
.efoPill{pointer-events:auto;border:0;border-radius:999px;background:var(--velvet,#221f1b);color:#f6f1e6;padding:6px 14px;font-size:11.5px;font-weight:700;box-shadow:0 8px 24px rgba(20,16,10,.22);animation:efoPillIn .3s cubic-bezier(.2,.8,.2,1) both}
.efoPill:hover{background:var(--velvet2,#2d2924)}
@keyframes efoPillIn{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:none}}
.efoList{display:grid;gap:8px;min-width:0;transition:opacity .25s ease}
.efoList.dim{opacity:.5}
.efoRow{position:relative;display:grid;gap:8px 16px;align-items:center;padding:10px 14px;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-radius:12px;min-width:0;cursor:pointer;transition:border-color .2s,box-shadow .25s,background .25s;grid-template-columns:var(--efoTW) minmax(150px,1fr) 124px 116px 88px 100px 46px;grid-template-areas:"th main when st dur stat qr"}
.efoRow:hover,.efoRow:focus-within{border-color:var(--ink25,#c4bdb0);background:#fff;box-shadow:0 1px 2px rgba(30,26,20,.04),0 8px 22px rgba(30,26,20,.07)}
.efoRow.efoIn{animation:efoIn .34s cubic-bezier(.2,.8,.2,1) both;animation-delay:calc(var(--k,0) * 22ms)}
@keyframes efoIn{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:none}}
.efoRow.efoFresh:before{content:"";position:absolute;inset:0;border-radius:inherit;background:var(--goldSoft,#f0e6cd);opacity:0;pointer-events:none;animation:efoWash 1.9s ease-out}
@keyframes efoWash{0%{opacity:.95}100%{opacity:0}}
.efoThumbsW{grid-area:th;min-width:0}
.efoThumbs{display:flex;flex-wrap:wrap;align-items:center;gap:6px;min-width:0}
.efoZ{position:relative;display:block;padding:0;margin:0;border:1px solid var(--line,#e4ddd0);background:#fff;border-radius:9px;overflow:hidden;cursor:pointer;flex:0 0 auto;color:var(--ink25,#c4bdb0);line-height:0}
.efoZ.sealZoomed{border-color:var(--gold2,#caa861);box-shadow:0 0 0 1px var(--gold2,#caa861),0 8px 22px rgba(30,26,20,.28)}
.efoTh{width:var(--efoT);height:var(--efoT)}
.efoTh img{display:block;width:100%;height:100%;object-fit:contain;padding:3px}
.efoTh.ph{background:var(--card2,#faf7f1);display:grid;place-items:center}
.efoTh[data-n]:after{content:attr(data-n);position:absolute;right:2px;bottom:2px;min-width:13px;height:13px;padding:0 3px;border-radius:7px;background:rgba(34,31,27,.72);color:#fff;font:700 8.5px/13px var(--sans,system-ui,sans-serif);text-align:center}
.efoThumbs:not(.open) .efoTh.over{display:none}
.efoMore{border:1px dashed var(--ink25,#c4bdb0);background:var(--card2,#faf7f1);border-radius:9px;height:var(--efoT);min-width:34px;padding:0 8px;font-size:11.5px;font-weight:700;color:var(--ink70,#5b554c);transition:background .2s,border-color .2s}
.efoMore:hover,.efoMore:focus-visible{background:var(--goldSoft,#f0e6cd);border-color:var(--gold2,#caa861);color:var(--ink,#1c1a17)}
.efoMain{grid-area:main;min-width:0;display:grid;gap:2px}
.efoOpen{display:flex;align-items:baseline;gap:4px 10px;flex-wrap:wrap;border:0;background:transparent;padding:0;margin:0;text-align:left;min-width:0;border-radius:6px;cursor:pointer}
.efoOpen:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:3px}
.efoNum{font:650 13px var(--mono,ui-monospace,Menlo,Consolas,monospace);letter-spacing:0;white-space:nowrap}
.efoRow:hover .efoNum{text-decoration:underline;text-decoration-color:var(--gold2,#caa861);text-underline-offset:3px}
.efoCust{color:var(--ink70,#5b554c);font-size:12.5px;font-weight:550;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%}
.efoSub{color:var(--ink45,#938c80);font-size:11.5px;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.efoMk{background:var(--goldSoft,#f0e6cd);color:inherit;border-radius:3px;padding:0 1px;box-shadow:0 0 0 1px var(--goldSoft,#f0e6cd)}
.efoMeta{display:contents}
.efoWhen{grid-area:when;display:grid;gap:1px;min-width:0;font-variant-numeric:tabular-nums}
.efoWhen .d{font-weight:650;font-size:12px;white-space:nowrap}.efoWhen .t{color:var(--ink45,#938c80);font-size:11.5px;white-space:nowrap;display:flex;align-items:center;gap:6px}
.efoNow{width:7px;height:7px;border-radius:50%;background:var(--sage,#5f7a5b);flex:0 0 7px;animation:efoPulse 2.4s ease-out infinite}
.efoSt{grid-area:st;display:flex;align-items:center;gap:5px;min-width:0}
.efoChip{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line,#e4ddd0);border-radius:999px;padding:2px 9px;font-size:11px;font-weight:600;color:var(--ink70,#5b554c);background:var(--card2,#faf7f1);white-space:nowrap;max-width:100%;overflow:hidden;text-overflow:ellipsis}
.efoChip i{width:6px;height:6px;border-radius:50%;background:var(--ink25,#c4bdb0);flex:0 0 6px}
.efoSt em{font-style:normal;font-size:11px;color:var(--ink45,#938c80)}
.efoDur{grid-area:dur;display:grid;gap:1px;font-variant-numeric:tabular-nums;white-space:nowrap}
.efoDur b{font-weight:650;font-size:13px}.efoDur small{font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45,#938c80);font-weight:700}
.efoStat{grid-area:stat;justify-self:start;border-radius:999px;padding:2px 9px;font-size:11px;font-weight:700;white-space:nowrap}
.efoStat[data-s=completed]{background:var(--sageSoft,#e7eddf);color:#46613f}
.efoStat[data-s=reopened]{background:var(--goldSoft,#f0e6cd);color:#7a5a1d}
.efoStat[data-s=issue]{background:var(--claySoft,#f4e3dc);color:var(--clay,#b0563f)}
.efoStat[data-s=handled]{background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c)}
.efoQrW{grid-area:qr;justify-self:end}
.efoQr{width:44px;height:44px;padding:2px;display:grid;place-items:center}
.efoQr img{display:block;width:100%;height:100%}
.efoState{display:grid;justify-items:center;gap:6px;text-align:center;padding:44px 16px;border:1px dashed var(--line,#e4ddd0);border-radius:12px;color:var(--ink70,#5b554c)}
.efoState b{font-size:13.5px;font-weight:650;color:var(--ink,#1c1a17)}.efoState p{margin:0;font-size:12px;color:var(--ink45,#938c80);max-width:46ch}
.efoState .spin{width:16px;height:16px;flex-basis:16px}
.efoState .efoWait{display:flex;align-items:center;gap:9px;font-size:12.5px}
.efoState svg{color:var(--ink25,#c4bdb0)}
.efoBtn{border:1px solid var(--line,#e4ddd0);background:var(--card,#fffefb);border-radius:9px;padding:5px 14px;font-size:12px;font-weight:650;color:var(--ink,#1c1a17);margin-top:4px}
.efoBtn:hover{border-color:var(--ink25,#c4bdb0);background:#fff}
.efoFoot{display:flex;align-items:center;justify-content:center;gap:9px;min-height:30px;padding:8px 0 2px;color:var(--ink45,#938c80);font-size:11.5px}
.efoSent{height:1px}
.efoTip{position:fixed;left:0;top:0;z-index:960;pointer-events:none;width:max-content;max-width:min(360px,calc(100vw - 16px));background:var(--card,#fffefb);color:var(--ink70,#5b554c);border:1px solid var(--line,#e4ddd0);border-radius:12px;padding:11px 13px;font:11.5px/1.4 var(--sans,system-ui,sans-serif);box-shadow:0 10px 26px rgba(30,26,20,.13),0 1px 3px rgba(30,26,20,.07);opacity:0;visibility:hidden;transform:translateY(3px);transition:opacity .14s ease,transform .14s ease,visibility 0s .14s;display:grid;gap:7px}
.efoTip.on{opacity:1;visibility:visible;transform:none;transition:opacity .14s ease,transform .14s ease}
.efoTipH{display:flex;align-items:baseline;justify-content:space-between;gap:14px}.efoTipH b{font:650 13px var(--mono,ui-monospace,Menlo,monospace);color:var(--ink,#1c1a17)}.efoTipH span{font-size:10.5px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45,#938c80)}
.efoTipW{color:var(--ink45,#938c80)}
.efoTipG{display:grid;grid-template-columns:auto 1fr;gap:2px 16px;color:var(--ink45,#938c80)}.efoTipG b{color:var(--ink,#1c1a17);font-weight:650;text-align:right;font-variant-numeric:tabular-nums}
.efoTipS{display:grid;gap:2px;border-top:1px solid var(--line2,#efe9dd);padding-top:6px}
.efoTipR{display:grid;grid-template-columns:70px auto 1fr;gap:10px;color:var(--ink45,#938c80);font-variant-numeric:tabular-nums}.efoTipR b{color:var(--ink,#1c1a17);font-weight:650}.efoTipR span:last-child{text-align:right}
.efoTipI{color:var(--clay,#b0563f)}
.efoTipF{color:var(--ink45,#938c80);font-size:10.5px;border-top:1px solid var(--line2,#efe9dd);padding-top:6px}
.efoTipP{display:grid;grid-template-columns:repeat(auto-fill,minmax(64px,1fr));gap:8px;max-width:340px}
.efoTipP div{display:grid;gap:3px;justify-items:center;text-align:center;min-width:0}
.efoTipP i{display:grid;place-items:center;width:56px;height:56px;background:#fff;border:1px solid var(--line2,#efe9dd);border-radius:9px;overflow:hidden;color:var(--ink25,#c4bdb0)}.efoTipP img{width:100%;height:100%;object-fit:contain;padding:3px}
.efoTipP span{font-size:10.5px;color:var(--ink45,#938c80);max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
@container efo (max-width:979px){
 .efoRow{grid-template-columns:minmax(0,1fr) auto auto;grid-template-areas:"main th qr" "meta meta qr";gap:6px 16px}
 .efoMeta{grid-area:meta;display:flex;flex-wrap:wrap;align-items:center;gap:4px 14px;min-width:0}
 .efoMeta>*{grid-area:auto}
 .efoWhen{display:flex;gap:6px;align-items:center;flex-wrap:nowrap}.efoWhen .t:before{content:"·";margin-right:6px;color:var(--ink25,#c4bdb0)}
 .efoDur{display:flex;align-items:baseline;gap:6px}.efoDur small{order:2;text-transform:none;letter-spacing:0;font-weight:500;font-size:11px}
 .efoThumbsW{align-self:start;justify-self:end}.efoQrW{align-self:start}.efoThumbs{justify-content:flex-end}
}
@container efo (max-width:559px){
 .efo{--efoT:44px}
 .efoRow{grid-template-columns:minmax(0,1fr) auto;grid-template-areas:"main qr" "th th" "meta meta";padding:11px 12px}
 .efoMeta{display:grid;grid-template-columns:max-content minmax(0,1fr);gap:6px 14px;align-items:center}.efoMeta>*{justify-self:start}
 .efoThumbsW{justify-self:start}.efoThumbs{justify-content:flex-start}
 .efoSearch input{font-size:12px;padding:0 32px 0 34px}
 .efoSearch{flex-basis:100%;max-width:none}.efoCount{margin-left:0}.efoSeg{margin-left:0}
 .efoDates input{width:112px}
 .efoCust{white-space:normal}
}
@media (prefers-reduced-motion:reduce){.efo *,.efoTip{transition:none!important;animation:none!important}}`;
    doc.head.appendChild(s);
  }

  /* ═════════ one list ═════════ */
  function mount(host, opts) {
    opts = opts || {};
    if (!host || !doc) throw new Error("EfficiencyOrders.mount needs an element");
    if (host._efo) host._efo.unmount();
    style();
    const M = {
      dead: false, name: S_(opts.name).trim(), q: S_(opts.query).trim(), station: S_(opts.station), from: "", to: "", sort: SORTS.some(s => s[0] === opts.sort) ? opts.sort : "newest",
      rows: [], lo: 0, hi: 0, byRid: new Map(), els: new Map(), next: "", total: 0, scanned: 0, searched: null, notes: [], partial: false, serverSort: "", found: true,
      busy: false, more: false, err: null, errMore: null, gen: 0, ctl: null, moreCtl: null, pollCtl: null, pollT: 0, fails: 0, slow: false, waiting: false, inView: true, skew: 0,
      buffer: [], expanded: new Set(), seen: new Set(), stations: new Set(CORE), loadedOnce: false, authStop: false, debounce: 0, limit: Math.max(1, Math.min(100, +opts.limit || options.pageSize))
    };
    if (opts.range) { M.from = S_(opts.range.from); M.to = S_(opts.range.to); }
    const pollMs = opts.pollMs == null ? options.pollMs : +opts.pollMs;
    const cap = () => Math.max(1, N(options.capThumbs) || 3);
    const nowMs = () => { const A = shell(); try { if (A && typeof A.now === "function") { const n = +A.now(); if (n > 0) return n; } } catch (_) {} return Date.now() + M.skew; };
    const showFilters = opts.filters !== false, showDates = showFilters && opts.dateFields !== false;

    /* ── the skeleton, built once; everything after updates in place ── */
    const root_ = el("div", "efo"); root_.setAttribute("data-s", "load");
    root_.innerHTML = `
<div class="efoBar">
  <form class="efoSearch" role="search" autocomplete="off">${MAG}<input type="text" name="efoq" inputmode="search" enterkeyhint="search" aria-label="Search this person's orders" placeholder="Search orders, customers, pieces, stations or dates" autocomplete="off" autocapitalize="off" spellcheck="false"><button type="button" class="efoClear" aria-label="Clear search" hidden>×</button></form>
  <span class="efoBusy" role="status" aria-live="polite" hidden><i class="spin" aria-hidden="true"></i><span class="efoBusyT"></span></span>
  <span class="efoCount" aria-live="polite"></span>
  <span class="efoLive" data-s="live" title="The first page is read again every few seconds while this list is on screen"><i></i><span class="efoLiveT">Live</span></span>
</div>
<div class="efoFilters" ${showFilters ? "" : "hidden"}>
  <div class="efoChips" role="group" aria-label="Station"></div>
  <div class="efoDates" role="group" aria-label="Dates" ${showDates ? "" : "hidden"}><label>From <input type="date" name="efofrom" aria-label="From day"></label><label>To <input type="date" name="efoto" aria-label="To day"></label><button type="button" class="efoDx" hidden>Clear dates</button></div>
  <div class="efoSeg" role="group" aria-label="Sort">${SORTS.map(s => `<button type="button" data-sort="${s[0]}" aria-pressed="false">${s[1]}</button>`).join("")}</div>
</div>
<div class="efoNote" role="status" hidden></div>
<div class="efoListW">
  <div class="efoPillW"><button type="button" class="efoPill" hidden></button></div>
  <div class="efoTop"></div>
  <div class="efoList"></div>
  <div class="efoState" hidden></div>
  <div class="efoFoot" hidden></div>
  <div class="efoSent"></div>
</div>`;
    host.textContent = ""; host.appendChild(root_);
    const $ = s => root_.querySelector(s);
    const E = { form: $(".efoSearch"), input: $(".efoSearch input"), clear: $(".efoClear"), busy: $(".efoBusy"), busyT: $(".efoBusyT"), count: $(".efoCount"), live: $(".efoLive"), liveT: $(".efoLiveT"), filters: $(".efoFilters"), chips: $(".efoChips"),
      dates: $(".efoDates"), dFrom: $('[name="efofrom"]'), dTo: $('[name="efoto"]'), dx: $(".efoDx"), seg: $(".efoSeg"), note: $(".efoNote"), pill: $(".efoPill"), top: $(".efoTop"), list: $(".efoList"), state: $(".efoState"), foot: $(".efoFoot"), sent: $(".efoSent") };
    E.input.value = M.q;

    /* ── requests ── */
    const sandboxOn = () => { try { return typeof opts.sandbox === "function" ? !!opts.sandbox() : !!opts.sandbox; } catch (_) { return false; } };
    const shell = () => { const A = root.Efficiency && root.Efficiency.api; return A && typeof A.call === "function" ? A : null; };
    function failure(status, j, offline) {
      const srv = j && (j.error || j.message) ? String(j.error || j.message).slice(0, 160) : "";
      let msg;
      if (offline) msg = "The connection dropped. Trying again.";
      else if (status === 401) msg = "The passcode was not accepted. Open the Employee efficiency tab and enter it again.";
      else if (status === 403) msg = srv || "The manager passcode is not set up for this screen.";
      else if (status === 429) msg = "Too many requests just now. Trying again shortly.";
      else if (status >= 500) msg = "The orders could not be read just now.";
      else msg = srv ? `${srv}.` : `The service answered ${status || "with an error"}.`;
      return Object.assign(new Error(msg), { status: status || 0, auth: status === 401, locked: status === 403, limited: status === 429 });
    }
    async function call(body, signal) {
      if (sandboxOn()) body.sandbox = true;
      if (typeof opts.call === "function") return opts.call(body, signal);
      const A = shell(); if (A) return A.call(body, { signal });
      let key = ""; try { key = root.sessionStorage.getItem(KEY_STORE) || ""; } catch (_) {}
      if (!key) throw Object.assign(new Error("Open the Employee efficiency tab and enter the manager passcode to see the orders."), { auth: true, status: 401 });
      let res; try { res = await fetch(root.location.origin + "/.netlify/functions/employeeEfficiency", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(Object.assign({}, body, { key })), cache: "no-store", signal }); } catch (e) { if (aborted(e)) throw e; throw failure(0, null, true); }
      const txt = await res.text(); let j = null; try { j = JSON.parse(txt); } catch (_) {}
      if (!res.ok || !j || j.ok === false) throw failure(res.status, j);
      return j;
    }
    const bodyFor = cursor => { const b = { op: "personOrders", name: M.name, q: M.q, cursor: cursor || "", limit: M.limit }; if (M.from) b.from = M.from; if (M.to) b.to = M.to; if (M.station) b.station = M.station; if (M.sort !== "newest") b.sort = M.sort; return b; };
    const hasFilters = () => !!(M.q || M.station || M.from || M.to);
    const abort = c => { try { c && c.abort(); } catch (_) {} };
    const ctlNew = () => (root.AbortController ? new root.AbortController() : null);

    /** The first page, for a new search, filter, sort or a retry. The rows now on screen stay (dimmed) until the answer arrives. */
    async function load() {
      clearTimeout(M.debounce); M.debounce = 0;
      if (M.dead) return;
      const gen = ++M.gen; abort(M.ctl); abort(M.moreCtl); abort(M.pollCtl); M.pollCtl = null; M.moreCtl = null; M.more = false; M.errMore = null;
      M.buffer = []; hideTip(true);
      const ctl = M.ctl = ctlNew(); M.busy = true; M.err = null; M.authStop = false; paint();
      try {
        const a = norm(await call(bodyFor(""), ctl && ctl.signal));
        if (gen !== M.gen || M.dead) return;
        take(a, true); M.loadedOnce = true; M.fails = 0; M.slow = false;
      } catch (e) {
        if (gen !== M.gen || aborted(e) || M.dead) return;
        M.err = e; if (e.auth || e.locked) M.authStop = true;
      } finally { if (gen === M.gen && !M.dead) { M.busy = false; paint(); schedulePoll(); maybeMore(); } }
    }
    /** An answer for the first page: it replaces the list. */
    function take(a, reset) {
      M.total = a.total; M.scanned = a.scanned; M.searched = a.searched; M.notes = a.notes; M.partial = a.partial; M.serverSort = a.sort; M.sortSupport = M.sortSupport || !!a.sort; M.found = a.found; M.next = a.next;
      if (a.now) M.skew = a.now - Date.now();
      if (reset) { M.rows = []; M.byRid = new Map(); M.seen = new Set(); M.lo = 0; M.hi = 0; }
      merge(a.orders, false);
    }
    /** New orders join the list: at the end (a page) or at the front (arrivals, newest first). Each order keeps `seq`, its place in the SERVER's order
     *  (its last action, not its first), so Newest is that order and a sort by working time can always come back to it. */
    function merge(list, live, front) {
      const fresh = [];
      for (const o of list) {
        const had = M.byRid.get(o.rid);
        if (had) { o.seq = had.seq; M.rows[M.rows.indexOf(had)] = o; } else fresh.push(o);
        M.byRid.set(o.rid, o);
        for (const s of o.stations) M.stations.add(s);
      }
      if (front) { for (let i = fresh.length - 1; i >= 0; i--) fresh[i].seq = --M.lo; M.rows = fresh.concat(M.rows); } else for (const o of fresh) { o.seq = M.hi++; M.rows.push(o); }
      if (live) for (const o of fresh) M.live.add(o.rid);
      if (M.sort !== "newest") order();
    }
    M.live = new Set();
    /** Newest is the server's order (by seq, never by the first action's time); slowest and fastest compare the working time, unknown times last. */
    function order() {
      if (M.sort === "newest") M.rows.sort((a, b) => a.seq - b.seq);
      else { const dir = M.sort === "slowest" ? -1 : 1, k = o => (o.durationMs > 0 ? o.durationMs : null); M.rows.sort((a, b) => { const x = k(a), y = k(b); if (x == null && y == null) return b.at - a.at; if (x == null) return 1; if (y == null) return -1; return (x - y) * dir || b.at - a.at; }); }
    }
    async function loadMore() {
      if (M.dead || M.more || M.busy || !M.next || M.err) return;
      const gen = M.gen, ctl = M.moreCtl = ctlNew(), cursor = M.next; M.more = true; M.errMore = null; paintFoot();
      try {
        const a = norm(await call(bodyFor(cursor), ctl && ctl.signal));
        if (gen !== M.gen || M.dead) return;
        M.next = a.next; M.total = a.total || M.total; M.scanned = a.scanned || M.scanned; merge(a.orders.filter(o => !M.byRid.has(o.rid)), false);
      } catch (e) { if (gen !== M.gen || aborted(e) || M.dead) return; M.errMore = e; if (e.auth || e.locked) M.authStop = true; }
      finally { if (gen === M.gen && !M.dead) { M.more = false; paint(); maybeMore(); } }
    }

    /* ── live: the first page, again and again, while the list is in sight ── */
    const inSight = () => !M.dead && doc.visibilityState !== "hidden" && root_.isConnected && root_.getClientRects().length > 0 && M.inView !== false;
    const pollingOn = () => pollMs > 0 && !M.authStop;
    function schedulePoll() {
      clearTimeout(M.pollT); M.pollT = 0; M.waiting = false;
      // a box taken out of the page without unmount() is let go after a while: no timer, listener or card is left behind
      if (!M.dead && !root_.isConnected && !M.orphanT) M.orphanT = setTimeout(() => { M.orphanT = 0; if (!M.dead && !root_.isConnected) unmount(); }, options.orphanMs);
      if (M.dead || !pollingOn()) { paintLive(); return; }
      if (!inSight()) { M.waiting = true; paintLive(); return; }
      const wait = M.fails ? Math.min(options.maxBackoffMs, pollMs * Math.pow(2, M.fails)) : pollMs;
      M.pollT = setTimeout(poll, wait); paintLive();
    }
    async function poll() {
      M.pollT = 0; if (M.dead) return;
      if (!inSight() || !pollingOn()) { schedulePoll(); return; }
      if (M.busy || M.more || M.err) { schedulePoll(); return; }
      const gen = M.gen, ctl = M.pollCtl = ctlNew();
      try {
        const a = norm(await call(bodyFor(""), ctl && ctl.signal));
        if (gen !== M.gen || M.dead) return;
        M.fails = 0; M.slow = false; absorb(a);
      } catch (e) {
        if (gen !== M.gen || aborted(e) || M.dead) return;
        M.fails++; M.slow = true; if (e.auth || e.locked) M.authStop = true;
      } finally { if (gen === M.gen && !M.dead) { M.pollCtl = null; paintLive(); schedulePoll(); } }
    }
    /** What a live check found: new orders arrive (at the top, or behind the pill), changed orders are redrawn where they stand. */
    function absorb(a) {
      M.total = a.total; M.scanned = a.scanned; M.notes = a.notes; M.partial = a.partial; M.searched = a.searched; if (a.now) M.skew = a.now - Date.now();
      const changed = [], fresh = [], first = a.orders;
      for (const o of first) {
        const had = M.byRid.get(o.rid);
        if (had) { if (sigOf(had) !== sigOf(o)) changed.push(o); }
        else if (!M.buffer.some(b => b.rid === o.rid)) fresh.push(o);
      }
      const hold = !!fresh.length && away();
      if (hold) { M.buffer = fresh.concat(M.buffer); if (changed.length) merge(changed, false); paintList(null); }
      else if (M.sort === "newest" && (fresh.length || changed.length)) {
        // the first page is the server's newest: it goes to the top in the server's order (an order worked again moves up too)
        const flip = snap(), top = new Set(first.map(o => o.rid));
        for (const o of fresh) M.live.add(o.rid);
        for (const o of first) { M.byRid.set(o.rid, o); for (const s of o.stations) M.stations.add(s); }
        M.rows = first.concat(M.rows.filter(r => !top.has(r.rid))); M.lo = 0; M.hi = M.rows.length; M.rows.forEach((r, i) => { r.seq = i; });
        paintList(flip);
      } else { const flip = fresh.length ? snap() : null; if (changed.length) merge(changed, false); if (fresh.length) merge(fresh, true, true); paintList(flip); }
      paintBar(); paintNote(); paintPill(); paintState(); paintFoot();
      if (opts.onState) emit();
    }
    function addLive(list) { const flip = snap(); merge(list, true, true); paintList(flip); paintBar(); paintState(); paintPill(); if (opts.onState) emit(); }
    /** The list's top is above what is on screen (the person scrolled down). */
    function away() {
      const r = E.top.getBoundingClientRect(); if (!r.height && !r.width && !root_.getClientRects().length) return false;
      let clip = 0, cs = root_.parentElement;
      for (; cs && cs !== doc.body && cs !== doc.documentElement; cs = cs.parentElement) { const st = root.getComputedStyle(cs); if (/auto|scroll/.test(st.overflowY) && cs.scrollHeight > cs.clientHeight + 1) { clip = cs.getBoundingClientRect().top; break; } }
      return r.top < clip - options.awayPx;
    }
    function flushBuffer(scroll) {
      if (!M.buffer.length) return;
      const list = M.buffer; M.buffer = [];
      addLive(list.filter(o => !M.byRid.has(o.rid)));
      if (scroll) { try { root_.scrollIntoView({ block: "start", behavior: still() ? "auto" : "smooth" }); } catch (_) {} }
    }
    const onScroll = () => { if (M.buffer.length && !M.scrollRaf) M.scrollRaf = root.requestAnimationFrame(() => { M.scrollRaf = 0; if (M.buffer.length && !away()) flushBuffer(false); }); };
    root.addEventListener("scroll", onScroll, { passive: true, capture: true });
    const onVis = () => { if (M.dead) return; if (doc.visibilityState === "hidden") { hideTip(true); paintLive(); } else resume(); };
    function resume() { if (M.waiting || !M.pollT) { clearTimeout(M.pollT); M.pollT = 0; if (inSight() && pollingOn() && !M.busy) { M.pollT = setTimeout(poll, 0); } paintLive(); } }
    doc.addEventListener("visibilitychange", onVis);
    let ioView = null, ioMore = null;
    if (root.IntersectionObserver) {
      ioView = new root.IntersectionObserver(es => { const was = M.inView; M.inView = es[es.length - 1].isIntersecting; if (M.inView && !was) resume(); else if (!M.inView) paintLive(); }, { rootMargin: "120px" });
      ioView.observe(root_);
      ioMore = new root.IntersectionObserver(es => { M.sentSeen = es[es.length - 1].isIntersecting; if (M.sentSeen) loadMore(); }, { rootMargin: "360px" });
      ioMore.observe(E.sent);
    }
    /** After a page lands the end of the list may still be in view (a tall screen, short pages): ask the observer again. */
    function maybeMore() { if (!ioMore) return; if (M.next && !M.more && !M.busy && !M.err) { ioMore.unobserve(E.sent); ioMore.observe(E.sent); } }

    /* ── painting ── */
    function paint() { paintBar(); paintNote(); paintList(null); paintState(); paintFoot(); paintPill(); paintChips(); paintLive(); if (opts.onState) emit(); }
    function emit() { try { opts.onState({ total: M.total, scanned: M.scanned, rows: M.rows.length, loading: M.busy, error: M.err ? M.err.message : "", query: M.q }); } catch (_) {} }
    function paintBar() {
      const ph = M.busy && !M.rows.length;
      E.busy.hidden = !(M.busy && M.rows.length); setText(E.busyT, M.q ? "Searching…" : "Updating…");
      root_.setAttribute("aria-busy", M.busy ? "true" : "false");
      let c = "";
      if (!ph && !M.err && M.loadedOnce) { const n = M.total || 0, all = M.scanned || n; c = hasFilters() ? `${nf(n)} of ${nf(Math.max(all, n))} order${all === 1 ? "" : "s"}` : `${nf(all)} order${all === 1 ? "" : "s"}`; }
      setText(E.count, c);
      E.clear.hidden = !E.input.value;
    }
    function paintLive() {
      const s = M.dead ? "paused" : M.authStop ? "paused" : M.slow ? "slow" : pollMs <= 0 || M.waiting || !inSight() ? "paused" : "live";
      E.live.dataset.s = s; setText(E.liveT, s === "live" ? "Live" : s === "slow" ? "Reconnecting…" : "Paused");
    }
    function paintNote() {
      const lines = M.notes.slice();
      if (M.partial) lines.push("Some history could not be read just now, so this list may be incomplete.");
      if (M.sort !== "newest" && M.next && M.serverSort !== M.sort) lines.push("Sorted among the orders loaded so far. Scroll to load more.");
      const html = lines.map(l => `<span>${esc(l)}</span>`).join("");
      E.note.hidden = !lines.length; setHtml(E.note, html);
    }
    function paintChips() {
      if (!showFilters) return;
      const keys = [...CORE, ...[...M.stations].map(dispSt).filter((s, i, a) => s && !CORE.includes(s) && a.indexOf(s) === i)];
      if (M.station && !keys.includes(dispSt(M.station))) keys.push(dispSt(M.station));
      const html = [["", "All stations"], ...keys.map(k => [k, stName(k)])].map(([k, l]) => `<button type="button" class="efoFc" data-st="${esc(k)}" aria-pressed="${M.station === k}">${esc(l)}</button>`).join("");
      setHtml(E.chips, html);
      for (const b of E.seg.querySelectorAll("button")) b.setAttribute("aria-pressed", String(b.dataset.sort === M.sort));
      if (E.dFrom.value !== M.from) E.dFrom.value = M.from; if (E.dTo.value !== M.to) E.dTo.value = M.to;
      E.dx.hidden = !(M.from || M.to);
    }
    function paintPill() {
      const n = M.buffer.length; E.pill.hidden = !n; if (n) setText(E.pill, `↑ ${n} new`);
    }
    function paintFoot() {
      let h = "";
      if (M.more) h = `<i class="spin" aria-hidden="true"></i><span role="status">Loading more orders…</span>`;
      else if (M.errMore) h = `<span>${esc(M.errMore.message)}</span><button type="button" class="efoBtn" data-act="more">Retry</button>`;
      else if (M.next && M.rows.length && !ioMore) h = `<button type="button" class="efoBtn" data-act="more">Show more orders</button>`;
      else if (!M.next && M.rows.length > 8 && !M.busy) h = `<span>All ${nf(M.rows.length)} orders shown</span>`;
      E.foot.hidden = !h; setHtml(E.foot, h);
    }
    function paintState() {
      let h = "";
      if (M.err && !M.rows.length) {
        h = `<b>The orders could not be read</b><p>${esc(M.err.message)}</p><button type="button" class="efoBtn" data-act="retry">Retry</button>`;
      } else if (M.err) {
        h = `<b>The list could not be updated</b><p>${esc(M.err.message)}</p><button type="button" class="efoBtn" data-act="retry">Retry</button>`;
      } else if (M.busy && !M.rows.length) {
        h = `<div class="efoWait" role="status"><i class="spin" aria-hidden="true"></i><span>${M.q ? "Searching orders…" : "Loading orders…"}</span></div>`;
      } else if (!M.rows.length && M.loadedOnce) {
        if (hasFilters()) {
          h = `${PH}<b>No orders match</b><p>${M.q ? `Nothing matches “${esc(M.q)}”${M.station || M.from || M.to ? " with these filters" : ""}. Try a customer, an order number, a piece, a station or a date.` : M.from || M.to ? "No order was handled on these days." : "Nothing was handled at this station."}</p><button type="button" class="efoBtn" data-act="clear">Clear search and filters</button>`;
        } else h = `${PH}<b>This person has not handled an order yet</b><p>Orders appear here as soon as one is scanned at a station.</p>`;
      }
      E.state.hidden = !h; setHtml(E.state, h);
    }

    /* the rows: reconciled by order id, so a poll or a page touches only what changed */
    const rowSig = (o) => sigOf(o) + "|" + M.q + "|" + (M.expanded.has(o.rid) ? 1 : 0) + "|" + (nowMs() - o.lastAt < ACTIVE_MS ? 1 : 0);
    function snap() { const m = new Map(); for (const [rid, e] of M.els) { const r = e.getBoundingClientRect(); if (r.bottom > -40 && r.top < (root.innerHeight || 800) + 40) m.set(rid, r.top); } return m; }
    function paintList(flip) {
      const list = E.list, ws = words(M.q), keep = new Set(), added = [];
      let ref = list.firstElementChild, k = 0;
      for (const o of M.rows) {
        keep.add(o.rid);
        let e = M.els.get(o.rid), isNew = false;
        if (!e) { e = makeRow(); M.els.set(o.rid, e); isNew = true; }
        fillRow(e, o, ws);
        if (e === ref) ref = ref.nextElementSibling; else list.insertBefore(e, ref);
        if (isNew) { added.push(e); if (!M.seen.has(o.rid) || M.live.has(o.rid)) { enter(e, o, k++); } M.seen.add(o.rid); }
      }
      for (const [rid, e] of [...M.els]) if (!keep.has(rid)) { if (M.hoverRow === e) hideTip(true); e.remove(); M.els.delete(rid); }
      list.classList.toggle("dim", M.busy && M.rows.length > 0);
      if (flip && flip.size && !still()) for (const [rid, top] of flip) { const e = M.els.get(rid); if (!e || !e.animate) continue; const dy = top - e.getBoundingClientRect().top; if (Math.abs(dy) > 1) e.animate([{ transform: `translateY(${dy}px)` }, { transform: "none" }], { duration: 320, easing: "cubic-bezier(.2,.8,.2,1)" }); }
      M.live.clear(); hydrate(list);
    }
    function enter(e, o, k) {
      if (still()) return;
      const live = M.live.has(o.rid);
      e.style.setProperty("--k", String(Math.min(k, 8))); e.classList.add("efoIn"); if (live) e.classList.add("efoFresh");
      const done = () => { e.classList.remove("efoIn", "efoFresh"); e.style.removeProperty("--k"); };
      e.addEventListener("animationend", ev => { if (ev.target === e && ev.animationName === "efoIn") done(); }, { once: true }); setTimeout(done, live ? options.freshMs : 1200);
    }
    function makeRow() {
      const e = el("article", "efoRow");
      e.innerHTML = `<div class="efoThumbsW"></div><div class="efoMain"></div><div class="efoMeta"></div><div class="efoQrW"></div>`;
      e._r = { th: e.firstChild, main: e.children[1], meta: e.children[2], qr: e.children[3] };
      return e;
    }
    function thumbOf(o, p) { return p && p.thumbUrl ? p.thumbUrl : o.piecesCount <= 1 ? o.thumbUrl : ""; }
    function tileHtml(o, p, i, n, over) {
      const url = thumbOf(o, p), label = n > 1 ? `Piece ${i + 1} of ${n}${p && p.label ? ": " + p.label : ""}` : p && p.label ? `Picture: ${p.label}` : "Order picture";
      const inner = url ? `<img src="${esc(url)}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" draggable="false">` : PH;
      return `<button type="button" class="efoZ efoTh${url ? "" : " ph"}${over ? " over" : ""}" data-zoom-dot="${options.zoom}" data-i="${i}"${n > 1 ? ` data-n="${i + 1}"` : ""}${p && p.sku ? ` data-sku="${esc(p.sku)}"` : ""} tabindex="-1" aria-label="${esc(label)}">${inner}</button>`;
    }
    function thumbsHtml(o, open) {
      const n = Math.max(1, o.piecesCount), tiles = [];
      for (let i = 0; i < Math.min(n, TILES); i++) tiles.push(tileHtml(o, o.pieces[i] || (n === 1 ? { thumbUrl: "", label: "", sku: "" } : null), i, n, i >= cap()));
      const more = n - cap();
      const chip = more > 0 ? `<button type="button" class="efoMore" data-act="pieces" aria-expanded="${open}" aria-label="${open ? "Show fewer pieces" : `Show all ${n} pieces`}">${open ? "Less" : "+" + more}</button>` : "";
      return `<div class="efoThumbs${open ? " open" : ""}" role="group" aria-label="${n === 1 ? "Order picture" : n + " pieces"}">${tiles.join("")}${chip}</div>`;
    }
    function fillRow(e, o, ws) {
      const sig = rowSig(o);
      if (e._sig === sig) return;
      e._sig = sig; e._o = o; e.dataset.rid = o.rid;
      const R = e._r, open = M.expanded.has(o.rid);
      const tsig = JSON.stringify([o.piecesCount, o.pieces.map(p => [p.id, p.sku, p.thumbUrl]), o.thumbUrl, open]);
      if (R.th._s !== tsig) { R.th._s = tsig; R.th.innerHTML = thumbsHtml(o, open); }
      const np = o.piecesCount, labels = o.pieces.map(p => p.label).filter(Boolean);
      const sub = np ? `${np} piece${np === 1 ? "" : "s"}${labels.length ? " · " + labels.slice(0, 3).join(", ") + (labels.length > 3 ? "…" : "") : ""}` : "";
      const aria = `Open order ${o.number}${o.customer ? ", " + o.customer : ""}`;
      setHtml(R.main, `<button type="button" class="efoOpen" aria-label="${esc(aria)}"><span class="efoNum">#${hl(o.number, ws)}</span>${o.customer ? `<span class="efoCust">${hl(o.customer, ws)}</span>` : ""}</button>${sub ? `<div class="efoSub">${hl(sub, ws)}</div>` : ""}`);
      const w = o.at ? when(o.at, nowMs()) : null, active = o.lastAt && nowMs() - o.lastAt < ACTIVE_MS;
      const extra = o.stations.length > 1 ? `<em title="${esc(o.stations.map(stName).join(", "))}">+${o.stations.length - 1}</em>` : "";
      setHtml(R.meta, `${w ? `<time class="efoWhen" datetime="${new Date(o.at).toISOString()}"><span class="d">${hlDate(o, w.d, ws)}</span><span class="t">${active ? '<i class="efoNow" title="Active in the last two minutes"></i>' : ""}${esc(w.t)}</span></time>` : `<span class="efoWhen"></span>`}`
        + `<span class="efoSt">${o.station ? `<b class="efoChip" data-st="${esc(o.station)}"><i></i>${hl(stName(o.station), ws)}</b>${extra}` : ""}</span>`
        + `<span class="efoDur" title="Logged working time on this order"><b>${esc(fmtDur(o.durationMs))}</b><small>Worked</small></span>`
        + `<span class="efoStat" data-s="${o.status.s}" title="${esc(o.status.why)}">${esc(o.status.label)}</span>`);
      const q = qrUrl(o.qr);
      setHtml(R.qr, q ? `<button type="button" class="efoZ efoQr" data-zoom-dot="${options.zoom}" data-qr="${esc(o.qr)}" tabindex="-1" aria-label="${esc("QR code of order " + o.number)}"><img src="${esc(q)}" alt="" draggable="false"></button>` : `<span class="efoZ efoQr ph" data-qr="${esc(o.qr)}" aria-hidden="true">${QRPH}</span>`);
    }
    /** Vector designs for the tiles that have no stored picture (or all of them with prefer:'vector'), visible tiles first. */
    function hydrate(scope) {
      const vec = opts.prefer === "vector";
      for (const t of scope.querySelectorAll(".efoTh[data-sku]:not(.over)")) {
        if (t._v) continue; if (!vec && !t.classList.contains("ph")) continue;
        t._v = 1; vecThumb(t.dataset.sku).then(u => { if (u && t.isConnected && !M.dead && (vec || t.classList.contains("ph"))) { t.classList.remove("ph"); t.innerHTML = `<img src="${esc(u)}" alt="" decoding="async" draggable="false">`; } });
      }
    }
    M.repaintQr = () => { for (const e of M.els.values()) { e._sig = ""; } paintList(null); };
    mounts.add(M);
    /* a stored picture that fails to load falls back to the piece's design, else the calm placeholder */
    E.list.addEventListener("error", ev => {
      const im = ev.target; if (!im || im.tagName !== "IMG") return; const t = im.closest(".efoTh"); if (!t) return;
      t.classList.add("ph"); t.innerHTML = PH; t._v = 0; hydrate(t.parentNode);
    }, true);

    /* ── hover cards: the timing facts of a row, all the pieces behind "+N". One card for the page, never over a zoom, hidden on any move away. ── */
    let tip = null, tipT = 0;
    const now_ = () => (root.performance && root.performance.now ? root.performance.now() : Date.now());
    const tipEl = () => { if (!tip) { tip = el("div", "efoTip"); tip.setAttribute("role", "tooltip"); doc.body.appendChild(tip); } return tip; };
    function factsHtml(o) {
      const w = o.at ? when(o.at, nowMs()) : null, last = o.lastAt && o.lastAt !== o.at ? clockFmt.format(new Date(o.lastAt)) : "";
      const g = [["Worked", fmtDur(o.durationMs)], ["Start to finish", fmtDur(o.spanMs)], ["Scans", nf(o.scans)], ["Completed", nf(o.completes)], ["Labels printed", nf(o.prints)], ["Pieces", nf(o.parts || 0)]];
      const steps = o.steps.map(s => `<div class="efoTipR"><span>${esc(stName(s.station))}</span><b>${s.firstAt ? esc(clockFmt.format(new Date(s.firstAt))) : "—"}</b><span>${esc(fmtDur(s.durationMs))}${s.parts ? ` · ${nf(s.parts)} piece${s.parts === 1 ? "" : "s"}` : ""}</span></div>`).join("");
      const iss = o.issues.map(i => `<div class="efoTipI">${esc(i.label || i.kind)}${i.at ? " · " + esc(clockFmt.format(new Date(i.at))) : ""}${i.note ? " · " + esc(i.note) : ""}</div>`).join("");
      return `<div class="efoTipH"><b>#${esc(o.number)}</b><span>${esc(o.status.label)}</span></div>`
        + `<div class="efoTipW">${w ? esc(w.d) + " · " + esc(w.t) + (last ? " to " + esc(last) : "") : ""}${o.customer ? `<br>${esc(o.customer)}` : ""}</div>`
        + `<div class="efoTipG">${g.map(([k, v]) => `<span>${esc(k)}</span><b>${esc(v)}</b>`).join("")}</div>`
        + (steps ? `<div class="efoTipS">${steps}</div>` : "") + (iss ? `<div class="efoTipS">${iss}</div>` : "")
        + `<div class="efoTipF">${esc(o.status.why)}. Worked is the time between this person's actions on the order (gaps over 5 minutes are not counted): logged activity, not effort.</div>`;
    }
    function piecesHtml(o) {
      const n = Math.max(1, o.piecesCount);
      const cells = []; for (let i = 0; i < Math.min(n, TILES); i++) { const p = o.pieces[i], url = thumbOf(o, p); cells.push(`<div><i${p && p.sku ? ` data-sku="${esc(p.sku)}"` : ""}>${url ? `<img src="${esc(url)}" alt="" referrerpolicy="no-referrer">` : PH}</i><span>${esc(p && p.label ? p.label : "Piece " + (i + 1))}</span></div>`); }
      return `<div class="efoTipH"><b>#${esc(o.number)}</b><span>${n} pieces</span></div><div class="efoTipP">${cells.join("")}</div>${n > TILES ? `<div class="efoTipW">and ${n - TILES} more</div>` : ""}`;
    }
    function showTip(row, kind) {
      const o = row._o; if (!o || M.dead || M.zooming || doc.visibilityState === "hidden") return;
      const t = tipEl(); t.innerHTML = kind === "pieces" ? piecesHtml(o) : factsHtml(o);
      if (kind === "pieces") for (const i of t.querySelectorAll("i[data-sku]")) { if (!i.querySelector("img")) vecThumb(i.dataset.sku).then(u => { if (u && i.isConnected && !i.querySelector("img")) i.innerHTML = `<img src="${esc(u)}" alt="">`; }); }
      t.classList.remove("on"); t.style.visibility = "hidden";
      const r = row.getBoundingClientRect(), vw = root.innerWidth || 1200, vh = root.innerHeight || 800, tb = doc.querySelector(".topbar"), top0 = tb && tb.getClientRects().length ? tb.getBoundingClientRect().bottom : 0;
      const w = t.offsetWidth, h = t.offsetHeight;
      const x = Math.max(8, Math.min(vw - w - 8, r.left + 14));
      let y = r.bottom + 8; if (y + h > vh - 8) y = r.top - h - 8; if (y < top0 + 4) y = Math.max(top0 + 4, Math.min(vh - h - 8, r.bottom - h - 6));
      t.style.left = `${Math.round(x)}px`; t.style.top = `${Math.round(y)}px`; t.style.visibility = "";
      M.hoverRow = row; root.requestAnimationFrame(() => { if (M.hoverRow === row) t.classList.add("on"); });
    }
    function hideTip(now) {
      clearTimeout(tipT); tipT = 0; M.hoverRow = null;
      if (!tip) return; tip.classList.remove("on"); if (now) tip.style.visibility = "hidden";
    }
    const zoneOf = t => (t.closest(".efoZ") ? "zoom" : t.closest(".efoMore") ? "pieces" : "facts");
    function wantTip(row, z) {
      clearTimeout(tipT);
      tipT = setTimeout(() => { tipT = 0; if (row.isConnected && M.cand === row && !M.zooming) showTip(row, z); }, options.hoverMs);   // (the pointer's own events, not :hover, which the browser updates late under a moving row)
    }
    E.list.addEventListener("pointerover", ev => {
      if (ev.pointerType === "touch") return;
      const row = ev.target.closest(".efoRow"); if (!row) return;
      M.cand = row; M.overAt = now_(); const z = zoneOf(ev.target); clearTimeout(tipT); tipT = 0;
      if (z === "zoom") { if (M.hoverRow) hideTip(false); return; }
      if (M.zooming || M.quiet === row) return;
      wantTip(row, z);
    });
    // a move inside a row that has no card and none coming (the first over was missed or came before the row settled) asks for one
    E.list.addEventListener("pointermove", ev => {
      if (ev.pointerType === "touch" || M.hoverRow || tipT || M.zooming) return;
      const row = ev.target.closest(".efoRow"); if (!row || M.quiet === row) return;
      const z = zoneOf(ev.target); if (z === "zoom") return;
      M.cand = row; wantTip(row, z);
    });
    E.list.addEventListener("pointerout", ev => {
      const row = ev.target.closest(".efoRow"); if (!row || row.contains(ev.relatedTarget)) return;
      if (M.cand === row) M.cand = null; if (M.quiet === row) M.quiet = null; clearTimeout(tipT); tipT = 0; if (M.hoverRow === row) hideTip(false);
    });
    E.list.addEventListener("focusin", ev => { const row = ev.target.closest(".efoRow"); if (row && ev.target.matches(".efoOpen:focus-visible")) showTip(row, "facts"); });
    E.list.addEventListener("focusout", ev => { const row = ev.target.closest(".efoRow"); if (row && M.hoverRow === row && !row.contains(ev.relatedTarget)) hideTip(false); });
    const onZoom = ev => { const d = ev.detail || {}; if (d.ask || !d.el || !root_.contains(d.el)) return; M.zooming = d.on ? d.el : null; if (d.on) hideTip(true); };
    doc.addEventListener("dotzoom", onZoom);
    const onKey = ev => { if (ev.key === "Escape" && (M.hoverRow || tipT)) { M.quiet = M.cand; hideTip(true); } };   // (Esc puts the card away and it stays away until the pointer leaves the row)
    // a scroll that arrives just after the pointer came onto a row (a browser reports a scroll one frame late) must not cancel that row's card
    const onAway = () => { if (!M.hoverRow && tipT && now_() - (M.overAt || 0) < 150) return; hideTip(true); };
    root.addEventListener("keydown", onKey, true); root.addEventListener("scroll", onAway, { passive: true, capture: true }); root.addEventListener("blur", onAway);

    /* ── presses ── */
    function open(rid, btn) {
      hideTip(true); const o = M.byRid.get(rid); if (!o) return;
      try {
        if (typeof opts.onOpen === "function") opts.onOpen(rid, btn, o);
        else if (shell() && typeof shell().openOrder === "function") shell().openOrder(btn, rid);
        else if (typeof root.openOrderFrom === "function") root.openOrderFrom(btn, rid);
        else if (root.OrderWin && typeof root.OrderWin.openOrder === "function") root.OrderWin.openOrder(rid, { from: btn });
      } catch (e) { try { console.warn("[efficiency orders] order not opened:", e && e.message); } catch (_) {} }
    }
    E.list.addEventListener("click", ev => {
      const row = ev.target.closest(".efoRow"); if (!row) return;
      const more = ev.target.closest(".efoMore");
      if (more) { ev.preventDefault(); const rid = row.dataset.rid; if (M.expanded.has(rid)) M.expanded.delete(rid); else M.expanded.add(rid); hideTip(true); row._sig = ""; fillRow(row, M.byRid.get(rid), words(M.q)); hydrate(row); return; }
      open(row.dataset.rid, row.querySelector(".efoOpen") || row);
    });
    root_.addEventListener("click", ev => {
      const b = ev.target.closest("[data-act]"); if (b) { const a = b.dataset.act; if (a === "retry") load(); else if (a === "more") { M.errMore = null; loadMore(); } else if (a === "clear") clearAll(); return; }
      const fc = ev.target.closest(".efoFc"); if (fc) { setStation(fc.dataset.st); return; }
      const sb = ev.target.closest(".efoSeg button"); if (sb) { setSort(sb.dataset.sort); return; }
      if (ev.target.closest(".efoDx")) { if (typeof opts.onRange === "function") { try { opts.onRange(null); } catch (_) {} } setRange(null); return; }
      if (ev.target.closest(".efoPill")) { flushBuffer(true); return; }
      if (ev.target.closest(".efoClear")) { E.input.value = ""; setQuery(""); E.input.focus(); }
    });
    E.input.addEventListener("input", () => {
      E.clear.hidden = !E.input.value; clearTimeout(M.debounce);
      const q = E.input.value.trim();
      M.debounce = setTimeout(() => { M.debounce = 0; if (q !== M.q) { M.q = q; load(); } }, options.debounceMs);
    });
    E.form.addEventListener("submit", ev => { ev.preventDefault(); clearTimeout(M.debounce); M.debounce = 0; const q = E.input.value.trim(); M.q = q; load(); });
    E.input.addEventListener("keydown", ev => { if (ev.key === "Escape" && E.input.value) { ev.preventDefault(); ev.stopPropagation(); E.input.value = ""; setQuery(""); } });
    const dateIn = () => { let f = E.dFrom.value, t = E.dTo.value; if (f && t && f > t) [f, t] = [t, f]; M.from = f; M.to = t; paintChips(); if (typeof opts.onRange === "function") { try { opts.onRange({ from: f, to: t }); } catch (_) {} } load(); };
    E.dFrom.addEventListener("change", dateIn); E.dTo.addEventListener("change", dateIn);

    /* ── the handle ── */
    function clearAll() { const had = !!(M.from || M.to); M.q = ""; M.station = ""; M.from = ""; M.to = ""; E.input.value = ""; paintChips(); if (had && typeof opts.onRange === "function") { try { opts.onRange(null); } catch (_) {} } return load(); }
    function setQuery(q) { q = S_(q).trim(); E.input.value = q; clearTimeout(M.debounce); M.debounce = 0; if (q === M.q && !M.err && M.loadedOnce) { paintBar(); return Promise.resolve(); } M.q = q; return load(); }
    function setStation(s) { s = dispSt(S_(s)); if (s === M.station) return Promise.resolve(); M.station = s; paintChips(); return load(); }
    function setSort(s) { if (!SORTS.some(x => x[0] === s) || s === M.sort) return Promise.resolve(); M.sort = s; paintChips(); if (M.sortSupport) return load(); order(); paintList(null); paintNote(); return Promise.resolve(); }
    function setRange(r) { const f = r ? S_(r.from) : "", t = r ? S_(r.to) : ""; if (f === M.from && t === M.to) return Promise.resolve(); M.from = f; M.to = t; paintChips(); return load(); }
    function setName(n) { n = S_(n).trim(); if (n === M.name) return Promise.resolve(); M.name = n; M.rows = []; M.byRid = new Map(); M.lo = 0; M.hi = 0; M.loadedOnce = false; return load(); }
    function unmount() {
      if (M.dead) return; M.dead = true; M.gen++;
      clearTimeout(M.debounce); clearTimeout(M.pollT); clearTimeout(M.orphanT); clearTimeout(tipT); abort(M.ctl); abort(M.moreCtl); abort(M.pollCtl);
      doc.removeEventListener("visibilitychange", onVis); doc.removeEventListener("dotzoom", onZoom);
      root.removeEventListener("scroll", onScroll, true); root.removeEventListener("scroll", onAway, true); root.removeEventListener("keydown", onKey, true); root.removeEventListener("blur", onAway);
      if (ioView) ioView.disconnect(); if (ioMore) ioMore.disconnect(); if (M.scrollRaf) root.cancelAnimationFrame(M.scrollRaf);
      if (tip) { tip.remove(); tip = null; } mounts.delete(M);
      if (host._efo === handle) { host._efo = null; host.textContent = ""; }
    }
    const handle = { unmount, refresh: () => load(), setQuery, setRange, setStation, setSort, setName,
      state: () => ({ name: M.name, query: M.q, station: M.station, from: M.from, to: M.to, sort: M.sort, rids: M.rows.map(o => o.rid), total: M.total, scanned: M.scanned, next: M.next, loading: M.busy, more: M.more, error: M.err ? M.err.message : "", polling: !!M.pollT, away: away(), buffered: M.buffer.length, live: E.live.dataset.s }) };
    host._efo = handle;
    paintChips(); paintLive();
    load();
    return handle;
  }

  root.EfficiencyOrders = { mount, options, norm, normOrder, fmtDur, statusOf, hl, hlDate, words, qrUrl, vecThumb, when };
})(typeof window !== "undefined" ? window : globalThis);
