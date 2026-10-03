/*  charm-nest-efficiency.js — the sorter's Employee efficiency console (Paul, 2 Oct 23:18; plans/employee-efficiency/contract.md).
 *  One screen for the whole shop: who is in and since when, which stations they worked, what they produced and scanned, their
 *  throughput by hour, the orders they touched, a business line over the day (or a daily trend over 7 and 30 days).
 *  It is a tab of the sorter (Workspace > Employee efficiency, #efficiencyView), not a pop-up: it stays mounted when another tab
 *  is shown, so its day, its open cards and its scroll are as they were left.
 *  Read-only: the gated function employeeEfficiency (ops overview and person). It asks for the manager passcode once, in the
 *  console itself; the passcode is held in memory and in sessionStorage for the tab only (never in localStorage, a URL or a log).
 *  Polls every 10 s while the tab is shown and the page is in sight, backs off after errors, catches up when shown again.
 *    Efficiency.open()   Efficiency.options = { pollMs, tickMs, maxBackoffMs, holdMs, growMs }   Efficiency.norm(answer) → the view model   */
(function (root) {
  "use strict";
  if (root.Efficiency) return;
  const doc = root.document, TZ = "America/New_York", DAY_MS = 86400000;
  const options = { pollMs: 10000, tickMs: 1000, maxBackoffMs: 60000, holdMs: 60000, growMs: 480 };
  const KEY_STORE = "cn.eff.key", DAYS_STORE = "cn.eff.days";
  const NAMES = { shipping: "Shipping", assembly: "Assembly", welding: "Welding", sorting: "Sorting", design: "Design", laser: "Laser", sorter: "Sorter", qr: "QR printer", inbox: "Inbox" };
  const CORE = ["shipping", "assembly", "welding", "sorting", "design"], EXTRA = ["laser", "sorter", "qr", "inbox"];
  const ACTIONS = { scan: "scanned", complete: "completed", print: "printed", reject: "rejected", undo: "undid", error: "error", note: "noted" };
  const EASE = "cubic-bezier(.2,.8,.2,1)";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const el = (tag, cls, html) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const setText = (e, s) => { if (e && e.textContent !== s) e.textContent = s; };
  const still = () => !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches);
  const raf = f => (root.requestAnimationFrame ? root.requestAnimationFrame(f) : setTimeout(() => f(Date.now()), 16));
  const store = { get(k) { try { return root.sessionStorage.getItem(k) || ""; } catch (_) { return ""; } }, set(k, v) { try { v ? root.sessionStorage.setItem(k, v) : root.sessionStorage.removeItem(k); } catch (_) {} } };

  /* ── New York days and clock (the shop day, as the midnight sign-out's) ── */
  const partsFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric" });
  function nyParts(t) { const o = {}; for (const p of partsFmt.formatToParts(new Date(t))) if (p.type !== "literal") o[p.type] = +p.value; return o; }
  const pad = n => String(n).padStart(2, "0");
  const nyDay = t => { const p = nyParts(t); return `${p.year}-${pad(p.month)}-${pad(p.day)}`; };
  const addDays = (day, n) => { const [y, m, d] = String(day).split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
  const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" });
  const clock = t => clockFmt.format(new Date(t));
  const dayFmt = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" });
  const mdFmt = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" });
  const wdFmt = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", day: "numeric" }), wdOnly = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short" });
  const dayDate = day => { const [y, m, d] = String(day).split("-").map(Number); return new Date(Date.UTC(y, m - 1, d, 12)); };
  const hourLabel = h => `${h % 12 || 12} ${h < 12 ? "AM" : "PM"}`, hourShort = h => `${h % 12 || 12}${h < 12 ? "a" : "p"}`;
  /* ── numbers ── */
  const N = v => { v = +v; return Number.isFinite(v) ? v : 0; };
  const T = v => { v = +v; return Number.isFinite(v) && v > 0 ? v : null; };
  const nf = n => Math.round(N(n)).toLocaleString("en-US");
  const dur = min => { const m = Math.round(Math.max(0, N(min))); if (m < 1) return "0 m"; return m < 60 ? `${m} m` : `${Math.floor(m / 60)} h${m % 60 ? ` ${m % 60} m` : ""}`; };
  const durMs = ms => { ms = Math.max(0, N(ms)); if (ms < 1000) return "—"; if (ms < 60000) return `${Math.round(ms / 1000)} s`; const m = Math.round(ms / 60000); if (m < 120) return `${m} m`; const h = Math.floor(m / 60); if (h < 48) return `${h} h${m % 60 ? ` ${m % 60} m` : ""}`; const d = Math.floor(h / 24); return `${d} d${h % 24 ? ` ${h % 24} h` : ""}`; };
  const rateTxt = v => (v > 0 ? (v >= 10 ? nf(v) : (Math.round(v * 10) / 10).toString()) : "—");
  const secTxt = s => (s > 0 ? (s < 90 ? `${Math.round(s)} s` : `${(Math.round(s / 6) / 10).toString()} min`) : "—");
  const ago = s => (s < 2 ? "just now" : s < 60 ? `${Math.round(s)}s ago` : s < 3600 ? `${Math.floor(s / 60)}m ago` : `${Math.floor(s / 3600)}h ago`);
  const stName = s => NAMES[s] || (s ? s.charAt(0).toUpperCase() + s.slice(1) : "Station");

  /* ── motion: one tween for numbers and chart geometry (instant under reduced motion) ── */
  function tween(ms, step, done) {
    if (still() || !(ms > 0)) { step(1); if (done) done(); return () => {}; }
    let t0 = null, dead = false;
    const f = ts => { if (dead) return; if (t0 == null) t0 = ts; const k = Math.min(1, (ts - t0) / ms); step(1 - Math.pow(1 - k, 3)); if (k < 1) raf(f); else if (done) done(); };
    raf(f);
    return () => { dead = true; };
  }
  /** A figure that counts to its new value (and stays readable to a script as data-v at once). */
  function setNum(e, to, fmt, from0) {
    if (!e) return; fmt = fmt || nf; to = N(to);
    if (e._v === to) return;
    const was = e._v == null ? (from0 ? 0 : to) : e._cur != null ? e._cur : e._v;
    if (e._stop) e._stop(); e._v = to; e.dataset.v = to;
    if (was === to) { e._cur = null; setText(e, fmt(to)); return; }
    e._stop = tween(options.growMs, k => { e._cur = was + (to - was) * k; setText(e, fmt(e._cur)); }, () => { e._cur = null; setText(e, fmt(to)); });
  }

  /** A figure the data does not know yet shows a dash, never a zero that would read as "nothing happened". */
  const dash = e => { if (!e) return; if (e._stop) e._stop(); e._v = null; e._cur = null; delete e.dataset.v; setText(e, "—"); };
  const fig = (e, v, known, fmt, from0) => (known ? setNum(e, v, fmt, from0) : dash(e));
  /* ── the answer, normalised (a missing field is zero or empty, never a crash) ── */
  function hours24(a) { const o = new Array(24).fill(0); if (Array.isArray(a)) for (let i = 0; i < 24; i++) o[i] = Math.max(0, N(a[i])); return o; }
  const sum24 = list => { const o = new Array(24).fill(0); for (const a of list) for (let i = 0; i < 24; i++) o[i] += a[i]; return o; };
  function norm(r) {
    r = r || {};
    const people = (Array.isArray(r.people) ? r.people : []).filter(p => p && p.name).map(p => {
      const t = p.totals || {}, orders = (Array.isArray(p.orders) ? p.orders : []).filter(o => o && o.orderId).map(o => ({ orderId: String(o.orderId), stations: (Array.isArray(o.stations) ? o.stations : []).map(String), parts: N(o.parts), lastAt: T(o.lastAt) }));
      const stations = (Array.isArray(p.stations) ? p.stations : []).filter(s => s && s.station).map(s => ({ station: String(s.station), minutes: N(s.minutes), parts: N(s.parts), scanParts: N(s.scanParts), scans: N(s.scans), completes: N(s.completes), prints: N(s.prints), orders: N(s.orders) })).sort((a, b) => b.minutes - a.minutes);
      const x = { parts: N(t.parts), scanParts: N(t.scanParts), scans: N(t.scans), rejects: N(t.rejects), errors: N(t.errors), orders: t.orders == null ? orders.length : N(t.orders), activeMin: N(t.activeMin), idleMin: N(t.idleMin), signedInMin: N(t.signedInMin), rate: N(t.rate), secPerScan: N(t.secPerScan) };
      if (!x.rate && x.activeMin >= 1 && x.parts) x.rate = x.parts / (x.activeMin / 60);
      if (!x.secPerScan && x.activeMin >= 1 && x.scans) x.secPerScan = x.activeMin * 60 / x.scans;
      return { name: String(p.name), on: p.status === "on", inDay: p.inDay ? String(p.inDay) : "", firstIn: T(p.firstIn), lastOut: T(p.lastOut), onSince: T(p.onSince), nowAt: (Array.isArray(p.nowAt) ? p.nowAt : []).map(String), source: String(p.source || ""), stations, t: x, perHour: hours24(p.perHour), orders };
    });
    const b = r.business || {}, bt = b.totals || {};
    const stRows = new Map();
    for (const s of Array.isArray(b.stations) ? b.stations : []) if (s && s.station) stRows.set(String(s.station), { station: String(s.station), parts: N(s.parts), scans: N(s.scans), orders: N(s.orders), now: (Array.isArray(s.peopleNow) ? s.peopleNow : []).map(String), hours: new Array(24).fill(0) });
    const ph = b.perHour && typeof b.perHour === "object" ? b.perHour : {};
    for (const k of Object.keys(ph)) { if (!stRows.has(k)) stRows.set(k, { station: k, parts: 0, scans: 0, orders: 0, now: [], hours: null }); stRows.get(k).hours = hours24(ph[k]); }
    const hoursAll = Object.keys(ph).length ? sum24(Object.keys(ph).map(k => hours24(ph[k]))) : sum24(people.map(p => p.perHour));
    const sumP = k => people.reduce((n, p) => n + p.t[k], 0);
    const activeMin = sumP("activeMin"), parts = bt.parts == null ? sumP("parts") : N(bt.parts);
    // Design stations log a fixed-text "note" (no order, no parts) only to make work time exact: it stays in time use on the server, but is never a line of the feed
    const marker = f => f.action === "note" && !f.orderId && !N(f.parts) && (f.detail == null || /^(opened|selected) order$/i.test(String(f.detail).trim()));
    const feed = (Array.isArray(r.feed) ? r.feed : []).filter(f => f && N(f.at) > 0 && !marker(f)).map(f => ({ id: String(f.id || `${f.at}|${f.person}|${f.action}|${f.orderId}`), at: N(f.at), person: String(f.person || ""), station: String(f.station || ""), action: String(f.action || ""), orderId: f.orderId ? String(f.orderId) : "", parts: N(f.parts) }));
    return {
      day: String(r.day || ""), days: N(r.days) || 1, now: N(r.now), cursor: r.cursor == null ? "" : String(r.cursor), delta: !!r.delta, people,
      biz: { parts, scans: bt.scans == null ? sumP("scans") : N(bt.scans), orders: bt.orders == null ? sumP("orders") : N(bt.orders), people: bt.people == null ? people.length : N(bt.people), on: people.filter(p => p.on).length, rate: activeMin >= 1 ? parts / (activeMin / 60) : 0, hours: hoursAll, stations: stRows,
        trend: (Array.isArray(b.trend) ? b.trend : []).filter(d => d && d.day).map(d => ({ day: String(d.day), parts: N(d.parts), orders: N(d.orders), people: N(d.people), source: String(d.source || "") })) },
      feed, sources: r.sources || {}, notes: (Array.isArray(r.notes) ? r.notes : []).map(String).filter(Boolean), partial: !!r.partial, errors: Array.isArray(r.errors) ? r.errors : []
    };
  }
  /** One person's days (op person), the empty days kept so the axis is whole. */
  function normHist(r) {
    r = r || {};
    const days = (Array.isArray(r.days) ? r.days : []).filter(d => d && d.day).map(d => ({ day: String(d.day), parts: N(d.parts), scans: N(d.scans), orders: N(d.orders), signedInMin: N(d.signedInMin), activeMin: N(d.activeMin), idleMin: N(d.idleMin), firstIn: T(d.firstIn), lastOut: T(d.lastOut), source: String(d.source || "") }));
    const t = r.totals || {};
    return { days, parts: N(t.parts), orders: N(t.orders), signedInMin: N(t.signedInMin), worked: days.filter(d => d.parts || d.orders || d.signedInMin || d.scans).length, notes: (Array.isArray(r.notes) ? r.notes : []).map(String).filter(Boolean) };
  }

  /** One order (op orders): who touched it where, how long they worked and how long it waited between steps. */
  function normOrder(r) {
    r = r || {}; const t = r.totals || {};
    const steps = (Array.isArray(r.steps) ? r.steps : []).filter(s => s && s.station).map(s => ({ station: String(s.station), person: String(s.person || ""), firstAt: T(s.firstAt), lastAt: T(s.lastAt), workMs: N(s.workMs), waitMs: N(s.waitMs), scans: N(s.scans), completes: N(s.completes), prints: N(s.prints), parts: N(s.parts), source: String(s.source || "events") }));
    return { orderId: String(r.orderId || ""), steps, events: Array.isArray(r.events) ? r.events.length : 0, firstAt: T(t.firstAt), lastAt: T(t.lastAt), spanMs: N(t.spanMs), workMs: N(t.workMs), people: N(t.people), stations: N(t.stations), notes: (Array.isArray(r.notes) ? r.notes : []).map(String).filter(Boolean), partial: !!r.partial };
  }

  /* ── charts: inline SVG, thin marks, one baseline, the current hour in gold ── */
  const SVG = "http://www.w3.org/2000/svg";
  const mk = (parent, name, attrs) => { const e = doc.createElementNS(SVG, name); for (const k in attrs) e.setAttribute(k, attrs[k]); parent.appendChild(e); return e; };
  const NICE = [4, 8, 12, 20, 40, 60, 80, 100];
  function niceMax(v) { for (let p = 1; p < 1e9; p *= 10) for (const f of NICE) if (f * p >= v) return f * p; return v; }
  /** Columns over a baseline: .set({ labels, values, hi, tips:[{ t, v, rows:[[k,v]] }], noun }). One tab stop; arrows walk the tooltip. */
  function columns(host, o) {
    const H = o.height || 130, L = 30, R = 4, T0 = 16, B = 20;
    const S = { W: 0, n: 0, cur: null, stop: null, idx: -1, d: null, svg: null, geo: null };
    host.classList.add("efCols");
    const tip = el("div", "efTip"); tip.hidden = true; host.appendChild(tip);
    function build(W, n) {
      const keep = S.n === n ? S.cur : null; S.W = W; S.n = n; S.cur = keep; S.sig = ""; if (S.svg) S.svg.remove();
      const svg = S.svg = doc.createElementNS(SVG, "svg"); for (const [k, v] of Object.entries({ viewBox: `0 0 ${W} ${H}`, width: W, height: H, class: "efSvg", tabindex: "0", role: "img" })) svg.setAttribute(k, v);
      host.insertBefore(svg, tip);
      const pw = W - L - R, ph = H - T0 - B, slot = pw / n;
      S.geo = { pw, ph, slot, cw: Math.max(3, Math.min(o.maxW || 22, slot - 5)), step: o.thin ? Math.max(1, Math.ceil((o.labelW || 30) / slot)) : 1 };
      S.grid = [0, .5, 1].map(f => { const y = T0 + ph * (1 - f); return { ln: mk(svg, "line", { x1: L, x2: W - R, y1: y, y2: y, class: f ? "efGrid" : "efBase" }), tx: f ? mk(svg, "text", { x: L - 6, y: y + 3.5, class: "efTick", "text-anchor": "end" }) : null }; });
      S.cols = []; for (let i = 0; i < n; i++) S.cols.push(mk(svg, "path", { class: "efCol" }));
      S.xl = []; for (let i = 0; i < n; i++) S.xl[i] = mk(svg, "text", { x: L + slot * (i + .5), y: H - 5, class: "efXl", "text-anchor": "middle" });
      S.v1 = mk(svg, "text", { class: "efVal", "text-anchor": "middle" }); S.v2 = mk(svg, "text", { class: "efVal soft", "text-anchor": "middle" });
      S.hit = mk(svg, "rect", { x: L, y: 0, width: pw, height: H, fill: "transparent" });
      const at = e => { const r = svg.getBoundingClientRect(), x = (e.clientX - r.left) * (W / (r.width || W)); return Math.max(0, Math.min(n - 1, Math.floor((x - L) / slot))); };
      S.hit.addEventListener("pointermove", e => show(at(e)));
      S.hit.addEventListener("pointerdown", e => show(at(e)));
      svg.addEventListener("pointerleave", () => hide());
      svg.addEventListener("blur", () => hide());
      svg.addEventListener("keydown", e => {
        const k = e.key; if (!["ArrowLeft", "ArrowRight", "Home", "End", "Escape"].includes(k)) return;
        e.preventDefault(); if (k === "Escape") return hide();
        show(k === "Home" ? 0 : k === "End" ? n - 1 : Math.max(0, Math.min(n - 1, (S.idx < 0 ? (S.d && S.d.hi >= 0 ? S.d.hi : 0) : S.idx) + (k === "ArrowRight" ? 1 : -1))));
      });
    }
    function paint(vals, max) {
      const { ph, slot, cw } = S.geo, d = S.d, base = T0 + ph;
      for (let i = 0; i < S.n; i++) {
        const v = Math.max(0, vals[i] || 0), h = v > 0 ? Math.max(1.5, v / max * ph) : 0, x = L + slot * i + (slot - cw) / 2, y = base - h, r = Math.min(4, h, cw / 2);
        S.cols[i].setAttribute("d", h ? `M${x},${base}V${y + r}Q${x},${y} ${x + r},${y}H${x + cw - r}Q${x + cw},${y} ${x + cw},${y + r}V${base}Z` : "");
      }
      if (S.grid[1].tx) { setText(S.grid[1].tx, nf(max / 2)); setText(S.grid[2].tx, nf(max)); }
      const lab = (t, i) => { if (i < 0 || !(d.values[i] > 0)) { t.setAttribute("display", "none"); return; } const h = Math.max(1.5, (vals[i] || 0) / max * ph); t.removeAttribute("display"); t.setAttribute("x", L + slot * (i + .5)); t.setAttribute("y", Math.max(10, base - h - 5)); setText(t, nf(d.values[i])); };
      let pk = -1; d.values.forEach((v, i) => { if (v > 0 && (pk < 0 || v > d.values[pk])) pk = i; });
      lab(S.v1, d.hi >= 0 ? d.hi : pk); lab(S.v2, d.hi >= 0 && pk !== d.hi ? pk : -1);
    }
    function show(i) {
      const d = S.d; if (!d || i < 0 || i >= S.n) return hide();
      if (S.idx >= 0 && S.cols[S.idx]) S.cols[S.idx].classList.remove("hov");
      S.idx = i; S.cols[i].classList.add("hov");
      const t = (d.tips && d.tips[i]) || { t: d.labels[i], v: nf(d.values[i]) + (o.unit ? " " + o.unit : ""), rows: [] };
      tip.textContent = "";
      tip.appendChild(el("div", "efTipT")).textContent = t.t;
      tip.appendChild(el("div", "efTipV")).textContent = t.v;
      for (const [k, v] of t.rows || []) { const r = tip.appendChild(el("div", "efTipR")); r.appendChild(el("span")).textContent = k; r.appendChild(el("b")).textContent = v; }
      tip.hidden = false;
      const { slot } = S.geo, w = tip.offsetWidth || 120, cx = L + slot * (i + .5);
      tip.style.left = Math.max(2, Math.min(S.W - w - 2, cx - w / 2)) + "px";
    }
    function hide() { if (S.idx >= 0 && S.cols[S.idx]) S.cols[S.idx].classList.remove("hov"); S.idx = -1; tip.hidden = true; }
    function set(d) {
      const W = Math.max(160, Math.floor(host.clientWidth || 0)), n = d.values.length || 1;
      S.d = d;
      if (!S.svg || S.W !== W || S.n !== n) build(W, n);
      d.labels.forEach((t, i) => { if (S.xl[i]) { setText(S.xl[i], i % S.geo.step === 0 || i === d.hi ? t : ""); S.xl[i].classList.toggle("on", i === d.hi); } });
      S.svg.setAttribute("aria-label", `${d.name || o.name}: ` + d.labels.map((t, i) => `${t} ${nf(d.values[i])}`).join(", "));
      const max = niceMax(Math.max(0, ...d.values)), to = { vals: d.values.slice(), max }, sig = JSON.stringify([to.vals, max, d.hi, S.W]);
      if (S.sig === sig) { if (S.idx >= 0) show(S.idx); return; }
      S.sig = sig; if (S.stop) S.stop();
      const from = S.cur || { vals: new Array(n).fill(0), max };
      S.stop = tween(options.growMs, k => { S.cur = { vals: to.vals.map((v, i) => from.vals[i] + (v - from.vals[i]) * k), max: from.max + (max - from.max) * k }; paint(S.cur.vals, S.cur.max); }, () => { S.cur = to; paint(to.vals, to.max); });
      S.cols.forEach((c, i) => c.classList.toggle("hi", i === d.hi));
      if (S.idx >= 0) show(S.idx);
    }
    if (root.ResizeObserver) new ResizeObserver(() => { if (S.d && host.clientWidth && Math.floor(host.clientWidth) !== S.W) set(S.d); }).observe(host);
    return { set, hide, get svg() { return S.svg; } };
  }
  /** A small line over the hours of the day (or any run of values), its own scale, a dot on the last point. */
  function spark(host, o) {
    const w = o.w || 100, h = o.h || 22, S = { cur: null, stop: null, svg: mk(host, "svg", { viewBox: `0 0 ${w} ${h}`, width: w, height: h, class: "efSpark", "aria-hidden": "true" }) };
    const area = mk(S.svg, "path", { class: "efSA" }), line = mk(S.svg, "path", { class: "efSL" }), dot = mk(S.svg, "circle", { r: 2.6, class: "efSD" });
    function paint(v, last) {
      const n = v.length, max = Math.max(1, ...v), x = i => 2 + (n < 2 ? 0 : i * (w - 4) / (n - 1)), y = a => h - 2.5 - (a / max) * (h - 6);
      let d = ""; for (let i = 0; i <= last && i < n; i++) d += (i ? "L" : "M") + x(i).toFixed(1) + "," + y(v[i]).toFixed(1);
      line.setAttribute("d", d); area.setAttribute("d", d && last > 0 ? `${d}L${x(Math.min(last, n - 1)).toFixed(1)},${h - 2}L${x(0).toFixed(1)},${h - 2}Z` : "");
      if (last >= 0 && last < n) { dot.setAttribute("cx", x(last).toFixed(1)); dot.setAttribute("cy", y(v[last]).toFixed(1)); dot.removeAttribute("display"); } else dot.setAttribute("display", "none");
    }
    return { set(vals, last) {
      const sig = vals.join() + "|" + last; if (S.sig === sig) return; S.sig = sig;
      if (S.stop) S.stop(); const from = S.cur && S.cur.length === vals.length ? S.cur : vals.map(() => 0);
      S.stop = tween(options.growMs, k => { S.cur = vals.map((v, i) => from[i] + (v - from[i]) * k); paint(S.cur, last); }, () => { S.cur = vals.slice(); paint(vals, last); });
    } };
  }

  /* ── state ── */
  const st = { built: false, shown: false, key: store.get(KEY_STORE), keyErr: "", checking: false, days: +store.get(DAYS_STORE) || 1, day: null, gen: 0, data: null, M: null, off: 0, at: 0, fails: 0, busy: false, err: "", timer: 0, tick: 0, ctl: null,
    rows: new Map(), stRows: new Map(), open: new Map(), hist: new Map(), ord: new Map(), ordOpen: new Set(), find: "", feed: [], feedOpen: false, feedSig: "", win: { lo: 7, hi: 18, nowH: -1, today: true }, charts: {}, sandbox: false, lastNames: [] };
  if (![1, 7, 30].includes(st.days)) st.days = 1;
  const now = () => Date.now() + st.off;
  let host = null, E = {};
  const endpoint = () => root.location.origin + "/.netlify/functions/employeeEfficiency";
  const isSandbox = () => !!(root.CN && root.CN.S && root.CN.S.settings && root.CN.S.settings.sandbox === "on");

  /* ── the skeleton, built once; everything after updates in place ── */
  function style() {
    if (doc.getElementById("efStyle")) return;
    const s = doc.createElement("style"); s.id = "efStyle";
    s.textContent = `
#efficiencyView{container-type:inline-size;container-name:ef;gap:12px;min-width:0;color:var(--ink);font-size:12.5px;padding-bottom:6px}
.ef .hidden{display:none!important}
.efBar{position:sticky;top:-6px;z-index:6;display:flex;align-items:center;gap:6px 12px;flex-wrap:wrap;min-height:34px;margin:-6px -10px 0;padding:5px 12px;background:rgba(243,240,234,.94);backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}
.efHead{display:flex;align-items:center;gap:6px 12px;min-width:0;flex-wrap:wrap}
.efTitle{margin:0;font:700 13px var(--sans);letter-spacing:.005em;white-space:nowrap}
.efLive{display:inline-flex;align-items:center;gap:7px;font-size:11.5px;color:var(--ink45);white-space:nowrap;min-width:0}
.efDot{width:7px;height:7px;border-radius:50%;background:var(--ink25);flex:0 0 7px}
.efLive[data-s=live] .efDot{background:var(--sage);animation:efPulse 2.4s ease-out infinite}
.efLive[data-s=slow] .efDot{background:var(--gold2)}
.efLive[data-s=load] .efDot{display:none}
.ef .spin{width:11px;height:11px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:spin .7s linear infinite;flex:0 0 11px;display:inline-block}
.efLive .spin{display:none}.efLive[data-s=load] .spin{display:inline-block}
.ef[data-lock] .efNav,.ef[data-lock] .efSeg{display:none}
@keyframes efPulse{0%{box-shadow:0 0 0 0 rgba(95,122,91,.4)}70%,100%{box-shadow:0 0 0 6px rgba(95,122,91,0)}}
.efFlag{font:700 9px var(--mono);letter-spacing:.04em;padding:3px 6px;border-radius:4px;background:var(--goldSoft);color:#7a5a1d;text-transform:uppercase}
.efGrow{flex:1 1 0}
.efNav{display:inline-flex;align-items:center;gap:2px}
.efDay{min-width:128px;text-align:center;font-weight:650;font-size:12px;white-space:nowrap}
.efIcon{border:0;background:transparent;border-radius:6px;width:26px;height:26px;color:var(--ink70);font-size:16px;line-height:1;padding:0}
.efIcon:hover:not(:disabled){background:var(--paper2);color:var(--ink)}.efIcon:disabled{opacity:.3;cursor:default}
.efToday{border:0;background:transparent;font:700 11px var(--sans);color:var(--gold);padding:4px 6px;border-radius:6px}.efToday:hover{background:var(--goldSoft)}
.efSeg button{padding:4px 11px;font-size:11px}
.efKey{align-self:center;margin-top:8vh;width:min(380px,100%);display:grid;gap:10px;padding:22px;background:var(--card);border:1px solid var(--line);border-radius:14px;box-shadow:var(--sh)}
.efKey h3{margin:0;font:700 14px var(--sans)}.efKey p{margin:0;color:var(--ink70);font-size:12px;line-height:1.5}
.efKeyRow{display:flex;gap:8px}.efKeyRow input{flex:1;min-width:0;border:1px solid var(--line);background:var(--card2);border-radius:9px;padding:8px 11px;font-size:13px}
.efKeyRow input:focus{outline:2px solid var(--gold);outline-offset:1px;background:#fff}
.efKeyErr{min-height:16px;color:var(--clay);font-size:11.5px}
.efKey .btn{display:inline-flex;align-items:center;gap:7px}.efKey .btn .spin{border-color:rgba(0,0,0,.2);border-top-color:currentColor;margin:0}
.efWait{display:flex;align-items:center;justify-content:center;gap:9px;padding:64px 0;color:var(--ink70);font-size:12.5px}
.efBody{display:grid;gap:12px;min-width:0;transition:opacity .25s ease}.efBody.dim{opacity:.45}
.efNote{margin:0;display:grid;gap:2px;color:var(--ink70);font-size:12px;padding:0 2px}
.efNote span:before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--gold2);margin-right:8px;vertical-align:1px}
.efCard{background:var(--card);border:1px solid var(--line);border-radius:12px;min-width:0}
.efKpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr))}
.efKpi{padding:14px 22px 6px;display:grid;gap:3px;min-width:0}.efKpi+.efKpi{border-left:1px solid var(--line2)}
.efKL .s{display:none}.efKL{font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efKV{font:600 36px/1.05 var(--sans);letter-spacing:-.025em;font-variant-numeric:proportional-nums}
.efKS{font-size:11.5px;color:var(--ink45);min-height:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efGraph{padding:4px 22px 14px;display:grid;gap:4px}
.efGraph.two{grid-template-columns:1fr 1fr;gap:4px 28px}.efGraph.two .efGH{margin-top:6px}
.efGH{display:flex;align-items:baseline;gap:10px;font-size:11px;color:var(--ink45);margin-top:8px}.efGT{font-weight:700;letter-spacing:.07em;text-transform:uppercase;font-size:10.5px}
.efGP{margin-left:auto;font-size:11.5px}
.efCols{position:relative;min-width:0}.efSvg{display:block;overflow:visible;max-width:100%;outline:none}.efSvg:focus-visible{outline:2px solid var(--gold);outline-offset:3px;border-radius:4px}
.efGrid{stroke:var(--line2);stroke-width:1}.efBase{stroke:var(--line);stroke-width:1}
.efTick,.efXl{font:10px var(--sans);fill:var(--ink45)}.efXl.on{fill:var(--gold);font-weight:700}
.efCol{fill:#85807a;transition:fill .15s}.efCol.hi{fill:var(--gold)}.efCol.hov{fill:var(--ink)}.efCol.hi.hov{fill:var(--gold2)}
.efVal{font:650 11px var(--sans);fill:var(--ink)}.efVal.soft{fill:var(--ink45);font-weight:600}
.efTip{position:absolute;top:-4px;z-index:3;pointer-events:none;background:var(--velvet);color:#f6f1e6;border-radius:9px;padding:7px 10px;font-size:11px;box-shadow:0 8px 24px rgba(20,16,10,.22);white-space:nowrap;display:grid;gap:1px}
.efTip[hidden]{display:none}.efTipT{color:#cdc4b2;font-size:10.5px}.efTipV{font:650 14px var(--sans)}
.efTipR{display:flex;justify-content:space-between;gap:16px;color:#cdc4b2}.efTipR b{color:#fff;font-weight:650}
.efLabel{font-size:10.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--ink45);font-weight:750;display:flex;align-items:center;gap:8px;margin:0 2px 7px}
.efLabel:after{content:"";flex:1;height:1px;background:var(--line);order:1}.efLabel b{color:var(--ink70);letter-spacing:0;font-weight:700}
.efSR{display:grid;grid-template-columns:96px minmax(0,1fr) 72px 72px 112px;align-items:center;gap:14px;padding:6px 18px;min-height:34px}
.efSR+.efSR{border-top:1px solid var(--line2)}
.efSN{font-weight:700;font-size:12.5px}.efSN:before{content:"";display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--ink25);margin-right:8px;vertical-align:1px;transition:background .3s}
.efSR.on .efSN:before{background:var(--sage)}.efSR.idle .efSN,.efSR.idle .efSV{color:var(--ink45)}.efSR.idle .efSV b{font-weight:500}
.efSW{color:var(--ink70);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.efSW.none{color:var(--ink25)}.efQuiet{font-style:normal;color:var(--ink45);font-size:11px;margin-left:10px}.efQuiet:before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--gold2);margin-right:6px;vertical-align:1px}
.efSV{text-align:right;font-variant-numeric:tabular-nums;font-weight:650;white-space:nowrap}.efSV small{font-weight:500;color:var(--ink45);margin-left:4px;font-size:10.5px}
.efSpark{display:block;overflow:visible}.efSL{fill:none;stroke:#6f6a62;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}.efSA{fill:rgba(93,90,82,.08);stroke:none}.efSD{fill:var(--gold);stroke:var(--card);stroke-width:1.5}
.efPH,.efPRow{display:grid;grid-template-columns:minmax(170px,1fr) minmax(214px,1.7fr) 56px 56px 66px 56px 66px 100px 92px;align-items:center;gap:0 12px}
.efPH{padding:9px 18px 8px;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--ink45);font-weight:700;border-bottom:1px solid var(--line2)}
.efPH span:nth-child(n+3):nth-child(-n+7){text-align:right}.ef[data-range="n"] .efPHs{visibility:hidden}
.efP+.efP{border-top:1px solid var(--line2)}.efP{transition:background .3s}.efP.open{background:var(--card2)}.efP.open:last-child{border-radius:0 0 12px 12px}
.efPRow{padding:9px 18px}
.efWho{display:grid;grid-template-columns:auto 1fr;grid-template-rows:auto auto;gap:1px 10px;text-align:left;border:0;background:transparent;padding:2px 0;border-radius:6px;min-width:0;align-items:center}
.efWho:hover .efName{text-decoration:underline;text-decoration-color:var(--ink25);text-underline-offset:3px}
.efSt{width:8px;height:8px;border-radius:50%;background:var(--ink25);grid-row:1/3;align-self:center;transition:background .3s,box-shadow .3s}
.efSt.on{background:var(--sage);box-shadow:0 0 0 3px var(--sageSoft)}
.efName{font-weight:700;font-size:13.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efWhen{grid-column:2;font-size:11.5px;color:var(--ink45);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efChips{display:flex;flex-wrap:wrap;gap:4px 5px;min-width:0}
.efChip{display:inline-flex;gap:5px;align-items:baseline;border:1px solid var(--line);border-radius:999px;padding:2px 9px;font-size:11px;color:var(--ink70);white-space:nowrap;background:var(--card2)}
.efChip b{font-weight:700;color:var(--ink)}.efChip.now{border-color:var(--sage);background:var(--sageSoft)}
.efN{text-align:right;font-variant-numeric:tabular-nums;min-width:0}.efN>b{font-weight:650;font-size:14px;display:block;white-space:nowrap}.efN:before{content:attr(data-l);display:none;font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;margin-bottom:1px}
.efOrd{border:0;background:transparent;border-radius:7px;padding:3px 4px;margin:-3px -4px;color:inherit}.efOrd:hover{background:var(--paper2)}.efOrd>b{display:inline-flex;align-items:center;gap:4px}
.efOrd i{font-style:normal;color:var(--ink45);font-size:9px;transition:transform .25s ease}.efP.o-orders .efOrd i{transform:rotate(180deg)}
.efAct{display:flex;align-items:center;gap:7px;min-width:0}.efAct:before{content:"Active";display:none;grid-column:1/-1;font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700}.efAct i{flex:1;height:5px;border-radius:3px;background:var(--line);overflow:hidden;display:block;min-width:36px}
.efAct b{display:block;height:100%;background:var(--sage);border-radius:3px;width:0;transition:width .5s ease}.efAct span{font-size:11px;color:var(--ink70);font-variant-numeric:tabular-nums;min-width:30px;text-align:right}
.efPanel{display:grid;grid-template-rows:0fr;visibility:hidden;transition:grid-template-rows .3s ease,visibility 0s .3s}
.efP.open .efPanel{grid-template-rows:1fr;visibility:visible;transition:grid-template-rows .3s ease}
.efPanelIn{min-height:0;overflow:hidden}.efPanelBox{padding:2px 18px 16px;display:grid;gap:12px}
.efTabs{display:inline-flex;justify-self:start}.efTabs button{padding:4px 12px;font-size:11px}
.efCols2{display:grid;grid-template-columns:minmax(0,5fr) minmax(0,7fr);gap:6px 28px;align-items:start}
.efMini{width:100%;border-collapse:collapse;font-size:12px}.efMini th{font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;text-align:right;padding:3px 0 5px}
.efMini th:first-child,.efMini td:first-child{text-align:left}.efMini td{padding:4px 0;border-top:1px solid var(--line2);text-align:right;font-variant-numeric:tabular-nums}
.efOl{display:grid;max-height:268px;overflow:auto;margin:0 -4px;padding:0 4px;gap:0}
.efOw+.efOw{border-top:1px solid var(--line2)}
.efOr{display:grid;grid-template-columns:116px minmax(0,1fr) auto 62px 26px;gap:10px;align-items:center;padding:5px 0;font-size:12px}
.efOx{border:0;background:transparent;border-radius:6px;width:24px;height:22px;padding:0;color:var(--ink45);font-size:9px;display:grid;place-items:center}.efOx:hover{background:var(--paper2);color:var(--ink)}
.efOx i{font-style:normal;transition:transform .25s ease}.efOw.open .efOx i{transform:rotate(180deg)}.efOw.open .efOx{color:var(--ink)}
.efOxw{display:grid;grid-template-rows:0fr;visibility:hidden;transition:grid-template-rows .3s ease,visibility 0s .3s}.efOw.open .efOxw{grid-template-rows:1fr;visibility:visible;transition:grid-template-rows .3s ease}
.efOxi{min-height:0;overflow:hidden}.efOxw .efOxb{padding:2px 4px 12px 2px}
.efOView{padding:12px 18px 14px;margin-bottom:10px;scroll-margin-top:48px;animation:efIn2 .3s ease}@keyframes efIn2{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:none}}
.efOVH{display:flex;align-items:center;gap:12px;min-height:26px}.efOVT{font:700 13px var(--sans)}
.efOVopen{border:0;background:transparent;font:700 11px var(--sans);color:var(--gold);padding:4px 7px;border-radius:6px}.efOVopen:hover{background:var(--goldSoft)}.efOVx{margin-left:auto}
.efOView .efOxb{padding:6px 0 0}
.efFind{display:flex;align-items:center;gap:2px;order:2;margin:-3px 0}
.efFind input{width:118px;border:1px solid var(--line);background:var(--card);border-radius:999px;padding:3px 11px;font:500 11.5px var(--sans);letter-spacing:0;text-transform:none;color:var(--ink);transition:width .25s ease,border-color .2s,background .2s}
.efFind input::placeholder{color:var(--ink45)}.efFind input:focus{width:168px;outline:none;border-color:var(--gold);background:#fff}
.efFind .efIcon{width:22px;height:22px;font-size:15px}
.efTabRow{display:flex;align-items:center;gap:14px;min-height:26px}.efQ{font-size:11.5px;color:var(--ink45)}
.efTsum{display:flex;flex-wrap:wrap;gap:2px 18px;font-size:11.5px;color:var(--ink45);margin-bottom:5px}.efTsum b{color:var(--ink);font-weight:650;margin-left:3px;font-variant-numeric:tabular-nums}
.efTr{display:grid;grid-template-columns:88px minmax(80px,1fr) 150px 44px 56px 60px minmax(70px,1.1fr);gap:10px;align-items:center;padding:5px 0;border-top:1px solid var(--line2);font-size:12px}
.efTr.head{border-top:0;font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;padding:2px 0 4px}.efTr.head span:nth-child(n+4):nth-child(-n+6){text-align:right}
.efTst{font-weight:700}.efTp{color:var(--ink70);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.efTt{color:var(--ink45);font-size:11.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-variant-numeric:tabular-nums}
.efTn{text-align:right;font-variant-numeric:tabular-nums}.efTr.seal .efTn,.efTr.seal .efTt{color:var(--ink45)}
.efTb{position:relative;height:6px;border-radius:3px;background:var(--line2);overflow:hidden}.efTb i{position:absolute;top:0;bottom:0;border-radius:3px;background:var(--gold);min-width:3px}
.efTr.seal .efTb i{background:repeating-linear-gradient(45deg,var(--ink25) 0 3px,transparent 3px 6px)}
.efTnote{margin-top:6px;font-size:11.5px}
.efOid{border:0;background:transparent;padding:2px 6px;margin:0 -6px;border-radius:6px;font:650 12px var(--mono);color:var(--ink);text-align:left;white-space:nowrap}
.efOid:hover{background:var(--goldSoft);text-decoration:underline;text-decoration-color:var(--gold2);text-underline-offset:3px}
.efOr .st{color:var(--ink70);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.efOr .pt{color:var(--ink45);font-size:11px;white-space:nowrap}.efOr time{color:var(--ink45);text-align:right;font-variant-numeric:tabular-nums;font-size:11.5px}
.efMuted{color:var(--ink45);font-size:12px}.efHist{display:grid;gap:8px}.efHist .efSum{font-size:12px;color:var(--ink70)}
.efHRange{display:inline-flex}.efHRange button{padding:3px 10px;font-size:10.5px}
.efPanelBusy{display:flex;align-items:center;gap:8px;color:var(--ink70);font-size:12px;padding:6px 0}
.efFeedBtn{display:flex;align-items:center;gap:8px;width:100%;border:0;background:transparent;padding:10px 18px;text-align:left;font:700 10.5px var(--sans);letter-spacing:.12em;text-transform:uppercase;color:var(--ink45);border-radius:12px}
.efFeedBtn:hover{color:var(--ink)}.efFeedBtn i{font-style:normal;font-size:9px;transition:transform .25s ease}.efFeedBtn[aria-expanded=true] i{transform:rotate(90deg)}
.efFeedBtn b{color:var(--ink70);letter-spacing:0;font-weight:700}
.efFeedWrap{display:grid;grid-template-rows:0fr;visibility:hidden;transition:grid-template-rows .3s ease,visibility 0s .3s}.efFeedWrap.open{grid-template-rows:1fr;visibility:visible;transition:grid-template-rows .3s ease}
.efFeedIn{min-height:0;overflow:hidden}.efFeed{max-height:340px;overflow:auto;padding:0 18px 10px}
.efFl{display:grid;grid-template-columns:62px 104px 84px 84px minmax(0,1fr);gap:10px;align-items:center;padding:5px 0;border-top:1px solid var(--line2);font-size:12px;color:var(--ink70)}
.efFl b{color:var(--ink);font-weight:650;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.efFl time{color:var(--ink45);font-variant-numeric:tabular-nums;font-size:11.5px}.efFl em{font-style:normal;color:var(--ink45);margin-left:8px;font-size:11px}
.efFl.new{animation:efIn .9s ease}@keyframes efIn{from{background:var(--goldSoft);opacity:.2}to{background:transparent;opacity:1}}
.efPeopleEmpty{padding:26px 18px;text-align:center;color:var(--ink45);font-size:12.5px}
/* nothing recorded yet (sign-ins only): no empty columns of dashes, just who is in and where */
.ef[data-nofig] :is(.efPH,.efN,.efSp,.efAct,.efSV,.efGraph),.ef[data-nofig] .efKpi:not([data-k=on]){display:none}
.ef[data-noparts] :is(.efKpi[data-k=parts],.efKpi[data-k=rate],.efSV[data-c=parts]){display:none}
.ef[data-noparts] .efKpis{grid-template-columns:repeat(2,minmax(0,1fr))}.ef[data-noparts] .efSR{grid-template-columns:96px minmax(0,1fr) 72px 112px}
.ef[data-nofig] .efKpis{grid-template-columns:1fr}.ef[data-nofig] .efKpi{padding-bottom:12px}
.ef[data-nofig] .efPRow{grid-template-columns:minmax(170px,1fr) minmax(214px,3fr)}
.ef[data-nofig] .efSR{grid-template-columns:96px minmax(0,1fr);grid-template-areas:none}.ef[data-nofig] .efSR>*{grid-area:auto!important}
@container ef (max-width:1060px){
 .efPH{display:none}
 .efPRow{grid-template-columns:repeat(5,minmax(0,1fr));gap:8px 10px;padding:12px 16px}
 .efWho,.efChips{grid-column:1/-1}.efN{text-align:left}.efN:before{display:block}
 .efSpark{width:100%;max-width:150px}.efSp{grid-column:1/3}.efAct{grid-column:3/-1}
 .efCols2{grid-template-columns:1fr}
 .efSR{grid-template-columns:90px minmax(0,1fr) 64px 64px 100px;padding:9px 14px}
}
@container ef (max-width:640px){
 .efKpis{grid-template-columns:repeat(2,minmax(0,1fr))}.efKpi{padding:14px 16px 8px}.efKpi:nth-child(3){border-left:0}.efKpi:nth-child(n+3){border-top:1px solid var(--line2)}.efKpi:nth-child(2){border-left:1px solid var(--line2)}
 .ef[data-noparts] .efKpi:nth-child(n){border-top:0}.ef[data-noparts] .efKpi[data-k=orders]{border-left:0}.ef[data-noparts] .efKpi[data-k=on]{border-left:1px solid var(--line2)}
 .efKV{font-size:30px}.efGraph{padding:4px 14px 12px}.efGraph.two{grid-template-columns:1fr}
 .efSR{grid-template-columns:minmax(0,1fr) auto auto;grid-template-areas:"n p o" "w w w";gap:3px 12px;padding:10px 14px}.efSR .efSN{grid-area:n}.efSR .efSW{grid-area:w}.efSV[data-c=parts]{grid-area:p}.efSV[data-c=orders]{grid-area:o}.efSR .efSp{display:none}
 .efDay{min-width:0;flex:1}.efBar{gap:2px 10px;padding:4px 12px}.efGrow{display:none}.efHead{flex:1 1 100%}.efNav{flex:1 1 0;min-width:0}.efSeg{margin-left:auto}.efLive .lg{display:none}
 .efKL .l{display:none}.efKL .s{display:inline}
 .efPRow{grid-template-columns:repeat(3,minmax(0,1fr));padding:12px 14px}.efSp{grid-column:1/-1;order:2}.efAct{grid-column:auto;order:1;align-self:end;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:2px 6px}.efAct:before{display:block}
 .efOr{grid-template-columns:100px minmax(0,1fr) 58px 26px}.efOr .pt{display:none}
 .efSR .efSW.none{display:none}
 .efFind input{width:120px}.efFind input:focus{width:150px}
 .efTr{grid-template-columns:repeat(6,minmax(0,1fr));gap:2px 8px}.efTr.head{display:none}.efTst{grid-column:1/3}.efTp{grid-column:3/-1}.efTt{grid-column:1/-1}
 .efTn{grid-column:span 2;text-align:left}.efTn:before{content:attr(data-l) " ";color:var(--ink45);font-size:10.5px}.efTb{grid-column:1/-1;margin-top:3px}
 .efFl{grid-template-columns:56px minmax(0,1fr) 128px;grid-template-areas:"t p a" "t s o"}.efFl>time{grid-area:t}.efFl>b{grid-area:p}.efFl>span:nth-of-type(1){grid-area:s}.efFl>span:nth-of-type(2){grid-area:a}.efFl>span:nth-of-type(3){grid-area:o}
 .efFeedBtn,.efFeed{padding-left:14px;padding-right:14px}.efKey{margin-top:3vh}
}
@media (prefers-reduced-motion:reduce){.ef *{transition:none!important;animation:none!important}}`;
    doc.head.appendChild(s);
  }
  function build() {
    host = doc.getElementById("efficiencyView"); if (!host || st.built) return !!host;
    st.built = true; style(); host.classList.add("ef");
    host.innerHTML = `
<header class="efBar">
  <div class="efHead"><h2 class="efTitle">Employee efficiency</h2>
  <span class="efLive" data-s="load" role="status"><i class="efDot"></i><span class="spin" aria-hidden="true"></span><span class="efLiveT">Connecting…</span></span>
  <span class="efFlag hidden" title="Sandbox copies only">Sandbox</span></div>
  <span class="efGrow"></span>
  <span class="efNav" role="group" aria-label="Day"><button type="button" class="efIcon" data-nav="-1" aria-label="Earlier">‹</button><span class="efDay" aria-live="polite"></span><button type="button" class="efIcon" data-nav="1" aria-label="Later">›</button><button type="button" class="efToday hidden" data-today>Today</button></span>
  <span class="seg efSeg" role="group" aria-label="Range"><button type="button" data-days="1">Day</button><button type="button" data-days="7">7 days</button><button type="button" data-days="30">30 days</button></span>
</header>
<form class="efKey hidden" autocomplete="off"><h3>Manager passcode</h3><p>This screen shows every person's work. Enter the manager passcode (the same one as the Ads console) to open it.</p>
  <div class="efKeyRow"><input type="password" name="efpass" aria-label="Manager passcode" autocomplete="off" autocapitalize="off" spellcheck="false" placeholder="Passcode"><button class="btn gold xs" type="submit">Open</button></div><div class="efKeyErr" role="alert"></div></form>
<div class="efWait"><span class="spin" aria-hidden="true"></span><span class="efWaitT">Reading employee activity…</span></div>
<div class="efBody hidden">
  <div class="efNote hidden" role="status"></div>
  <section class="efCard efHero" aria-label="The business at a glance">
    <div class="efKpis">
      <div class="efKpi" data-k="parts"><span class="efKL">Parts today</span><b class="efKV">0</b><span class="efKS"></span></div>
      <div class="efKpi" data-k="orders"><span class="efKL">Orders today</span><b class="efKV">0</b><span class="efKS"></span></div>
      <div class="efKpi" data-k="on"><span class="efKL">People on now</span><b class="efKV">0</b><span class="efKS"></span></div>
      <div class="efKpi" data-k="rate"><span class="efKL"><span class="l">Parts per active hour</span><span class="s">Per active hour</span></span><b class="efKV">0</b><span class="efKS"></span></div>
    </div>
    <div class="efGraph" data-g="hours"><div class="efGH"><span class="efGT">Parts per hour</span><span class="efGP"></span></div><div class="efChart"></div></div>
    <div class="efGraph two hidden" data-g="trend"><div data-t="parts"><div class="efGH"><span class="efGT">Parts per day</span></div><div class="efChartA"></div></div><div data-t="orders"><div class="efGH"><span class="efGT">Orders per day</span></div><div class="efChartB"></div></div></div>
  </section>
  <section aria-label="Stations"><div class="efLabel">Stations</div><div class="efCard efStations"></div></section>
  <section aria-label="People"><div class="efLabel">People <b class="efPN"></b><form class="efFind" autocomplete="off"><input inputmode="numeric" name="eforder" aria-label="Trace an order: who worked it, where and for how long" placeholder="Trace an order" autocomplete="off" spellcheck="false"><button type="submit" class="efIcon" aria-label="Trace this order">›</button></form></div>
  <div class="efCard efOView hidden" aria-label="Order trace"><div class="efOVH"><b class="efOVT"></b><button type="button" class="efOVopen" data-order="">Open order</button><button type="button" class="efIcon efOVx" data-find-close aria-label="Close the trace">✕</button></div><div class="efOxb"></div></div>
  <div class="efCard"><div class="efPH" aria-hidden="true"><span>Person</span><span>Stations</span><span>Parts</span><span>Scanned</span><span>Orders</span><span>Per hr</span><span>Per scan</span><span class="efPHs">By hour</span><span>Active</span></div><div class="efPeople"></div></div></section>
  <section class="efCard efFeedCard" aria-label="Live activity"><button type="button" class="efFeedBtn" aria-expanded="false"><i aria-hidden="true">▶</i>Live activity <b class="efFC"></b></button><div class="efFeedWrap"><div class="efFeedIn"><div class="efFeed"></div></div></div></section>
</div>`;
    E = { bar: host.querySelector(".efBar"), live: host.querySelector(".efLive"), liveT: host.querySelector(".efLiveT"), flag: host.querySelector(".efFlag"), day: host.querySelector(".efDay"), today: host.querySelector(".efToday"), next: host.querySelector('[data-nav="1"]'), prev: host.querySelector('[data-nav="-1"]'),
      key: host.querySelector(".efKey"), keyIn: host.querySelector(".efKey input"), keyErr: host.querySelector(".efKeyErr"), keyBtn: host.querySelector(".efKey .btn"), wait: host.querySelector(".efWait"), waitT: host.querySelector(".efWaitT"), body: host.querySelector(".efBody"), note: host.querySelector(".efNote"),
      kpi: Object.fromEntries([...host.querySelectorAll(".efKpi")].map(k => [k.dataset.k, k])), gHours: host.querySelector('[data-g="hours"]'), gTrend: host.querySelector('[data-g="trend"]'), gp: host.querySelector(".efGP"),
      find: host.querySelector(".efFind"), findIn: host.querySelector(".efFind input"), ov: host.querySelector(".efOView"), ovT: host.querySelector(".efOVT"), ovOpen: host.querySelector(".efOVopen"), ovBox: host.querySelector(".efOView .efOxb"),
      stations: host.querySelector(".efStations"), people: host.querySelector(".efPeople"), pn: host.querySelector(".efPN"), feedBtn: host.querySelector(".efFeedBtn"), feedWrap: host.querySelector(".efFeedWrap"), feed: host.querySelector(".efFeed"), fc: host.querySelector(".efFC") };
    st.charts.hours = columns(host.querySelector(".efChart"), { height: 124, name: "Parts per hour", maxW: 24, unit: "parts", labelW: 26, thin: true });
    st.charts.tA = columns(host.querySelector(".efChartA"), { height: 118, name: "Parts per day", maxW: 18, unit: "parts", labelW: 40 });
    st.charts.tB = columns(host.querySelector(".efChartB"), { height: 118, name: "Orders per day", maxW: 18, unit: "orders", labelW: 40 });
    host.addEventListener("click", onClick);
    E.keyIn.form.addEventListener("submit", onKey); E.find.addEventListener("submit", onFind);
    E.keyIn.addEventListener("input", () => { if (st.keyErr) { st.keyErr = ""; setText(E.keyErr, ""); } });
    st.sandbox = isSandbox(); E.flag.classList.toggle("hidden", !st.sandbox);
    segs(); paintDay();
    if (root.MutationObserver) new MutationObserver(visibility).observe(host, { attributes: true, attributeFilter: ["class"] });
    doc.addEventListener("visibilitychange", visibility);
    return true;
  }

  /* ── the bar ── */
  function segs() { host.querySelectorAll(".efSeg button").forEach(b => { const on = +b.dataset.days === st.days; b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); }); }
  const today = () => nyDay(now());
  function dayLabel() {
    const td = today(), end = st.day || (st.data && st.data.day) || td, n = st.days;
    if (n > 1) return `${mdFmt.format(dayDate(addDays(end, -(n - 1))))} – ${mdFmt.format(dayDate(end))}`;
    const d = dayFmt.format(dayDate(end));
    return end === td ? `Today · ${d}` : end === addDays(td, -1) ? `Yesterday · ${d}` : d;
  }
  function paintDay() {
    if (!E.day) return;
    setText(E.day, dayLabel());
    const end = st.day || today(); E.next.disabled = end >= today(); E.today.classList.toggle("hidden", !st.day);
    const one = st.days === 1; E.prev.setAttribute("aria-label", one ? "Previous day" : `Previous ${st.days} days`); E.next.setAttribute("aria-label", one ? "Next day" : `Next ${st.days} days`);
    setText(E.kpi.parts.querySelector(".efKL"), st.days > 1 ? `Parts · ${st.days} days` : (st.day && st.day !== today() ? "Parts" : "Parts today"));
    setText(E.kpi.orders.querySelector(".efKL"), st.days > 1 ? `Orders · ${st.days} days` : (st.day && st.day !== today() ? "Orders" : "Orders today"));
  }
  const live = () => !st.day || st.day >= today();
  function paintLive() {
    if (!E.live) return;
    let s, t;
    if (!st.key) { s = "off"; t = "Locked"; }
    else if (st.busy && !st.data) { s = "load"; t = "Reading…"; }
    else if (st.busy && st.loadingDay) { s = "load"; t = "Reading…"; }
    else if (st.err && st.data) { s = "slow"; t = `${esc(st.errShort || "Reconnecting…")} · last update ${ago((Date.now() - st.at) / 1000)}`; }
    else if (!st.data) { s = "off"; t = st.err ? "Not connected" : "Connecting…"; }
    else if (!live()) { s = "off"; t = `Updated ${ago((Date.now() - st.at) / 1000)}`; }
    else { s = "live"; t = `Live · <span class="lg">updated </span>${ago((Date.now() - st.at) / 1000)}`; }
    if (E.live.dataset.s !== s) E.live.dataset.s = s; if (E.liveT.innerHTML !== t) E.liveT.innerHTML = t;
  }

  /* ── the key: asked once, inside the console ── */
  function showKey(msg) {
    st.keyErr = msg || ""; setText(E.keyErr, st.keyErr);
    E.key.classList.remove("hidden"); E.wait.classList.add("hidden"); E.body.classList.add("hidden"); host.setAttribute("data-lock", "");
    paintLive(); if (st.shown) setTimeout(() => { try { E.keyIn.focus(); } catch (_) {} }, 30);
  }
  function unlockBar() { host.removeAttribute("data-lock"); paintDay(); }
  async function onKey(e) {
    e.preventDefault(); if (st.checking) return;
    const k = E.keyIn.value.trim(); if (!k) { setText(E.keyErr, "Enter the passcode."); E.keyIn.focus(); return; }
    st.checking = true; E.keyBtn.disabled = true; E.keyBtn.innerHTML = `<span class="spin" aria-hidden="true"></span>Checking…`; setText(E.keyErr, "");
    try {
      const r = await call({ op: "overview", day: st.day || undefined, days: st.days }, k);
      st.key = k; store.set(KEY_STORE, k); E.keyIn.value = ""; E.key.classList.add("hidden"); unlockBar(); accept(r, true); schedule(options.pollMs);
    } catch (x) {
      E.keyIn.value = ""; setText(E.keyErr, x.auth ? "That passcode was not accepted. Try again." : x.message); E.keyIn.focus();
    } finally { st.checking = false; E.keyBtn.disabled = false; E.keyBtn.textContent = "Open"; }
  }

  /* ── the wire ── */
  /** What a refused or failed call says on screen, in plain words (never the passcode, never raw HTML). */
  function failure(status, j, net) {
    const code = j && j.code ? String(j.code) : "", srv = j && j.error ? String(j.error).replace(/\s+/g, " ").slice(0, 100) : "";
    let msg, short = "Reconnecting…";
    if (net) msg = "The service cannot be reached from here. Check the connection.";
    else if (status === 401) msg = "That passcode was not accepted.";
    else if (status === 403) msg = "No manager passcode is set up yet. Add EDIT_PASSCODE in Netlify (or the passcode in Firebase, config/editPasscode), then open this again.";
    else if (status === 429) { msg = "Too many attempts from this address. Wait a minute and try again."; short = "Paused · too many requests"; }
    else if (status === 405) { msg = "The efficiency service refused this request (405). It may still be updating."; short = "Service refused the request (405)"; }
    else if (status === 404) { msg = "The efficiency service is not published yet (404)."; short = "Service not found (404)"; }
    else if (status === 400 || status === 413) { msg = `The request was refused${srv ? `: ${srv}` : ""}.`; short = "Request refused"; }
    else if (status >= 500) msg = `The data could not be read just now${srv ? ` (${srv})` : ""}.`;
    else msg = srv ? `${srv}.` : `The service answered ${status || "with an error"}.`;
    return Object.assign(new Error(msg), { status: status || 0, auth: status === 401, locked: status === 403 || code === "EDIT_PASSCODE_NOT_SET", limited: status === 429, short, code });
  }
  async function call(body, key, signal) {
    const k = key || st.key, payload = Object.assign({}, body, { key: k }); st.sandbox = isSandbox(); if (st.sandbox) payload.sandbox = true;
    if (payload.day === undefined) delete payload.day;
    let res; try { res = await fetch(endpoint(), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload), cache: "no-store", signal }); } catch (e) { if (e && e.name === "AbortError") throw e; throw failure(0, null, true); }
    const txt = await res.text(); let j = null; try { j = JSON.parse(txt); } catch (_) {}
    if (!res.ok || !j || j.ok === false) throw failure(res.status, j);
    return j;
  }
  const active = () => st.built && st.shown && doc.visibilityState !== "hidden" && !!st.key;
  function schedule(ms) { clearTimeout(st.timer); st.timer = 0; if (active()) st.timer = setTimeout(poll, ms); }
  function backoff() { return Math.min(options.maxBackoffMs, options.pollMs * Math.pow(2, st.fails)); }
  async function poll() {
    clearTimeout(st.timer); st.timer = 0;
    if (!active() || st.busy) return;
    const gen = st.gen, delta = st.data && st.data.cursor && !st.reset ? st.data.cursor : "";
    st.busy = true; st.ctl = root.AbortController ? new AbortController() : null; paintLive();
    try {
      const r = await call({ op: "overview", day: st.day || undefined, days: st.days, after: delta || undefined }, null, st.ctl && st.ctl.signal);
      if (gen !== st.gen) return;
      st.fails = 0; st.err = ""; st.errShort = ""; accept(r, false);
    } catch (e) {
      if (gen !== st.gen || (e && e.name === "AbortError")) return;
      if (e.auth || e.locked) { st.key = ""; store.set(KEY_STORE, ""); st.data = null; showKey(e.auth ? "The passcode was not accepted. Enter it again." : e.message); return; }
      st.fails++; st.err = String(e.message || e).slice(0, 200); st.errShort = e.short || "Reconnecting…"; st.hold = e.limited ? options.holdMs : 0;
      if (!st.data) { setText(E.waitT, `${st.err} Trying again.`); E.wait.querySelector(".spin").style.visibility = "hidden"; } paintLive();
    } finally { if (gen === st.gen) { st.busy = false; st.loadingDay = false; st.reset = false; E.body.classList.remove("dim"); paintLive(); schedule(st.err ? Math.max(backoff(), st.hold || 0) : (live() ? options.pollMs : Math.max(options.pollMs * 3, 30000))); } }
  }
  /** A fresh answer: its numbers into the page in place. With a feed delta (`after`), the new lines join the old by id. */
  function accept(r, first) {
    const M = norm(r); first = first || !st.data;
    if (r.delta && st.data && st.feed.length) { const seen = new Set(M.feed.map(f => f.id)); M.feed = M.feed.concat(st.feed.filter(f => !seen.has(f.id))).sort((a, b) => b.at - a.at).slice(0, 40); }
    st.off = M.now ? M.now - Date.now() : st.off; st.at = Date.now(); st.data = r; st.M = M; st.feed = M.feed;
    E.flag.classList.toggle("hidden", !st.sandbox); host.removeAttribute("data-lock"); E.wait.classList.add("hidden"); E.key.classList.add("hidden"); E.body.classList.remove("hidden"); E.wait.querySelector(".spin").style.visibility = ""; setText(E.waitT, "Reading employee activity…");
    render(M, first); refreshOrders();
  }

  /* ── render: every part updates what is there ── */
  function window24(M) {
    const td = M.day && M.day === today(), nowH = td ? nyParts(now()).hour : -1;
    let lo = 7, hi = 18; const act = []; M.biz.hours.forEach((v, i) => { if (v > 0) act.push(i); }); M.people.forEach(p => p.perHour.forEach((v, i) => { if (v > 0) act.push(i); }));
    if (act.length) { lo = Math.min(lo, ...act); hi = Math.max(hi, ...act); }
    if (nowH >= 0) hi = Math.max(hi, Math.min(23, nowH));
    return { lo, hi, nowH, today: td };
  }
  function render(M, first) {
    st.win = window24(M); M.past = !!M.day && M.day < today();
    M.nofig = M.sources.events === false && !M.sources.seals && !M.people.some(p => p.t.parts || p.t.scans || p.t.orders);
    host.toggleAttribute("data-nofig", M.sources.events === false && !M.sources.seals && !M.people.some(p => p.t.parts || p.t.scans || p.t.orders));
    host.toggleAttribute("data-noparts", M.sources.events === false && !M.nofig);   // history from seals: orders and scans only, no columns of dashes for parts
    host.setAttribute("data-range", M.days > 1 ? "n" : "1"); paintDay(); paintLive();
    renderKpis(M, first); renderGraph(M); renderStations(M, first); renderPeople(M, first); renderFeed(M); renderNote(M);
  }
  function renderNote(M) {
    let lines = M.notes.slice();
    if (!lines.length && M.partial) lines = [M.sources.events === false ? "Activity logging starts when stations send events." : "Part of the data could not be read just now, so some figures may be low."];
    if (!lines.length && !M.people.length) lines = [M.days > 1 ? "No sign-ins or activity in these days." : "Nobody has signed in on this day yet."];
    else if (!lines.length && M.sources.events === false && !M.people.some(p => p.t.parts)) lines = ["Activity logging starts when stations send events."];
    const sig = lines.join("|"); if (E.note._sig === sig) return; E.note._sig = sig;
    E.note.textContent = ""; for (const l of lines) E.note.appendChild(el("span")).textContent = l; E.note.classList.toggle("hidden", !lines.length);
  }
  function renderKpis(M, first) {
    const k = E.kpi, b = M.biz;
    const ev = M.sources.events !== false;
    fig(k.parts.querySelector(".efKV"), b.parts, ev, nf, first); fig(k.orders.querySelector(".efKV"), b.orders, ev || !!M.sources.seals, nf, first);
    // "on now" is only now: a day gone by shows who worked it
    setText(k.on.querySelector(".efKL"), M.past ? (M.days > 1 ? "People in range" : "People that day") : "People on now");
    setNum(k.on.querySelector(".efKV"), M.past ? b.people : b.on, nf, first); setNum(k.rate.querySelector(".efKV"), b.rate, rateTxt, first);
    setText(k.parts.querySelector(".efKS"), ev && b.scans ? `${nf(b.scans)} scans` : "");
    setText(k.on.querySelector(".efKS"), M.past ? "" : `of ${nf(b.people)} ${M.days > 1 ? `in ${M.days} days` : "today"}`);
  }
  const hoursOf = (win) => { const o = []; for (let h = win.lo; h <= win.hi; h++) o.push(h); return o; };
  function renderGraph(M) {
    const trend = M.days > 1, ev = M.sources.events !== false, sl = !!M.sources.seals;
    // seals know orders, not parts: where they fill in, the hourly line counts order steps and says so
    const noun = ev && !sl ? "parts" : !ev ? "steps" : "parts and steps", title = ev && !sl ? "Parts per hour" : !ev ? "Order steps per hour" : "Parts and order steps per hour";
    const showH = !trend && (ev || sl), showT = trend && (ev || sl);
    E.gHours.classList.toggle("hidden", !showH); E.gTrend.classList.toggle("hidden", !showT);
    E.gTrend.querySelector('[data-t="parts"]').classList.toggle("hidden", !ev); E.gTrend.classList.toggle("two", ev);
    if (!showH && !showT) return;
    if (!trend) {
      const w = st.win, hrs = hoursOf(w), vals = hrs.map(h => M.biz.hours[h]), sts = [...M.biz.stations.values()];
      setText(E.gHours.querySelector(".efGT"), title);
      const tips = hrs.map((h, i) => ({ t: hourLabel(h) + (h === w.nowH ? " · now" : ""), v: `${nf(vals[i])} ${noun}`, rows: sts.filter(s => s.hours && s.hours[h] > 0).sort((a, b) => b.hours[h] - a.hours[h]).map(s => [stName(s.station), nf(s.hours[h])]) }));
      st.charts.hours.set({ name: title, labels: hrs.map(hourShort), values: vals, hi: w.today ? hrs.indexOf(w.nowH) : -1, tips });
      let pk = -1; vals.forEach((v, i) => { if (v > 0 && (pk < 0 || v > vals[pk])) pk = i; });
      setText(E.gp, pk >= 0 ? `Busiest hour · ${hourLabel(hrs[pk])}` : M.biz.parts ? "" : `No ${ev ? "parts" : "steps"} recorded yet`);
    } else {
      const end = M.day || today(), n = M.days, byDay = new Map(M.biz.trend.map(d => [d.day, d])), days = [];
      for (let i = n - 1; i >= 0; i--) days.push(addDays(end, -i));
      const rows = days.map(d => byDay.get(d) || { day: d, parts: 0, orders: 0, people: 0, source: "none" }), lab = days.map((d, i) => (n <= 7 ? wdFmt.format(dayDate(d)) : (i % 5 === (n - 1) % 5 || i === 0 ? mdFmt.format(dayDate(d)) : "")));
      const hi = end === today() ? n - 1 : -1, logged = d => d.source === "events" || d.source === "mixed" || !d.source;
      const tipP = d => ({ t: dayFmt.format(dayDate(d.day)), v: logged(d) ? `${nf(d.parts)} parts` : d.source === "seals" ? "Parts not logged" : "No activity", rows: [["Orders", nf(d.orders)], ["People", nf(d.people)]] });
      const tipO = d => ({ t: dayFmt.format(dayDate(d.day)), v: `${nf(d.orders)} orders`, rows: (logged(d) ? [["Parts", nf(d.parts)]] : []).concat([["People", nf(d.people)]]) });
      if (ev) st.charts.tA.set({ labels: lab, values: rows.map(d => (logged(d) ? d.parts : 0)), hi, tips: rows.map(tipP) });
      st.charts.tB.set({ labels: lab, values: rows.map(d => d.orders), hi, tips: rows.map(tipO) });
    }
  }
  /* the stations: one thin row each; the same four columns for every one */
  function stationList(M) {
    const have = M.biz.stations, out = CORE.slice();
    for (const k of EXTRA) { const s = have.get(k); if (s && (s.parts || s.orders || s.scans || s.now.length || (s.hours && s.hours.some(v => v)))) out.push(k); }
    for (const k of have.keys()) if (!out.includes(k)) { const s = have.get(k); if (s.parts || s.orders || s.now.length) out.push(k); }
    return out;
  }
  function renderStations(M, first) {
    const keys = stationList(M), trend = M.days > 1;
    for (const k of keys) {
      let r = st.stRows.get(k);
      if (!r) {
        const e = el("div", "efSR", `<span class="efSN"></span><span class="efSW none">—</span><span class="efSV" data-c="parts"><b class="efNum">0</b><small>parts</small></span><span class="efSV" data-c="orders"><b class="efNum">0</b><small>orders</small></span><span class="efSp"></span>`);
        e.dataset.station = k; setText(e.querySelector(".efSN"), stName(k));
        r = { e, w: e.querySelector(".efSW"), parts: e.querySelector('[data-c="parts"] .efNum'), orders: e.querySelector('[data-c="orders"] .efNum'), sp: spark(e.querySelector(".efSp"), { w: 112, h: 22 }), spEl: e.querySelector(".efSp") };
        st.stRows.set(k, r);
      }
      const s = M.biz.stations.get(k) || { parts: 0, orders: 0, now: [], hours: null };
      const now = M.past ? [] : s.now, hrs = s.hours || [];
      r.e.classList.toggle("on", now.length > 0);
      const names = now.join(", "), w = st.win; let quiet = "";
      if (names && !trend && w.today && w.nowH >= 2 && hrs.length) { let last = -1; for (let h = w.nowH; h >= 0; h--) if (hrs[h] > 0) { last = h; break; } if (last >= 0 && w.nowH - last >= 2) quiet = `No parts since ${hourLabel(last + 1)}`; }
      const sig = names + "|" + quiet; if (r.sig !== sig) { r.sig = sig; r.w.textContent = names || "—"; if (quiet) r.w.appendChild(el("em", "efQuiet")).textContent = quiet; r.w.classList.toggle("none", !names); r.w.title = names + (quiet ? " · " + quiet : ""); }
      const evS = M.sources.events !== false;
      fig(r.parts, s.parts, evS, nf, first); fig(r.orders, s.orders, evS || !!M.sources.seals, nf, first);
      const idle = evS && !s.parts && !s.orders && !now.length; r.e.classList.toggle("idle", idle);   // a station with nothing yet is quiet on screen too
      r.spEl.style.visibility = trend || !evS || idle ? "hidden" : "";
      if (!trend && evS) { const w = st.win, vals = []; for (let h = w.lo; h <= w.hi; h++) vals.push((s.hours || [])[h] || 0); r.sp.set(vals, w.today ? Math.min(vals.length - 1, w.nowH - w.lo) : vals.length - 1); }
    }
    // keep the rows in the same order, add late ones at the end, drop none that were shown
    const want = [...st.stRows.keys()].filter(k => keys.includes(k)); want.forEach((k, i) => { const e = st.stRows.get(k).e; if (E.stations.children[i] !== e) E.stations.insertBefore(e, E.stations.children[i] || null); });
  }
  /* the people: a row each, most active first; a reorder slides, a new row fades in */
  function personRow(name) {
    const e = el("article", "efP"); e.dataset.name = name;
    e.innerHTML = `<div class="efPRow">
  <button type="button" class="efWho" aria-expanded="false" title="Open ${esc(name)}'s days"><i class="efSt" aria-hidden="true"></i><span class="efName"></span><span class="efWhen"></span></button>
  <div class="efChips"></div>
  <div class="efN" data-l="Parts"><b data-r="parts">0</b></div><div class="efN" data-l="Scanned"><b data-r="scans">0</b></div>
  <button type="button" class="efN efOrd" data-l="Orders" aria-expanded="false" title="Open the newest orders"><b><span data-r="orders">0</span><i aria-hidden="true">▼</i></b></button>
  <div class="efN" data-l="Per hour"><b data-r="rate">—</b></div><div class="efN" data-l="Per scan"><b data-r="sec">—</b></div>
  <div class="efSp" data-l="By hour"></div>
  <div class="efAct" title=""><i><b></b></i><span>—</span></div></div>
<div class="efPanel"><div class="efPanelIn"><div class="efPanelBox"></div></div></div>`;
    const r = { e, name, who: e.querySelector(".efWho"), ord: e.querySelector(".efOrd"), nm: e.querySelector(".efName"), when: e.querySelector(".efWhen"), st: e.querySelector(".efSt"), chips: e.querySelector(".efChips"), parts: e.querySelector('[data-r="parts"]'), scans: e.querySelector('[data-r="scans"]'),
      orders: e.querySelector('[data-r="orders"]'), rate: e.querySelector('[data-r="rate"]'), sec: e.querySelector('[data-r="sec"]'), spEl: e.querySelector(".efSp"), act: e.querySelector(".efAct"), bar: e.querySelector(".efAct b"), pct: e.querySelector(".efAct span"), box: e.querySelector(".efPanelBox"), chipSig: "", ordSig: "", built: false };
    r.sp = spark(r.spEl, { w: 100, h: 22 }); setText(r.nm, name);
    return r;
  }
  /** "Parts scanned" is the pieces scanned; when a station logged scans without a piece count, the scans themselves. */
  const scanned = t => (t.scanParts > 0 ? { n: t.scanParts, tip: t.scans && t.scans !== t.scanParts ? `${nf(t.scanParts)} pieces scanned in ${nf(t.scans)} scans` : "Pieces scanned" } : { n: t.scans, tip: t.scans ? `${nf(t.scans)} scans (pieces were not counted)` : "" });
  function whenText(p, M) {
    if (!p.firstIn) return p.source === "sessions" || !p.source ? "No sign-in recorded" : "From sealed work";
    const pre = p.inDay && (M.days > 1 || p.inDay !== M.day) ? wdOnly.format(dayDate(p.inDay)) + " " : "";
    let s = `In ${pre}${clock(p.firstIn)}`;
    if (p.on) { if (p.onSince && p.onSince - p.firstIn > 90000) s += ` · back ${clock(p.onSince)}`; }
    else if (p.lastOut) s += ` · Out ${clock(p.lastOut)}`;
    if (M.sources.events !== false) { if (p.source === "seals") s += " · sealed work"; else if (p.source === "sessions" && !M.nofig) s += " · sign-in only"; }   // the note says it once when nobody has events
    return s;
  }
  function updatePerson(r, p, M, first) {
    const on = p.on && !M.past;
    r.st.classList.toggle("on", on); r.st.title = on ? "On now" : "Locked out"; r.who.setAttribute("aria-label", `${p.name}, ${on ? "on now" : "locked out"}. ${whenText(p, M)}`);
    const wt = whenText(p, M); setText(r.when, wt); r.when.title = wt;
    const chipSig = p.stations.map(s => s.station + s.minutes).join() + "|" + p.nowAt.join() + on;
    if (chipSig !== r.chipSig) { r.chipSig = chipSig; r.chips.innerHTML = p.stations.length ? p.stations.map(s => `<span class="efChip${p.nowAt.includes(s.station) && on ? " now" : ""}"><b>${esc(stName(s.station))}</b>${esc(dur(s.minutes))}</span>`).join("") : `<span class="efMuted">—</span>`; }
    const t = p.t, src = p.source, kParts = src !== "seals" && src !== "sessions", kOther = src !== "sessions";   // seals know orders and scans, not parts; sessions know only time
    const sc = scanned(t);
    fig(r.parts, t.parts, kParts, nf, first); fig(r.scans, sc.n, kOther, nf, first); fig(r.orders, t.orders, kOther, nf, first);
    fig(r.rate, t.rate, kParts && t.rate > 0, rateTxt, first); fig(r.sec, t.secPerScan, kOther && t.secPerScan > 0, secTxt, first);
    r.parts.title = kParts ? "" : src === "seals" ? "Parts were not logged then; sealed work counts orders and scans" : "";
    r.scans.title = kOther && sc.n ? sc.tip : "";
    r.rate.title = t.rate ? "Parts per active hour" : ""; r.sec.title = t.secPerScan ? "Active seconds per scan" : "";
    const tot = t.activeMin + t.idleMin, pc = tot >= 1 ? Math.round(t.activeMin / tot * 100) : null;
    r.bar.style.width = pc == null ? "0%" : pc + "%"; setText(r.pct, pc == null ? "—" : pc + "%");
    r.act.title = pc == null ? "No activity timing yet" : `Active ${dur(t.activeMin)} · idle ${dur(t.idleMin)}${t.signedInMin ? ` · signed in ${dur(t.signedInMin)}` : ""}`;
    const steps = src === "seals"; r.spEl.style.visibility = M.days > 1 || !(kParts || steps) ? "hidden" : ""; r.spEl.title = steps ? "Order steps by hour" : "Parts by hour";
    if (M.days === 1 && (kParts || steps)) { const w = st.win, vals = []; for (let h = w.lo; h <= w.hi; h++) vals.push(p.perHour[h] || 0); r.sp.set(vals, w.today ? Math.min(vals.length - 1, w.nowH - w.lo) : vals.length - 1); }
    if (r.built) paintPanel(r, p);
    r.p = p;
  }
  const FLIP = (e, from) => { if (still() || !e.animate) return; const to = e.getBoundingClientRect().top; if (from == null) e.animate([{ opacity: 0, transform: "translateY(8px)" }, { opacity: 1, transform: "none" }], { duration: 340, easing: EASE }); else if (Math.abs(from - to) > 1) e.animate([{ transform: `translateY(${from - to}px)` }, { transform: "none" }], { duration: 380, easing: EASE }); };
  function renderPeople(M, first) {
    const list = M.people.slice().sort((a, b) => b.t.parts - a.t.parts || b.t.scans - a.t.scans || (b.on - a.on) || a.name.localeCompare(b.name));
    setText(E.pn, list.length ? String(list.length) : "");
    const wasTop = new Map([...E.people.children].map(c => [c, c.getBoundingClientRect().top])), want = [];
    let empty = E.people.querySelector(".efPeopleEmpty"); if (empty) empty.remove();
    for (const p of list) {
      let r = st.rows.get(p.name); const fresh = !r;
      if (!r) { r = personRow(p.name); st.rows.set(p.name, r); r.who.onclick = () => togglePanel(r, "days"); r.ord.onclick = () => togglePanel(r, "orders"); }
      updatePerson(r, p, M, first || fresh); want.push(r);
    }
    for (const [name, r] of st.rows) if (!list.some(p => p.name === name)) { st.rows.delete(name); st.open.delete(name); r.e.remove(); }
    let moved = false; want.forEach((r, i) => { if (E.people.children[i] !== r.e) { E.people.insertBefore(r.e, E.people.children[i] || null); moved = true; } });
    if (!first && moved) want.forEach(r => FLIP(r.e, wasTop.has(r.e) ? wasTop.get(r.e) : null));
    if (!list.length) E.people.appendChild(el("div", "efPeopleEmpty")).textContent = M.days > 1 ? "Nobody signed in during these days." : "Nobody has signed in on this day yet.";
  }
  /* a person's panel: their newest orders (and, when they worked more than one station, what each gave), or their days */
  function togglePanel(r, tab) {
    const cur = st.open.get(r.name);
    if (cur === tab) { st.open.delete(r.name); } else st.open.set(r.name, tab);
    if (!r.built) { r.built = true; r.box.innerHTML = `<div class="efTabRow"><div class="seg efTabs" role="tablist" aria-label="${esc(r.name)}"><button type="button" role="tab" data-t="orders">Orders</button><button type="button" role="tab" data-t="days">Days</button></div><span class="efQ"></span></div><div class="efPane" data-p="orders"></div><div class="efPane hidden" data-p="days"></div>`;
      r.box.querySelectorAll(".efTabs button").forEach(b => b.onclick = () => { st.open.set(r.name, b.dataset.t); paintPanel(r, r.p); }); }
    paintPanel(r, r.p);
  }
  function paintPanel(r, p) {
    const tab = st.open.get(r.name), open = !!tab;
    r.e.classList.toggle("open", open); r.e.classList.toggle("o-orders", tab === "orders"); r.e.classList.toggle("o-days", tab === "days");
    r.who.setAttribute("aria-expanded", tab === "days"); r.ord.setAttribute("aria-expanded", tab === "orders");
    if (!open || !p) return;
    r.box.querySelectorAll(".efTabs button").forEach(b => { const on = b.dataset.t === tab; b.classList.toggle("on", on); b.setAttribute("aria-selected", on); });
    r.box.querySelectorAll(".efPane").forEach(x => x.classList.toggle("hidden", x.dataset.p !== tab));
    const q = [p.t.rejects ? `${nf(p.t.rejects)} rejected` : "", p.t.errors ? `${nf(p.t.errors)} error${p.t.errors === 1 ? "" : "s"}` : ""].filter(Boolean).join(" · "); setText(r.box.querySelector(".efQ"), q);
    if (tab === "orders") paintOrders(r, p); else paintHistory(r, p);
  }
  function paintOrders(r, p) {
    const pane = r.box.querySelector('[data-p="orders"]'), multi = p.stations.length > 1;
    const sig = JSON.stringify([p.orders, multi ? p.stations : 0]); if (r.ordSig === sig) return; r.ordSig = sig;
    const table = multi ? `<div><div class="efLabel" style="margin-top:0">By station</div><table class="efMini"><thead><tr><th>Station</th><th>Parts</th><th>Scanned</th><th>Orders</th></tr></thead><tbody>${p.stations.map(s => `<tr><td>${esc(stName(s.station))}</td><td>${nf(s.parts)}</td><td>${nf(scanned(s).n)}</td><td>${nf(s.orders)}</td></tr>`).join("")}</tbody></table></div>` : "";
    const row = o => `<div class="efOw" data-oid="${esc(o.orderId)}"><div class="efOr"><button type="button" class="efOid" data-order="${esc(o.orderId)}" title="Open this order">${esc(o.orderId)}</button><span class="st">${esc(o.stations.map(stName).join(" · "))}</span><span class="pt">${o.parts ? nf(o.parts) + " parts" : ""}</span><time>${o.lastAt ? esc(clock(o.lastAt)) : ""}</time><button type="button" class="efOx" data-steps="${esc(o.orderId)}" aria-expanded="false" aria-label="Who worked order ${esc(o.orderId)}, where and for how long" title="Who, where and for how long"><i aria-hidden="true">▼</i></button></div><div class="efOxw"><div class="efOxi"><div class="efOxb"></div></div></div></div>`;
    const orders = p.orders.length ? `<div class="efOl">${p.orders.map(row).join("")}</div>`
      : `<div class="efMuted">${p.t.orders ? "The newest orders are listed once stations send events." : "No orders yet."}</div>`;
    const keep = pane.querySelector(".efOl"), top = keep ? keep.scrollTop : 0;
    pane.innerHTML = `<div class="${multi ? "efCols2" : ""}">${table}<div>${multi ? `<div class="efLabel" style="margin-top:0">Newest orders</div>` : ""}${orders}</div></div>`;
    const list = pane.querySelector(".efOl"); if (list) list.scrollTop = top;
    pane.querySelectorAll(".efOw").forEach(syncOrderRow);
  }
  /* ── one order, traced: who touched it where, the work and the waiting between steps (op orders) ── */
  const stamp = t => (nyDay(t) === today() ? clock(t) : `${mdFmt.format(dayDate(nyDay(t)))}, ${clock(t)}`);
  function stepsHtml(d) {
    if (!d.steps.length) return `<div class="efMuted">No activity recorded for this order yet.</div>`;
    const t0 = d.firstAt || d.steps[0].firstAt || 0, span = Math.max(1, (d.lastAt || t0) - t0), wait = d.steps.reduce((n, s) => n + s.waitMs, 0), sealed = d.steps.some(s => s.source === "seals");
    const one = d.steps.length === 1, sum = (one ? [["Work", sealed && !d.workMs ? "—" : durMs(d.workMs)]] : [["Elapsed", durMs(d.spanMs || span)], ["Work", sealed && !d.workMs ? "—" : durMs(d.workMs)], ["Waiting", durMs(wait)]]).map(([k, v]) => `<span>${k} <b>${esc(v)}</b></span>`).join("");
    const rows = d.steps.map(s => {
      const a = s.firstAt, b = s.lastAt, seal = s.source === "seals", when = !a ? "—" : !b || b - a < 60000 ? stamp(a) : `${stamp(a)} – ${nyDay(b) === nyDay(a) ? clock(b) : stamp(b)}`;
      const wd = a && b ? Math.max(4, Math.min(100, (b - a) / span * 100)) : 4, left = a ? Math.max(0, Math.min(100 - wd, (a - t0) / span * 100)) : 0;
      const facts = seal ? "From the order's seals: no part counts or work time" : [s.parts ? `${nf(s.parts)} parts` : "", s.scans ? `${nf(s.scans)} scan${s.scans === 1 ? "" : "s"}` : "", s.prints ? `${nf(s.prints)} print${s.prints === 1 ? "" : "s"}` : ""].filter(Boolean).join(" · ");
      return `<div class="efTr${seal ? " seal" : ""}" title="${esc(facts)}"><b class="efTst">${esc(stName(s.station))}</b><span class="efTp">${esc(s.person || "—")}</span><span class="efTt">${esc(when)}</span><span class="efTn" data-l="Parts">${seal || !s.parts ? "—" : nf(s.parts)}</span><span class="efTn" data-l="Work">${seal ? "—" : esc(durMs(s.workMs))}</span><span class="efTn" data-l="Waited">${d.steps[0] === s ? "—" : esc(durMs(s.waitMs))}</span><span class="efTb"><i style="left:${left.toFixed(2)}%;width:${wd.toFixed(2)}%"></i></span></div>`;
    }).join("");
    return `<div class="efTs"><div class="efTsum">${sum}</div><div class="efTr head" aria-hidden="true"><span>Station</span><span>Person</span><span>When</span><span>Parts</span><span>Work</span><span>Waited</span><span></span></div>${rows}${d.notes.length ? `<div class="efMuted efTnote">${d.notes.map(esc).join(" ")}</div>` : ""}</div>`;
  }
  function paintSteps(box, id) {
    const o = st.ord.get(id) || {}, sig = o.data ? JSON.stringify([o.data.steps, o.data.notes, o.data.spanMs]) : o.err ? "err:" + o.err : "wait";
    if (box._sig === sig) return; box._sig = sig;
    if (o.data) box.innerHTML = stepsHtml(o.data);
    else if (o.err) box.innerHTML = `<div class="efMuted">Not read: ${esc(o.err)} Trying again.</div>`;
    else box.innerHTML = `<div class="efPanelBusy"><span class="spin" aria-hidden="true"></span>Reading who worked on ${esc(id)}…</div>`;
  }
  function syncOrderRow(w) {
    const id = w.dataset.oid, open = st.ordOpen.has(id);
    w.classList.toggle("open", open); w.querySelector(".efOx").setAttribute("aria-expanded", open);
    if (open) { paintSteps(w.querySelector(".efOxb"), id); loadOrder(id, false); }
  }
  function paintOrderViews(id) {
    host.querySelectorAll(".efOw").forEach(w => { if (w.dataset.oid === id && st.ordOpen.has(id)) paintSteps(w.querySelector(".efOxb"), id); });
    if (st.find === id) paintSteps(E.ovBox, id);
  }
  async function loadOrder(id, force) {
    let o = st.ord.get(id); if (!o) st.ord.set(id, o = { data: null, busy: false, err: "", at: 0, gen: 0 });
    if (o.busy || !st.key || !active()) return;
    if (!force && o.at && Date.now() - o.at < 30000) return;
    const gen = ++o.gen; o.busy = true; paintOrderViews(id);
    try { const j = await call({ op: "orders", orderId: id }); if (gen !== o.gen) return; o.data = normOrder(j); o.err = ""; o.at = Date.now(); }
    catch (e) { if (gen !== o.gen || (e && e.name === "AbortError")) return; if (e.auth || e.locked) { st.key = ""; store.set(KEY_STORE, ""); st.data = null; showKey(e.auth ? "The passcode was not accepted. Enter it again." : e.message); return; } o.err = String(e.message || e).slice(0, 120); o.at = Date.now() - 20000; }
    finally { if (gen === o.gen) { o.busy = false; paintOrderViews(id); } }
  }
  /** Open traces are read again with the next polls (at most every 30 s each). */
  function refreshOrders() { for (const id of st.ordOpen) loadOrder(id, false); if (st.find) loadOrder(st.find, false); }
  function toggleSteps(id) {
    if (st.ordOpen.has(id)) st.ordOpen.delete(id); else st.ordOpen.add(id);
    host.querySelectorAll(".efOw").forEach(w => { if (w.dataset.oid === id) syncOrderRow(w); });
  }
  function traceOrder(id) {
    st.find = id; const show = !!id;
    E.ov.classList.toggle("hidden", !show); if (!show) { E.findIn.value = ""; return; }
    setText(E.ovT, `Order ${id}`); E.ovOpen.dataset.order = id; E.ovBox._sig = ""; paintSteps(E.ovBox, id); loadOrder(id, true);
    try { E.ov.scrollIntoView({ block: "nearest", behavior: still() ? "auto" : "smooth" }); } catch (_) {}
  }
  function onFind(e) {
    e.preventDefault(); const id = E.findIn.value.replace(/\D/g, "");
    if (!id) { E.findIn.focus(); return; }
    if (id.length < 5) { st.find = ""; E.ov.classList.remove("hidden"); setText(E.ovT, "Order number"); E.ovOpen.dataset.order = ""; E.ovBox._sig = "short"; E.ovBox.innerHTML = `<div class="efMuted">Enter the whole order number (digits only).</div>`; return; }
    E.findIn.value = id; traceOrder(id);
  }

  /* the days: op person, once on opening and again each minute while open */
  function histKey(r) { const h = st.hist.get(r.name); return h; }
  function paintHistory(r, p) {
    const pane = r.box.querySelector('[data-p="days"]');
    let h = st.hist.get(r.name);
    if (!h) { h = { range: st.days > 1 ? st.days : 7, data: null, busy: false, err: "", at: 0, gen: 0, chart: null, sig: "" }; st.hist.set(r.name, h); }
    if (!pane.querySelector(".efHist")) {
      pane.innerHTML = `<div class="efHist"><div class="efHead" style="display:flex;align-items:center;gap:12px;min-height:24px"><span class="seg efHRange" role="group" aria-label="Days"><button type="button" data-r="7">7 days</button><button type="button" data-r="30">30 days</button></span><span class="efHS efMuted"></span></div><div class="efHC"></div><div class="efSum"></div></div>`;
      pane.querySelectorAll(".efHRange button").forEach(b => b.onclick = () => { h.range = +b.dataset.r; h.data = null; h.sig = ""; paintHistory(r, r.p); loadHistory(r, true); });
      h.chart = columns(pane.querySelector(".efHC"), { height: 110, name: `${r.name}, parts per day`, maxW: 16, unit: "parts", labelW: 40 });
    }
    pane.querySelectorAll(".efHRange button").forEach(b => { const on = +b.dataset.r === h.range; b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
    const hs = pane.querySelector(".efHS"), sum = pane.querySelector(".efSum");
    if (h.busy && !h.data) hs.innerHTML = `<span class="efPanelBusy" style="padding:0"><span class="spin" aria-hidden="true"></span>Reading ${esc(r.name)}'s days…</span>`;
    else if (h.err && !h.data) hs.textContent = `Not read (${h.err}) · trying again`; else hs.textContent = "";
    if (h.data) {
      const d = h.data, sig = JSON.stringify(d.days) + h.range; if (h.sig !== sig) {
        h.sig = sig; const n = h.range, end = (st.M && st.M.day) || today(), by = new Map(d.days.map(x => [x.day, x])), days = []; for (let i = n - 1; i >= 0; i--) days.push(addDays(end, -i));
        const rows = days.map(x => by.get(x) || { day: x, parts: 0, orders: 0, scans: 0, signedInMin: 0, activeMin: 0, idleMin: 0, firstIn: null, lastOut: null });
        h.chart.set({ labels: days.map((x, i) => (n <= 7 ? wdFmt.format(dayDate(x)) : (i % 5 === (n - 1) % 5 || i === 0 ? mdFmt.format(dayDate(x)) : ""))), values: rows.map(x => x.parts), hi: end === today() ? n - 1 : -1,
          tips: rows.map(x => ({ t: dayFmt.format(dayDate(x.day)), v: `${nf(x.parts)} parts`, rows: [["Orders", nf(x.orders)], ["Scans", nf(x.scans)], ["Signed in", x.signedInMin ? dur(x.signedInMin) : "—"]].concat(x.firstIn ? [["In · out", `${clock(x.firstIn)}${x.lastOut ? " · " + clock(x.lastOut) : ""}`]] : []) })) });
        sum.textContent = d.worked ? `${d.worked} day${d.worked === 1 ? "" : "s"} worked · ${nf(d.parts)} parts · ${nf(d.orders)} orders · ${dur(d.signedInMin)} signed in` : "No activity in these days.";
      }
    } else sum.textContent = "";
    if (!h.data || Date.now() - h.at > 60000) loadHistory(r, false);
  }
  async function loadHistory(r, force) {
    const h = st.hist.get(r.name); if (!h || h.busy || !st.key || !active()) return;
    if (!force && h.at && Date.now() - h.at < 60000) return;
    const gen = ++h.gen; h.busy = true; h.err = ""; if (!h.data) paintHistory(r, r.p);
    try {
      const j = await call({ op: "person", name: r.name, day: st.day || undefined, days: h.range });
      if (gen !== h.gen) return; h.data = normHist(j); h.at = Date.now(); h.err = "";
    } catch (e) { if (gen !== h.gen) return; if (e.auth) { st.key = ""; showKey("The passcode was not accepted. Enter it again."); return; } h.err = String(e.message || e).slice(0, 80); h.at = Date.now() - 50000; }
    finally { if (gen === h.gen) { h.busy = false; if (st.open.get(r.name) === "days") paintHistory(r, r.p); } }
  }
  /* live activity: collapsed by default; newest 40, one line each */
  function renderFeed(M) {
    setText(E.fc, M.feed.length ? String(M.feed.length) : "");
    if (!st.feedOpen) return;
    const sig = M.feed.map(f => f.id).join("|"); if (sig === st.feedSig) return;
    const prev = new Set(st.feedSig ? st.feedSig.split("|") : []); st.feedSig = sig;
    E.feed.innerHTML = M.feed.length ? M.feed.map(f => `<div class="efFl${prev.size && !prev.has(f.id) ? " new" : ""}"><time>${esc(clock(f.at))}</time><b>${esc(f.person)}</b><span>${esc(stName(f.station))}</span><span>${esc(ACTIONS[f.action] || f.action)}</span><span>${f.orderId ? `<button type="button" class="efOid" data-order="${esc(f.orderId)}" title="Open this order">${esc(f.orderId)}</button>` : ""}${f.parts ? `<em>${nf(f.parts)} parts</em>` : ""}</span></div>`).join("") : `<div class="efMuted" style="padding:6px 0">Nothing yet. Actions appear here as stations send them.</div>`;
  }

  /* ── clicks ── */
  function openOrder(btn, id) {
    id = String(id || "").replace(/\D/g, ""); if (!id) return;
    try { if (typeof root.openOrderFrom === "function") root.openOrderFrom(btn, id); else if (root.OrderWin && root.OrderWin.openOrder) root.OrderWin.openOrder(id, { from: btn }); } catch (e) { console.warn("[efficiency] order not opened:", e && e.message); }
  }
  function go(change) { Object.assign(st, change); st.gen++; if (st.ctl) { try { st.ctl.abort(); } catch (_) {} } st.busy = false; st.loadingDay = true; st.reset = true; st.fails = 0; st.err = ""; paintDay(); segs(); E.body.classList.add("dim"); st.hist.forEach(h => { h.data = null; h.sig = ""; h.at = 0; }); poll(); }
  function onClick(e) {
    const b = e.target.closest && e.target.closest("button"); if (!b || !host.contains(b)) return;
    if (b.dataset.steps) return toggleSteps(b.dataset.steps);
    if (b.hasAttribute("data-find-close")) { traceOrder(""); return; }
    if (b.dataset.order !== undefined) return b.dataset.order ? openOrder(b, b.dataset.order) : undefined;
    if (b.dataset.nav) { const step = +b.dataset.nav * st.days, base = st.day || today(), to = addDays(base, step); return go({ day: to >= today() ? null : to }); }
    if (b.hasAttribute("data-today")) return go({ day: null });
    if (b.dataset.days && +b.dataset.days !== st.days) { const n = +b.dataset.days; store.set(DAYS_STORE, String(n)); return go({ days: n }); }
    if (b.classList.contains("efFeedBtn")) {
      st.feedOpen = !st.feedOpen; b.setAttribute("aria-expanded", st.feedOpen); E.feedWrap.classList.toggle("open", st.feedOpen);
      if (st.feedOpen) { st.feedSig = ""; if (st.M) renderFeed(st.M); }
    }
  }

  /* ── when it is in sight: poll, pause, catch up ── */
  function visibility() {
    if (!st.built) return;
    const was = st.shown; st.shown = !host.classList.contains("hidden");
    if (st.shown && !was) { if (!st.key) showKey(st.keyErr); }
    if (!(st.shown && doc.visibilityState !== "hidden")) { clearTimeout(st.timer); st.timer = 0; return; }   // out of sight: nothing is read
    if (st.key && !st.busy) { if (!st.at || Date.now() - st.at > 2000) poll(); else if (!st.timer) schedule(options.pollMs); }   // in sight again: catch up
    paintLive();
  }
  function tickLive() { if (st.built && st.shown && doc.visibilityState !== "hidden") { paintLive(); const td = today(); if (st.data && !st.day && st.data.day && st.data.day !== td) { paintDay(); if (!st.busy) poll(); } } }
  function open() { try { if (root.CN && typeof root.CN.setMode === "function") root.CN.setMode("efficiency"); } catch (_) {} }
  function mount() {
    if (!build()) return;
    visibility();
    setInterval(tickLive, Math.max(250, +options.tickMs || 1000));
  }
  if (doc.getElementById("efficiencyView")) mount(); else doc.addEventListener("DOMContentLoaded", mount, { once: true });
  root.Efficiency = { open, options, norm, normHist, normOrder, niceMax, get state() { return { key: !!st.key, days: st.days, day: st.day, shown: st.shown, fails: st.fails, busy: st.busy, at: st.at, rows: [...st.rows.keys()] }; } };
})(window);
