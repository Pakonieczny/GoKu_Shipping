/*  charm-nest-efficiency-charts.js — the Employee efficiency charts (Paul, 5 Oct 2026; plans/employee-hr/plan.md, api.md "EfficiencyCharts").
 *  Hand-drawn SVG chart primitives, no libraries. One file shared by the employee page (E5) and the console's Overview (E1).
 *  Every chart is built the same way:  const c = EfficiencyCharts.line(el, opts);  c.update(data, opts?);  c.destroy();
 *  – It draws into `el` (its own .efc box inside; calling a factory twice on one element replaces the first chart).
 *  – It reads the console's own tokens (--ink, --gold, --sage, --clay, --slate, --card, --line, --velvet … with the same fallbacks)
 *    so it is amber/clay on the console's paper and never brings a colour of its own.
 *  – Hover (mouse), touch (tap or scrub) and keyboard (Tab to the chart, ← → Home End, Esc) all open ONE tooltip card with the
 *    numbers of that point: the value, the previous period and the change, a plain label, an optional one-line definition.
 *  – update() morphs from what is drawn to the new data (a new range stretches the old curve into the new; a live tick only moves
 *    what changed) and keeps the tooltip on the same point, now with the new numbers.  Only transform, opacity and
 *    stroke-dashoffset are ever animated by CSS (geometry morphs are paint-only SVG attribute updates, never layout);
 *    prefers-reduced-motion switches every animation off.
 *  – Empty = a calm "No activity in this range" (a series of nothing but nulls/zeros);  Loading = a small labelled spinner, the
 *    previous drawing held dimmed, never a skeleton.  null is a GAP (a day with nothing logged), never a zero.
 *  – Responsive to its container (ResizeObserver) from 1440 down to 390 px: fewer ticks, taller touch targets, same card.
 *
 *  ── common options (every factory) ──────────────────────────────────────────────────────────────────────────────────────
 *    height, heightSm      px; heightSm (default 85 % of height) applies below 520 px wide
 *    unit                  'pieces' 'orders' 'scans' 'prints' 'days' 'count' 'hours' 'seconds' 'percent' 'clock' 'ms' 'min'
 *                          'pieces/hour' 'orders/hour' 'pieces/day' 'orders/day' (E4's metric units; sets tick and card text)
 *    fmt(v)                your own text for a value in the card (default: by unit, e.g. "328 per hour", "4 m 12 s", "72%")
 *    tickFmt(v)            your own text for an axis tick (default: short form by unit)
 *    label(point)          the card's title for a point (default: "Thu, Oct 2" / "Week of Oct 6" / "8 AM" / the label as given)
 *    tipRows(point)        extra card rows: [[name, text], …] or [{ k, v, tone:'good'|'bad' }]
 *    def                   one plain line in the card's foot (the metric's definition)
 *    better                'up' (default) | 'down' | null: which direction of change is good (colours the change chip)
 *    prevLabel             card text for the previous period (default 'Previous period')
 *    nullText              card text for a gap (default 'Nothing logged')
 *    emptyText, loadingLabel   'No activity in this range' / 'Loading'
 *    onPoint(point)        a click (or Enter, or a second tap) on a point
 *    name                  accessible name of the chart
 *  A point handed to label / tipRows / onPoint is  { index, x, label, values:{key:v}, prev:{key:v}, meta, series:[{key,label,value,prev,delta}] }.
 *
 *  ── factories (all take (el, opts) and return { update(data, opts?), destroy(), show(i), hide(), setLoading(on, label?) }) ──
 *   line(el, opts)      multi-series lines over days / weeks / hours / labels, dashed previous period, optional median→p90 band
 *       data  { x:['2026-10-01',…], series:[{ key, label, values:[n|null…], prev?:[n|null…], color?, dashed?, def? }],
 *               band?:{ lo:[…], hi:[…], label? }, meta?:[any per x] }          x = day strings (gaps for missing days, spacing by
 *               date), hour numbers (xKind:'hour'), or any labels.  Shorthand: { x, values, prev } or { points:[{ x, y, prev, lo, hi }] }.
 *       opts  xKind:'day'|'week'|'hour'|'cat' (default: found from x), granularity:'week', area:false, yMax, yMin (number|'auto'),
 *             markers (default: when ≤ 16 points), onRange(fromX, toX) (drag to zoom), seriesLabel
 *   area(el, opts)      the same, with the wash under the line (area:true)
 *   bars(el, opts)      columns on one baseline; grouped, or stacked (opts.stack:true); the highlighted bar is gold like the console's
 *       data  { x:[…], series:[{ key, label, values, prev?, color? }], highlight?: index | x }   (a previous-period tick on each bar)
 *       opts  stack, highlight (index|x|fn(x,i)), maxBar (24), labelPeak (true), onRange
 *   stacked(el, opts)   the station mix over time: stacked area (opts.mode:'bars' = stacked columns); opts.share:true = share of the day
 *       data  { x:[…], series:[{ key:'welding', label:'Welding', values:[…] }] }   (colour follows the station key, never its rank)
 *   hourHeatmap(el, opts)   24 hours × days, one hue; data { rows:[{ key, label, values:[24 × n|null] }] } or { values:[24] } (a strip)
 *       opts  nowHour, hourFrom/hourTo (crop), rowLabel(row), tipRows(cell)  (cell = { row, hour, value, share, rowTotal })
 *   calendarHeat(el, opts)  a month / year of days: states worked / partial / off / future / closed / before / pending
 *       data  { days:[{ day:'2026-10-02', state, signedMs?, firstIn?, lastOut?, parts?, orders?, note? }], today?, from?, to? }
 *       opts  onDay(day, item) (click, tap or Enter), selected:'2026-10-02', layout:'auto'|'month'|'months', clickable(item)
 *   sparkline(el, opts) a small line, its own scale, a dot on the last value; hover card optional (opts.hover:false to switch off)
 *       data  { values:[…], x?:[…], labels?:[…] }   opts  width, height (default 28), yMin ('auto' | 0), fill (true)
 *   donut(el, opts)     part of a whole (the station mix); data { slices:[{ key, label, value, color?, meta? }], center?:{ value, label } }
 *       opts  max (7: the tail folds into "Other"), onSlice(slice), legend (true)
 *  helpers
 *   EfficiencyCharts.countUp(el, from, to, fmt?, ms?)   a figure that counts to its value (data-v at once); returns a stopper
 *   EfficiencyCharts.fmt   int num compact duration("4 m 12 s") hm("1 h 5 m") mins secs perHour percent clock day hour week delta byUnit
 *   EfficiencyCharts.color(key)   the css colour a station / series key gets (the same key is always the same colour)
 *   EfficiencyCharts.version
 */
(function (root) {
  "use strict";
  if (root.EfficiencyCharts) return;
  const doc = root.document, SVGNS = "http://www.w3.org/2000/svg", DAY_MS = 86400000, EASE = "cubic-bezier(.2,.8,.2,1)";

  /* ── small tools ── */
  const N = v => (v == null || v === "" || typeof v === "boolean" ? null : Number.isFinite(+v) ? +v : null);
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const noop = () => {};
  const still = () => !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches);
  const raf = f => (root.requestAnimationFrame ? root.requestAnimationFrame(f) : setTimeout(() => f(Date.now()), 16));
  const caf = id => (root.cancelAnimationFrame ? root.cancelAnimationFrame(id) : clearTimeout(id));
  const ease = k => 1 - Math.pow(1 - k, 3);
  const pf = v => String(Math.round(v * 10) / 10);
  const mk = (parent, name, attrs, cls) => { const e = doc.createElementNS(SVGNS, name); if (attrs) for (const k in attrs) e.setAttribute(k, attrs[k]); if (cls) e.setAttribute("class", cls); if (parent) parent.appendChild(e); return e; };
  const h = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
  const setText = (e, s) => { if (e && e.textContent !== s) e.textContent = s; };
  const fade = (e, ms) => { if (e && e.animate && !still()) { try { e.animate([{ opacity: 0 }, { opacity: 1 }], { duration: ms || 260, easing: "ease-out" }); } catch (_) {} } };
  const sameArr = (a, b) => { if (a === b) return true; if (!a || !b || a.length !== b.length) return false; for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false; return true; };

  /** One tween for chart geometry (paint-only SVG attributes). Instant under reduced motion. Returns its stopper. */
  function tween(ms, step, done) {
    if (still() || !(ms > 0)) { step(1); if (done) done(); return noop; }
    let t0 = null, dead = false, id = 0;
    const f = ts => { if (dead) return; if (t0 == null) t0 = ts; const k = Math.min(1, (ts - t0) / ms); step(ease(k), k); if (k < 1) id = raf(f); else if (done) done(); };
    id = raf(f);
    return () => { dead = true; caf(id); };
  }

  /* ── New York-free day maths: days are 'YYYY-MM-DD' strings read as plain calendar days ── */
  const DAYRE = /^(\d{4})-(\d{2})-(\d{2})$/;
  const isDay = s => typeof s === "string" && DAYRE.test(s);
  const dayT = s => { const m = DAYRE.exec(s); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3], 12) : NaN; };
  const dayAdd = (s, n) => new Date(dayT(s) + n * DAY_MS).toISOString().slice(0, 10);
  const fWd = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short" });
  const fWdLong = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "long" });
  const fMd = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" });
  const fMon = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short" });
  const fMonLong = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "long", year: "numeric" });
  const dow = s => new Date(dayT(s)).getUTCDay();            // 0 Sunday … 6 Saturday
  const dd = s => +s.slice(8, 10);
  const mm = s => +s.slice(5, 7);

  /* ── numbers as words: the same helpers everywhere (durations 4 m 12 s, h m, per hour, percent) ── */
  const grp = n => Math.round(n).toLocaleString("en-US");
  function numTxt(v, d) { if (v == null || !Number.isFinite(+v)) return "—"; v = +v; if (d == null) d = Math.abs(v) >= 100 ? 0 : 1; const p = Math.pow(10, d); return (Math.round(v * p) / p).toLocaleString("en-US", { maximumFractionDigits: d }); }
  function compact(v) { if (v == null) return "—"; v = +v; const a = Math.abs(v); if (a >= 1e6) return numTxt(v / 1e6, 1) + "M"; if (a >= 1e4) return numTxt(v / 1e3, 1) + "K"; return grp(v); }
  function duration(ms) {
    if (ms == null || !Number.isFinite(+ms)) return "—";
    let s = Math.round(Math.max(0, +ms) / 1000);
    if (s < 60) return s + " s";
    if (s < 3600) { const m = Math.floor(s / 60), r = s % 60; if (m < 10) return r ? m + " m " + r + " s" : m + " m"; const mr = Math.round(s / 60); if (mr < 60) return mr + " m"; s = 3600; }
    if (s < 86400) { const hh = Math.floor(s / 3600), m = Math.round((s % 3600) / 60); return m === 60 ? hh + 1 + " h" : m ? hh + " h " + m + " m" : hh + " h"; }
    const d = Math.floor(s / 86400), hh = Math.round((s % 86400) / 3600); return hh && hh < 24 ? d + " d " + hh + " h" : d + " d";
  }
  function hm(ms) { if (ms == null || !Number.isFinite(+ms)) return "—"; const m = Math.round(Math.max(0, +ms) / 60000); if (m < 1) return "0 m"; const hh = Math.floor(m / 60), r = m % 60; return hh ? (r ? hh + " h " + r + " m" : hh + " h") : r + " m"; }
  const perHour = v => (v == null || !Number.isFinite(+v) ? "—" : numTxt(v) + " per hour");
  const percent = (v, d) => (v == null || !Number.isFinite(+v) ? "—" : numTxt(v, d == null ? (Math.abs(v) < 10 && v % 1 ? 1 : 0) : d) + "%");
  function clockTxt(min) { if (min == null || !Number.isFinite(+min)) return "—"; const t = Math.round(+min), H = Math.floor(t / 60) % 24, M = t % 60; return (H % 12 || 12) + ":" + String(M).padStart(2, "0") + " " + (H < 12 ? "AM" : "PM"); }
  const dayTxt = (s, style) => {
    if (!isDay(s)) return String(s);
    const t = new Date(dayT(s));
    if (style === "short") return fMd.format(t);
    if (style === "wd") return fWd.format(t) + " " + dd(s);
    if (style === "month") return fMon.format(t);
    if (style === "monthLong") return fMonLong.format(t);
    if (style === "weekday") return fWdLong.format(t);
    return fWd.format(t) + ", " + fMd.format(t);
  };
  const hourTxt = hh => { hh = ((Math.round(hh) % 24) + 24) % 24; return (hh % 12 || 12) + " " + (hh < 12 ? "AM" : "PM"); };
  const hourShort = hh => { hh = ((Math.round(hh) % 24) + 24) % 24; return (hh % 12 || 12) + (hh < 12 ? "a" : "p"); };
  const weekTxt = s => "Week of " + dayTxt(s, "short");
  /** How a value changed: { abs, pct, text:'+20%', dir:'up'|'down'|'flat'|null } (null dir = not comparable). */
  function delta(cur, prev) {
    cur = N(cur); prev = N(prev);
    if (cur == null || prev == null) return { abs: null, pct: null, text: "", dir: null };
    const abs = cur - prev, pct = prev !== 0 ? abs / Math.abs(prev) * 100 : null, mag = Math.abs(abs);
    if (mag < 1e-9) return { abs: 0, pct: 0, text: "no change", dir: "flat" };
    const sign = abs > 0 ? "+" : "−";
    const text = pct != null ? sign + numTxt(Math.abs(pct), Math.abs(pct) < 10 ? 1 : 0) + "%" : sign + numTxt(mag);
    return { abs, pct, text, dir: abs > 0 ? "up" : "down" };
  }
  const plural = (v, one, many) => (Math.round(+v) === 1 ? one : many);
  /** "34 pieces" -> { text:'34', unit:'pieces' } for the card's big figure; times and clocks stay whole. */
  function splitBig(txt) { const m = /^(-?[\d,]+(?:\.\d+)?)\s+([A-Za-z][A-Za-z ]*)$/.exec(txt); return m && !/^(h|m|s|d|AM|PM)$/.test(m[2]) ? { text: m[1], unit: m[2] } : { text: txt }; }
  /** value → text, by E4's unit. short = for axis ticks (no unit word). */
  function byUnit(unit, short) {
    switch (unit) {
      case "pieces": return v => (v == null ? "—" : short ? grp(v) : grp(v) + " " + plural(v, "piece", "pieces"));
      case "orders": return v => (v == null ? "—" : short ? grp(v) : grp(v) + " " + plural(v, "order", "orders"));
      case "scans": return v => (v == null ? "—" : short ? grp(v) : grp(v) + " " + plural(v, "scan", "scans"));
      case "prints": return v => (v == null ? "—" : short ? grp(v) : grp(v) + " " + plural(v, "print", "prints"));
      case "days": return v => (v == null ? "—" : short ? grp(v) : grp(v) + " " + plural(v, "day", "days"));
      case "hours": return v => (v == null ? "—" : numTxt(v, 1) + " h");
      case "seconds": return v => (v == null ? "—" : duration(v * 1000));
      case "ms": return v => (v == null ? "—" : duration(v));
      case "min": return v => (v == null ? "—" : duration(v * 60000));
      case "percent": return v => percent(v);
      case "clock": return v => clockTxt(v);
      case "pieces/hour": case "orders/hour": return v => (v == null ? "—" : short ? numTxt(v) : perHour(v));
      case "pieces/day": case "orders/day": return v => (v == null ? "—" : short ? numTxt(v) : numTxt(v) + " per day");
      case "count": return v => (v == null ? "—" : grp(v));
      default: return v => (v == null ? "—" : numTxt(v));
    }
  }
  const fmt = { int: v => (v == null ? "—" : grp(v)), num: numTxt, compact, duration, hm, mins: m => (m == null ? "—" : duration(m * 60000)), secs: s => (s == null ? "—" : duration(s * 1000)), perHour, percent, clock: clockTxt, day: dayTxt, hour: hourTxt, hourShort, week: weekTxt, delta, byUnit };

  /** A figure that counts to its value. data-v carries the final value at once (a script reads it); instant under reduced motion. */
  function countUp(el, from, to, f, ms) {
    if (!el) return noop;
    f = typeof f === "function" ? f : fmt.int; to = N(to);
    if (el._efcStop) { el._efcStop(); el._efcStop = null; }
    if (to == null) { delete el.dataset.v; el._efcCur = null; setText(el, "—"); return noop; }
    el.dataset.v = String(to);
    let a = N(from); if (a == null) a = el._efcCur != null ? el._efcCur : to;
    if (a === to || still()) { el._efcCur = to; setText(el, f(to)); return noop; }
    el._efcStop = tween(ms || 650, k => { el._efcCur = a + (to - a) * k; setText(el, f(el._efcCur)); }, () => { el._efcCur = to; setText(el, f(to)); el._efcStop = null; });
    return () => { if (el._efcStop) { el._efcStop(); el._efcStop = null; } };
  }

  /* ── colours: the console's tokens, in the order that keeps hues apart; a key keeps its colour ── */
  const CYCLE = ["gold", "slate", "sage", "clay", "silver", "rose", "ink70", "gold2"];
  const FIXED = { sorter: "gold", shipping: "slate", assembly: "sage", welding: "clay", sorting: "silver", design: "rose", laser: "ink70", inbox: "gold2", qr: "ink45", other: "ink25" };
  const TOKEN_OK = /^(gold|gold2|slate|sage|clay|silver|rose|ink|ink70|ink45|ink25|goldSoft|sageSoft|claySoft)$/;
  const cssColor = c => (TOKEN_OK.test(c) ? "var(--efc-" + c + ")" : c);
  function colorFor(key, index, explicit) {
    if (explicit) return cssColor(String(explicit));
    const k = String(key == null ? "" : key).toLowerCase();
    if (FIXED[k]) return cssColor(FIXED[k]);
    return cssColor(CYCLE[(index || 0) % CYCLE.length]);
  }

  /* ── nice axis ticks ── */
  const INTEGER_UNITS = { pieces: 1, orders: 1, scans: 1, prints: 1, days: 1, count: 1 };
  function niceTicks(dataMax, intervals, unit, forcedMax) {
    let max = forcedMax != null ? forcedMax : dataMax;
    if (!(max > 0)) max = unit === "percent" ? 100 : intervals;
    let ladder = null, scale = 1;
    if (unit === "seconds") ladder = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400];
    else if (unit === "ms") { ladder = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400]; scale = 1000; }
    else if (unit === "min") ladder = [1, 2, 5, 10, 15, 30, 60, 120, 180, 240, 360, 480, 720, 1440];
    else if (unit === "hours") ladder = [0.5, 1, 2, 3, 4, 6, 8, 12, 24, 48, 96];
    else if (unit === "clock") ladder = [30, 60, 120, 180, 240, 360, 720];
    const raw = max / scale / intervals;
    let step;
    if (ladder) { step = ladder.find(s => s >= raw) || ladder[ladder.length - 1]; step *= scale; }
    else {
      const p = Math.pow(10, Math.floor(Math.log10(raw))), cands = [];
      for (const mul of [1, 10]) for (const b of [1, 2, 2.5, 5, 10]) cands.push(b * p * mul);
      step = cands.find(s => s >= raw - 1e-9 && (!INTEGER_UNITS[unit] || (s >= 1 && Math.abs(s - Math.round(s)) < 1e-9)));
      if (step == null) step = Math.max(1, Math.ceil(raw));
    }
    const top = forcedMax != null ? forcedMax : Math.max(step, Math.ceil(max / step - 1e-9) * step);
    const ticks = []; for (let v = 0; v <= top + step * 1e-6; v += step) ticks.push(Math.round(v * 1e6) / 1e6);
    return { max: top, step, ticks };
  }

  /* ── the sheet of styles, injected once (tokens with fallbacks; only transform, opacity, stroke-dashoffset animate) ── */
  const CSS = `
.efc{--efc-ink:var(--ink,#1c1a17);--efc-ink70:var(--ink70,#5b554c);--efc-ink45:var(--ink45,#938c80);--efc-ink25:var(--ink25,#c4bdb0);--efc-line:var(--line,#e4ddd0);--efc-line2:var(--line2,#efe9dd);--efc-surface:var(--card,#fffefb);--efc-card2:var(--card2,#faf7f1);--efc-paper2:var(--paper2,#ebe5d9);--efc-gold:var(--gold,#a9823f);--efc-gold2:var(--gold2,#caa861);--efc-goldSoft:var(--goldSoft,#f0e6cd);--efc-sage:var(--sage,#5f7a5b);--efc-sageSoft:var(--sageSoft,#e7eddf);--efc-clay:var(--clay,#b0563f);--efc-claySoft:var(--claySoft,#f4e3dc);--efc-slate:var(--slate,#4a6b78);--efc-silver:var(--m-silver,#8d95a0);--efc-rose:var(--m-rose,#c08578);--efc-velvet:var(--velvet,#221f1b);--efc-sans:var(--sans,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif);--efc-bar:#85807a;position:relative;display:block;width:100%;min-width:0;font:12px/1.35 var(--efc-sans);color:var(--efc-ink);-webkit-tap-highlight-color:transparent;-webkit-user-select:none;user-select:none}
.efc-body{position:relative;transition:opacity .25s ease}
.efc[data-loading] .efc-body{opacity:.42}
.efc[data-state=empty] .efc-body{opacity:.55}
.efc-svg{display:block;max-width:100%;overflow:visible;outline:none;touch-action:pan-y}
.efc-svg:focus-visible{outline:2px solid var(--efc-gold);outline-offset:3px;border-radius:4px}
.efc-svg text{font-family:var(--efc-sans)}
.efc-tick{font-size:10px;fill:var(--efc-ink45)}
.efc-xl{font-size:10px;fill:var(--efc-ink45)}.efc-xl.on{fill:var(--efc-gold);font-weight:700}
.efc-grid{stroke:var(--efc-line2);stroke-width:1}
.efc-base{stroke:var(--efc-line);stroke-width:1}
.efc-tk{opacity:1;transition:opacity .26s ease}.efc-tk.pre,.efc-tk.out{opacity:0}
.efc-ser.pre,.efc-ser.out{opacity:0}.efc-ser{opacity:1;transition:opacity .4s ease}
.efc-line{fill:none;stroke:var(--c);stroke-width:2;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:1;stroke-dashoffset:0;transition:stroke-dashoffset .9s ${EASE}}
.efc-line.draw{stroke-dashoffset:1}
.efc-edge{fill:none;stroke:var(--c);stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}
.efc-area{fill:var(--c);fill-opacity:.1;stroke:none;opacity:1;transition:opacity .7s ease}.efc-area.pre{opacity:0}
.efc-stacked .efc-edge{stroke:var(--efc-surface);stroke-width:1.5}
.efc-fill{fill:var(--c);fill-opacity:.74;stroke:none;opacity:1;transition:opacity .7s ease}.efc-fill.pre{opacity:0}
.efc-band{fill:var(--efc-ink25);fill-opacity:.28;stroke:none;opacity:1;transition:opacity .5s ease}.efc-band.pre{opacity:0}
.efc-prev{fill:none;stroke:var(--efc-ink45);stroke-width:1.5;stroke-dasharray:4 4;stroke-linecap:round;opacity:1;transition:opacity .5s ease}.efc-prev.pre{opacity:0}
.efc-mk{fill:var(--c);stroke:var(--efc-surface);stroke-width:1.5;opacity:1;transition:opacity .5s ease}.efc-mk.pre{opacity:0}
.efc-closed{fill:var(--efc-ink);opacity:.035;pointer-events:none}
.efc-guide{pointer-events:none;opacity:0;transition:opacity .12s ease,transform .08s ease-out}
.efc-guide.show{opacity:1}.efc-guide.fresh{transition:opacity .12s ease}
.efc-guide line{stroke:var(--efc-ink25);stroke-width:1}
.efc-dot{fill:var(--c);stroke:var(--efc-surface);stroke-width:2;transition:transform .08s ease-out}
.efc-fresh .efc-dot{transition:none}
.efc-brush{fill:var(--efc-gold);fill-opacity:.12;stroke:var(--efc-gold2);stroke-width:1;pointer-events:none}
.efc-hit{fill:transparent;cursor:default}.efc-hit.can{cursor:pointer}
.efc-hit.zoom{cursor:crosshair}
.efc-bar{fill:var(--c);opacity:1;transition:opacity .15s ease}
.efc-bar.cur{fill:var(--efc-gold)}
.efc-dimmed .efc-bar,.efc-dimmed .efc-pv{opacity:.5}.efc-dimmed .efc-bar.on,.efc-dimmed .efc-pv.on{opacity:1}
.efc-pv{stroke:var(--efc-ink);stroke-width:1.5;stroke-linecap:round;opacity:.55;transition:opacity .15s ease}
.efc-slot{fill:var(--efc-ink);opacity:0;transition:opacity .12s ease;pointer-events:none}.efc-slot.cl{opacity:.03}.efc-slot.on{opacity:.05}
.efc-val{font-size:11px;font-weight:650;fill:var(--efc-ink)}.efc-val.soft{fill:var(--efc-ink45);font-weight:600}
.efc-cell{fill:var(--efc-gold);opacity:1;transition:opacity .35s ease}
.efc-cell.z{fill:var(--efc-line2)}.efc-cell.nul{fill:var(--efc-line2);opacity:.45}
.efc-cell.hot{stroke:var(--efc-ink);stroke-width:1.5}
.efc-cell.now{stroke:var(--efc-gold);stroke-width:1.5}
.efc-scale rect{fill:var(--efc-gold)}
.efc-lg{font-size:10.5px;fill:var(--efc-ink45)}
.efc-day{opacity:1}
.efc-day .bg{stroke:none}
.efc-day.worked .bg{fill:var(--efc-sage)}
.efc-day.partial .bg{fill:var(--efc-gold2)}
.efc-day.off .bg{fill:var(--efc-claySoft);stroke:var(--efc-clay);stroke-width:1.5}
.efc-day.pending .bg{fill:none;stroke:var(--efc-gold);stroke-width:1.5}
.efc-day.future .bg{fill:none;stroke:var(--efc-line);stroke-width:1;stroke-dasharray:2 3}
.efc-day.closed .bg{fill:var(--efc-line2)}
.efc-day.before .bg{fill:none}
.efc-day .n{font-size:10px;font-weight:600;fill:var(--efc-ink70);pointer-events:none}
.efc-day.worked .n{fill:#fff}.efc-day.partial .n{fill:var(--efc-ink)}.efc-day.future .n,.efc-day.before .n,.efc-day.closed .n{fill:var(--efc-ink45)}.efc-day.before .n{fill:var(--efc-ink25)}
.efc-day.hot .bg{stroke:var(--efc-ink);stroke-width:1.5}
.efc-day.sel .bg{stroke:var(--efc-gold);stroke-width:2.5}
.efc-day.today .ring{stroke:var(--efc-ink);stroke-width:1.5;fill:none}
.efc-day.can{cursor:pointer}
.efc-day.in{animation:efcPop .38s ${EASE} both}
.efc-mt{font-size:11px;font-weight:700;fill:var(--efc-ink70);letter-spacing:.02em}
.efc-wd{font-size:9px;fill:var(--efc-ink25);font-weight:700}
.efc-legend{display:flex;flex-wrap:wrap;gap:4px 14px;margin-top:8px;font-size:11px;color:var(--efc-ink70);align-items:center;min-width:0}
.efc-lgi{display:inline-flex;align-items:center;gap:6px;min-width:0;white-space:nowrap}
.efc-sw{width:10px;height:10px;border-radius:3px;background:var(--c);flex:0 0 10px;display:inline-block}
.efc-sw.line{height:2px;border-radius:1px;width:14px;flex-basis:14px}
.efc-sw.dash{height:0;border-top:2px dashed var(--efc-ink45);background:none;width:14px;flex-basis:14px;border-radius:0}
.efc-sw.tick{height:2px;border-radius:1px;background:var(--efc-ink);opacity:.55;width:14px;flex-basis:14px}
.efc-sw.band{background:var(--efc-ink25);opacity:.5;width:14px;flex-basis:14px}
.efc-sw.off{background:var(--efc-claySoft);box-shadow:inset 0 0 0 1.5px var(--efc-clay)}
.efc-sw.fut{background:none;box-shadow:inset 0 0 0 1px var(--efc-line)}
.efc-sw.clo{background:var(--efc-line2)}
.efc-sw.pen{background:none;box-shadow:inset 0 0 0 1.5px var(--efc-gold)}
.efc-sc{display:inline-flex;gap:2px;align-items:center}.efc-sc i{width:12px;height:10px;border-radius:2px;background:var(--efc-gold);display:inline-block}
.efc-tip{position:absolute;left:0;top:0;z-index:5;pointer-events:none;background:var(--efc-velvet);color:#f6f1e6;border-radius:9px;padding:8px 11px 8px;font-size:11px;line-height:1.35;box-shadow:0 8px 24px rgba(20,16,10,.22);width:max-content;max-width:min(268px,calc(100% - 4px));opacity:0;transform:translate(0,0);transition:opacity .14s ease,transform .09s ease-out}
.efc-tip[data-open]{opacity:1}.efc-tip[data-fresh]{transition:opacity .14s ease}
.efc-tip-t{color:#cdc4b2;font-size:10.5px;margin-bottom:1px}
.efc-tip-v{display:flex;align-items:baseline;gap:5px;margin:1px 0 3px}.efc-tip-v b{font:650 15px var(--efc-sans);letter-spacing:-.01em;color:#fff;white-space:nowrap}.efc-tip-v span{color:#cdc4b2;font-size:11px}
.efc-tip-r{display:flex;align-items:center;gap:7px;color:#cdc4b2;margin-top:2px;min-width:0}.efc-tip-r span{flex:1 1 auto;min-width:0}.efc-tip-r b{color:#fff;font-weight:650;white-space:nowrap;margin-left:12px}
.efc-tip-r.mut b{color:#cdc4b2;font-weight:600}
.efc-tip-r.strong{border-top:1px solid rgba(246,241,230,.14);padding-top:3px;margin-top:4px}
.efc-key{width:12px;height:2px;border-radius:1px;background:var(--c);flex:0 0 12px;display:inline-block}.efc-key.sq{width:8px;height:8px;border-radius:2px;flex-basis:8px}.efc-key.dash{background:none;border-top:2px dashed #ada393;height:0;border-radius:0}
.efc-delta{font-style:normal;font-weight:700;font-size:10px;padding:1px 6px;border-radius:999px;background:rgba(246,241,230,.14);color:#f6f1e6;white-space:nowrap}
.efc-delta.good{background:var(--efc-sage);color:#fff}.efc-delta.bad{background:var(--efc-clay);color:#fff}
.efc-tip-f{margin-top:6px;padding-top:5px;border-top:1px solid rgba(246,241,230,.14);color:#ada393;font-size:10.5px;line-height:1.35;white-space:normal;max-width:236px}
.efc-state{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;z-index:2}
.efc-empty{color:var(--efc-ink45);font-size:12.5px;text-align:center;padding:0 12px;animation:efcIn .35s ease both}
.efc-wait{display:inline-flex;align-items:center;gap:9px;color:var(--efc-ink70);font-size:12px;background:var(--efc-surface);border:1px solid var(--efc-line);border-radius:999px;padding:5px 13px 5px 11px;animation:efcIn .25s ease .18s both}
.efc-spin{width:11px;height:11px;border:2px solid var(--efc-line);border-top-color:var(--efc-ink70);border-radius:50%;animation:efcSpin .7s linear infinite;display:inline-block;flex:0 0 11px}
.efc-donut{display:flex;align-items:center;gap:18px;flex-wrap:wrap;min-width:0}
.efc-donut .efc-body{flex:0 0 auto}
.efc-dlist{flex:1 1 160px;min-width:0;display:grid;gap:2px}
.efc-dli{display:flex;align-items:center;gap:8px;min-width:0;padding:4px 8px;margin:0 -8px;border-radius:7px;font-size:12px;color:var(--efc-ink70);cursor:default;opacity:1;transition:opacity .15s ease;outline:none}
.efc-dli:hover,.efc-dli.on,.efc-dli:focus-visible{background:var(--efc-card2)}
.efc-dli:focus-visible{box-shadow:inset 0 0 0 2px var(--efc-gold)}
.efc-dimmed .efc-dli{opacity:.5}.efc-dimmed .efc-dli.on{opacity:1}
.efc-dli em{font-style:normal;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1 1 auto;color:var(--efc-ink)}
.efc-dli b{font-weight:650;color:var(--efc-ink);font-variant-numeric:tabular-nums;white-space:nowrap}.efc-dli small{color:var(--efc-ink45);font-size:11px;font-variant-numeric:tabular-nums;min-width:34px;text-align:right}
.efc-arc{fill:none;stroke:var(--c);opacity:1;transition:transform .16s ease,opacity .15s ease}
.efc-dimmed .efc-arc{opacity:.45}.efc-dimmed .efc-arc.on{opacity:1}
.efc-ctr-v{font-size:22px;font-weight:600;fill:var(--efc-ink);letter-spacing:-.02em}.efc-ctr-l{font-size:11px;fill:var(--efc-ink45)}
@keyframes efcSpin{to{transform:rotate(360deg)}}
@keyframes efcIn{from{opacity:0}to{opacity:1}}
@keyframes efcPop{from{opacity:0;transform:scale(.86)}to{opacity:1;transform:scale(1)}}
.efc-day.in{transform-box:fill-box;transform-origin:center}
@media (prefers-reduced-motion:reduce){.efc *,.efc-tip{transition:none!important;animation:none!important}}
`;
  function injectStyle() {
    if (!doc || doc.getElementById("efcStyle")) return;
    const s = doc.createElement("style"); s.id = "efcStyle"; s.textContent = CSS;
    (doc.head || doc.documentElement).appendChild(s);
  }

  /* ── the shared frame: box, tooltip card, empty / loading states, size watching, clean-up ── */
  let UID = 0, ACTIVE = null;
  function core(el, kind, o0) {
    if (!doc) throw new Error("EfficiencyCharts needs a document");
    if (!el || !el.appendChild) throw new Error("EfficiencyCharts." + kind + ": give it an element");
    injectStyle();
    if (el.__efc) { try { el.__efc.destroy(); } catch (_) {} }
    const C = { kind, el, o: Object.assign({}, o0 || {}), dead: false, W: 0, H: 0, idx: -1, stops: new Set(), timers: new Set(), ro: null, id: "efc" + (++UID), state: "", drawn: false, listeners: [] };
    const box = C.root = h("div", "efc efc-" + kind); box.dataset.chart = kind;
    C.body = h("div", "efc-body"); box.appendChild(C.body);
    C.stateEl = h("div", "efc-state"); box.appendChild(C.stateEl);
    const tip = C.tip = h("div", "efc-tip"); tip.id = C.id + "-tip"; tip.setAttribute("role", "tooltip"); tip.setAttribute("aria-hidden", "true"); box.appendChild(tip);
    el.appendChild(box);
    C.later = (fn, ms) => { const t = setTimeout(() => { C.timers.delete(t); if (!C.dead) fn(); }, ms); C.timers.add(t); return t; };
    C.tween = (ms, step, done) => { const st = tween(ms, step, () => { C.stops.delete(st); if (done) done(); }); C.stops.add(st); return st; };
    C.on = (target, type, fn, opt) => { target.addEventListener(type, fn, opt); C.listeners.push(() => target.removeEventListener(type, fn, opt)); };
    C.width = () => Math.floor(box.clientWidth || el.clientWidth || 0) || C.o.width || 560;
    C.phone = () => C.width() < 520;
    C.opt = (k, d) => (C.o[k] != null ? C.o[k] : d);
    C.fmtLong = () => (typeof C.o.fmt === "function" ? C.o.fmt : byUnit(C.o.unit, false));
    C.fmtTick = () => (typeof C.o.tickFmt === "function" ? C.o.tickFmt : byUnit(C.o.unit, true));
    C.setState = (mode, text) => {
      const st = C.state === mode && C.stateText === text;
      C.state = mode; C.stateText = text;
      box.dataset.state = mode === "loading" ? (C.drawn ? "ready" : "loading") : mode;
      C.body.style.minHeight = !C.drawn && mode !== "ready" ? Math.round(C.o.minHeight || C.o.height || 140) + "px" : "";
      if (mode === "loading") box.dataset.loading = "1"; else delete box.dataset.loading;
      if (st) return;
      C.stateEl.textContent = "";
      if (mode === "empty") C.stateEl.appendChild(h("div", "efc-empty", text || C.opt("emptyText", "No activity in this range")));
      else if (mode === "loading") { const w = h("div", "efc-wait"); w.setAttribute("role", "status"); w.appendChild(h("i", "efc-spin")); w.appendChild(h("span", null, text || C.opt("loadingLabel", "Loading"))); C.stateEl.appendChild(w); }
    };
    C.setLoading = (on, label) => { if (on) C.setState("loading", label); else if (C.state === "loading") C.setState(C.hasData === false ? "empty" : "ready"); };
    /** The legend under the drawing: items [{ label, css?, kind:'sq'|'line'|'dash'|'band'|'tick' }]; hidden when empty. */
    C.legend = items => {
      items = C.o.legend === false ? [] : items || [];
      const sig = JSON.stringify(items);
      if (C.lgSig === sig) return; C.lgSig = sig;
      if (!C.lg) { C.lg = h("div", "efc-legend"); box.insertBefore(C.lg, C.stateEl); }
      C.lg.textContent = ""; C.lg.hidden = !items.length;
      for (const it of items) { const i = h("span", "efc-lgi"), sw = h("i", "efc-sw" + (it.kind && it.kind !== "sq" ? " " + it.kind : "")); if (it.css) sw.style.setProperty("--c", it.css); i.appendChild(sw); i.appendChild(h("span", null, it.label)); C.lg.appendChild(i); }
    };
    /* the tooltip card */
    function fillTip(card) {
      tip.textContent = "";
      if (card.title) tip.appendChild(h("div", "efc-tip-t", card.title));
      if (card.big) { const v = h("div", "efc-tip-v"); v.appendChild(h("b", null, card.big.text)); if (card.big.unit) v.appendChild(h("span", null, card.big.unit)); tip.appendChild(v); }
      for (const r of card.rows || []) {
        const row = h("div", "efc-tip-r" + (r.muted ? " mut" : "") + (r.strong ? " strong" : "")); row.dataset.k = r.k;
        if (r.color || r.dashed) { const k = h("i", "efc-key" + (r.sq ? " sq" : "") + (r.dashed ? " dash" : "")); if (r.color) k.style.setProperty("--c", r.color); row.appendChild(k); }
        row.appendChild(h("span", null, r.k)); row.appendChild(h("b", null, r.v));
        if (r.delta && r.delta.text) row.appendChild(h("em", "efc-delta " + (r.delta.tone || ""), r.delta.text));
        tip.appendChild(row);
      }
      if (card.foot) tip.appendChild(h("div", "efc-tip-f", card.foot));
    }
    /** Open the card for `card`, anchored at (ax, ay) in the chart's own box: 'side' (beside a guide), 'above' (over a mark). */
    C.showTip = (card, ax, ay, mode, half) => {
      if (ACTIVE && ACTIVE !== C) ACTIVE.hideTip();
      ACTIVE = C;
      const fresh = !tip.hasAttribute("data-open");
      if (fresh) tip.setAttribute("data-fresh", "1");
      fillTip(card);
      const w = tip.offsetWidth, hh = tip.offsetHeight, W = C.W || C.width(), Ht = Math.max(C.H, hh + 4);
      let x, y;
      if (mode === "side") { x = ax + 14; if (x + w > W - 2) x = ax - 14 - w; y = ay; }
      else {
        x = ax - w / 2; y = ay - hh - 10;
        if (y < 0) { // no room above the mark: beside it, never on top of it
          const hf = (half || 8) + 10; x = ax + hf; if (x + w > W - 2) x = ax - hf - w; y = ay - 6;
          if (x < 2 && ax + hf + w > W - 2) { x = ax - w / 2; y = ay + 12; }
        }
      }
      x = clamp(x, 2, Math.max(2, W - w - 2)); y = clamp(y, 0, Math.max(0, Ht - hh));
      tip.style.transform = "translate(" + Math.round(x) + "px," + Math.round(y) + "px)";
      tip.setAttribute("data-open", "1"); tip.setAttribute("aria-hidden", "false");
      if (fresh) raf(() => tip.removeAttribute("data-fresh"));
      C.tipTitle = card.title;
    };
    C.hideTip = () => { tip.removeAttribute("data-open"); tip.removeAttribute("data-fresh"); tip.setAttribute("aria-hidden", "true"); if (ACTIVE === C) ACTIVE = null; };
    C.watch = fn => {
      if (!root.ResizeObserver) { C.on(root, "resize", () => fn()); return; }
      let last = 0, pend = 0;
      C.ro = new root.ResizeObserver(() => { const w = Math.floor(box.clientWidth); if (!w || w === last) return; last = w; if (pend) return; pend = raf(() => { pend = 0; if (!C.dead) fn(); }); });
      C.ro.observe(box);
    };
    C.destroy = () => {
      if (C.dead) return; C.dead = true;
      C.hideTip(); for (const s of C.stops) s(); C.stops.clear(); for (const t of C.timers) clearTimeout(t); C.timers.clear();
      if (C.ro) C.ro.disconnect(); for (const off of C.listeners) off(); C.listeners = [];
      if (box.parentNode) box.parentNode.removeChild(box);
      if (el.__efc && el.__efc.__c === C) delete el.__efc;
    };
    return C;
  }
  /** The part of update() every chart shares: options merge, loading flag, "nothing to draw". Returns true when there is data to draw. */
  function intake(C, data, opts) {
    if (opts) Object.assign(C.o, opts);
    const wantsLoading = (data && data.loading === true) || (opts && opts.loading === true);
    if (wantsLoading) { C.setLoading(true, (data && data.label) || (opts && opts.loadingLabel)); }
    return !wantsLoading || (data && (data.series || data.values || data.points || data.rows || data.days || data.slices)) ? "go" : "wait";
  }
  const publish = (C, api) => { api.__c = C; C.el.__efc = api; return api; };

  /* ── shaping data: one columnar form for line, area, stacked, bars, sparkline ── */
  function xKindOf(xs, o, data) {
    if (o.xKind) return o.xKind;
    if (xs.length && xs.every(isDay)) {
      if (o.granularity === "week" || (data && data.granularity === "week")) return "week";
      if (xs.length > 2) { let wk = true; for (let i = 1; i < xs.length && wk; i++) if (Math.round((dayT(xs[i]) - dayT(xs[i - 1])) / DAY_MS) !== 7) wk = false; if (wk) return "week"; }
      return "day";
    }
    return "cat";
  }
  function shape(data, o, flavor) {
    data = data || {};
    let xs, series = [], band = null, meta = Array.isArray(data.meta) ? data.meta : null;
    const lab = o.seriesLabel || o.name || "Value";
    if (Array.isArray(data.points)) {
      const P = data.points; xs = P.map((p, i) => (p && p.x != null ? p.x : i));
      series = [{ key: "v", label: lab, values: P.map(p => N(p && (p.y != null ? p.y : p.value))), prev: P.some(p => p && p.prev != null) ? P.map(p => N(p && p.prev)) : null }];
      if (P.some(p => p && p.lo != null && p.hi != null)) band = { label: o.bandLabel || "Typical range", lo: P.map(p => N(p && p.lo)), hi: P.map(p => N(p && p.hi)) };
      if (!meta) meta = P;
    } else {
      const raw = Array.isArray(data.series) ? data.series : Array.isArray(data.values) ? [{ key: "v", label: lab, values: data.values, prev: data.prev }] : [];
      series = raw.map((s, i) => ({ key: s.key != null ? String(s.key) : "s" + i, label: s.label || s.key || "Series " + (i + 1), values: (s.values || s.vals || []).map(N), prev: Array.isArray(s.prev) ? s.prev.map(N) : null, color: s.color, dashed: !!s.dashed, def: s.def, unit: s.unit, fmt: s.fmt }));
      xs = Array.isArray(data.x) ? data.x.slice() : Array.isArray(data.labels) ? data.labels.slice() : series[0] ? series[0].values.map((_, i) => i) : [];
      if (data.band && Array.isArray(data.band.lo) && Array.isArray(data.band.hi)) band = { label: data.band.label || o.bandLabel || "Typical range", lo: data.band.lo.map(N), hi: data.band.hi.map(N) };
    }
    const kind = xKindOf(xs, o, data);
    let n = xs.length, from = [], keys = xs;
    const take = (arr, len, idx) => { const out = new Array(len).fill(null); if (arr) for (let i = 0; i < arr.length && i < xs.length; i++) out[idx[i]] = arr[i] == null ? null : arr[i]; return out; };
    const id0 = xs.map((_, i) => i);
    let idx = id0;
    if (kind === "day" && n) {
      const order = id0.slice().sort((a, b) => dayT(xs[a]) - dayT(xs[b])), t0 = dayT(xs[order[0]]), span = Math.round((dayT(xs[order[n - 1]]) - t0) / DAY_MS) + 1;
      if (span <= 800) { idx = new Array(n); for (const i of order) idx[i] = Math.round((dayT(xs[i]) - t0) / DAY_MS); keys = []; for (let i = 0; i < span; i++) keys.push(dayAdd(xs[order[0]], i)); n = span; }
      else { idx = new Array(n); order.forEach((src, k) => { idx[src] = k; }); keys = order.map(i => xs[i]); }
    } else if (kind === "week" && n) {
      const order = id0.slice().sort((a, b) => dayT(xs[a]) - dayT(xs[b])); idx = new Array(n); order.forEach((src, k) => { idx[src] = k; }); keys = order.map(i => xs[i]);
    }
    series = series.map(s => Object.assign({}, s, { values: take(s.values, n, idx), prev: s.prev ? take(s.prev, n, idx) : null }));
    if (band) band = Object.assign({}, band, { lo: take(band.lo, n, idx), hi: take(band.hi, n, idx) });
    const metaOut = meta ? (() => { const m = new Array(n).fill(null); for (let i = 0; i < meta.length && i < xs.length; i++) m[idx[i]] = meta[i]; return m; })() : null;
    let closed = null;
    if (Array.isArray(data.closed)) { closed = new Array(n).fill(false); data.closed.forEach((c, i) => { if (typeof c === "boolean") { if (c && i < xs.length) closed[idx[i]] = true; } else { const j = keys.findIndex(k => String(k) === String(c)); if (j >= 0) closed[j] = true; } }); if (!closed.some(Boolean)) closed = null; }
    return { kind, n, xs: keys, series, band, meta: metaOut, closed, hi: data.highlight != null ? data.highlight : data.hi };
  }
  const hasPositive = d => d.series.some(s => s.values.some(v => v != null && v > 0));
  const colorsOf = d => d.series.forEach((s, i) => { s.css = colorFor(s.key, i, s.color); });
  const defaultLabel = (d, o, i) => { const x = d.xs[i]; if (typeof o.xLabel === "function") return o.xLabel(x, i); if (d.kind === "day") return dayTxt(x); if (d.kind === "week") return weekTxt(x); if (d.kind === "hour") return hourTxt(+x); return String(x); };
  const tickLabel = (d, x) => (d.kind === "day" ? dayTxt(x, "short") : d.kind === "week" ? dayTxt(x, "short") : d.kind === "hour" ? hourShort(+x) : String(x));
  /** Which x labels to print: day ticks by length of range, thinned to what fits; the newest point is kept. */
  function xTicks(d, slot, phone) {
    const n = d.n; if (!n) return [];
    let cand;
    if (d.kind === "day") {
      if (n <= 7) cand = d.xs.map((x, i) => ({ i, text: dayTxt(x, "wd") }));
      else if (n <= 62) cand = d.xs.map((x, i) => ({ i, text: dayTxt(x, "short") }));
      else if (n <= 130) cand = d.xs.map((x, i) => ({ i, x })).filter(c => dow(c.x) === 1).map(c => ({ i: c.i, text: dayTxt(c.x, "short") }));
      else cand = d.xs.map((x, i) => ({ i, x })).filter(c => dd(c.x) === 1).map(c => ({ i: c.i, text: mm(c.x) === 1 ? String(c.x.slice(0, 4)) : dayTxt(c.x, "month") }));
    } else cand = d.xs.map((x, i) => ({ i, text: tickLabel(d, x) }));
    const wMax = Math.max(...cand.map(c => c.text.length), 3) * 5.9 + (phone ? 16 : 12);
    const all = cand.length === n;
    if (all) {
      let stride = Math.max(1, Math.ceil(wMax / Math.max(1, slot)));
      if (d.kind === "hour" || d.kind === "cat") { if (d.kind === "hour") stride = [1, 2, 3, 4, 6, 8, 12, 24].find(v => v >= stride) || 24; return cand.filter(c => c.i % stride === 0); }
      return cand.filter(c => (n - 1 - c.i) % stride === 0);
    }
    const out = []; let right = -1e9;
    for (const c of cand) { const x = c.i * slot; if (x - wMax / 2 >= right) { out.push(c); right = x + wMax / 2; } }
    return out;
  }
  /** The y axis: gridlines and labels kept by value, so a new scale fades the new ones in and the old out; positions follow the scale. */
  function yAxis(C, svg, g0) {
    const layer = mk(svg, "g", { class: "efc-yaxis" }), items = new Map();
    return {
      layer,
      set(vals, f, g) {
        const want = new Map(vals.map(v => [v + "|" + f(v), v]));
        for (const [key, it] of items) if (!want.has(key)) { items.delete(key); it.g.setAttribute("class", "efc-tk out"); const gone = it.g; C.later(() => gone.remove(), 300); }
        for (const [key, v] of want) {
          if (items.has(key)) continue;
          const gg = mk(layer, "g", { class: "efc-tk" + (C.drawn && !still() ? " pre" : "") });
          const ln = mk(gg, "line", { class: v === 0 ? "efc-base" : "efc-grid" }), tx = mk(gg, "text", { class: "efc-tick", "text-anchor": "end" });
          tx.textContent = f(v); items.set(key, { g: gg, ln, tx, v });
          if (C.drawn && !still()) raf(() => gg.setAttribute("class", "efc-tk"));
        }
      },
      place(g, yOf) { this.yOf = yOf; for (const it of items.values()) { const y = yOf(it.v); it.g.setAttribute("transform", "translate(0," + pf(y) + ")"); it.ln.setAttribute("x1", g.l); it.ln.setAttribute("x2", g.W - g.r); it.tx.setAttribute("x", g.l - 7); it.tx.setAttribute("y", 3.5); } },
      yOf: null
    };
  }
  /** x labels, pooled text nodes laid out again at every size / data change. */
  function xAxis(svg) {
    const layer = mk(svg, "g", { class: "efc-xaxis" }), pool = [];
    return { layer, set(ticks, xOf, g, hiIdx) {
      while (pool.length < ticks.length) pool.push(mk(layer, "text", { class: "efc-xl", "text-anchor": "middle" }));
      pool.forEach((t, k) => {
        const c = ticks[k]; if (!c) { t.setAttribute("display", "none"); return; }
        t.removeAttribute("display"); const w = c.text.length * 5.9, x = xOf(c.i);
        let a = "middle", px = x; if (x - w / 2 < 0) { a = "start"; px = 0; } else if (x + w / 2 > g.W) { a = "end"; px = g.W; }
        t.setAttribute("text-anchor", a); t.setAttribute("x", pf(px)); t.setAttribute("y", pf(g.H - 5)); t.setAttribute("class", "efc-xl" + (c.i === hiIdx ? " on" : "")); setText(t, c.text);
      });
    } };
  }
  /** Monotone cubic through the points (no overshoot): returns segments [x0,y0,c1x,c1y,c2x,c2y,x1,y1]. */
  function curveSegs(P) {
    const n = P.length, segs = [];
    if (n < 2) return segs;
    if (n === 2) { segs.push([P[0][0], P[0][1], P[0][0], P[0][1], P[1][0], P[1][1], P[1][0], P[1][1], true]); return segs; }
    const dx = [], sl = [], m = new Array(n);
    for (let i = 0; i < n - 1; i++) { dx[i] = P[i + 1][0] - P[i][0] || 1e-6; sl[i] = (P[i + 1][1] - P[i][1]) / dx[i]; }
    m[0] = sl[0]; m[n - 1] = sl[n - 2];
    for (let i = 1; i < n - 1; i++) m[i] = sl[i - 1] * sl[i] <= 0 ? 0 : (sl[i - 1] + sl[i]) / 2;
    for (let i = 0; i < n - 1; i++) {
      if (sl[i] === 0) { m[i] = 0; m[i + 1] = 0; continue; }
      const a = m[i] / sl[i], b = m[i + 1] / sl[i], s = a * a + b * b; if (s > 9) { const t = 3 / Math.sqrt(s); m[i] = t * a * sl[i]; m[i + 1] = t * b * sl[i]; }
    }
    for (let i = 0; i < n - 1; i++) { const w = dx[i] / 3; segs.push([P[i][0], P[i][1], P[i][0] + w, P[i][1] + m[i] * w, P[i + 1][0] - w, P[i + 1][1] - m[i + 1] * w, P[i + 1][0], P[i + 1][1]]); }
    return segs;
  }
  const pt2 = (x, y) => pf(x) + "," + pf(y);
  const fwd = segs => segs.map(s => "C" + pt2(s[2], s[3]) + " " + pt2(s[4], s[5]) + " " + pt2(s[6], s[7])).join("");
  const back = segs => segs.slice().reverse().map(s => "C" + pt2(s[4], s[5]) + " " + pt2(s[2], s[3]) + " " + pt2(s[0], s[1])).join("");
  /** Runs of consecutive non-null points. */
  function runs(xs, ys, skip) { const out = []; let cur = null; for (let i = 0; i < ys.length; i++) { if (ys[i] == null) { if (!(skip && skip[i])) cur = null; continue; } if (!cur) { cur = []; out.push(cur); } cur.push([xs[i], ys[i]]); } return out; }
  const linePath = rs => rs.map(r => (r.length === 1 ? "M" + pt2(r[0][0], r[0][1]) + "h0.01" : "M" + pt2(r[0][0], r[0][1]) + fwd(curveSegs(r)))).join("");

  /* ═════════════ line · area · stacked area · sparkline ═════════════ */
  function lineChart(el, o0, flavor) {
    const C = core(el, flavor, o0), spark = flavor === "sparkline", stacked = flavor === "stacked", areaCls = stacked ? "efc-fill" : "efc-area";
    const S = { d: null, cur: null, tgt: null, svg: null, g: null, ser: new Map(), first: true, drawnData: false, sig: "", lsig: "", stopM: null, hover: null, ptr: false, touch: false };
    let yax = null, xax = null, hit, guide, guideLine, bandEl, brush = null, layers = {};
    const o = C.o;
    function geometry(d, ticks) {
      const W = C.width(), phone = W < 520, o = C.o;
      let H = o.height || (spark ? 28 : 220); if (phone && !spark) H = o.heightSm || Math.round(H * .85);
      const f = C.fmtTick(), lw = spark ? 0 : Math.max(...ticks.map(v => f(v).length), 2) * 6 + 12;
      const g = { W, H, l: spark ? 6 : Math.max(32, lw), r: spark ? 6 : 12, t: spark ? 4 : 12, b: spark ? 4 : 24 };
      g.pw = Math.max(10, W - g.l - g.r); g.ph = Math.max(10, H - g.t - g.b);
      g.xOf = i => (d.n <= 1 ? g.l + g.pw / 2 : g.l + (g.pw * i) / (d.n - 1));
      return g;
    }
    function scale(d) {
      const o = C.o, phone = C.width() < 520; let mx = 0, mn = Infinity;
      const upd = v => { if (v != null) { if (v > mx) mx = v; if (v < mn) mn = v; } };
      if (stacked) { for (let i = 0; i < d.n; i++) { let t = 0; for (const s of d.series) t += s.values[i] || 0; upd(t); } if (o.share) { mx = 100; } }
      else for (const s of d.series) { s.values.forEach(upd); if (s.prev) s.prev.forEach(upd); }
      if (d.band) d.band.hi.forEach(upd);
      const H = o.height || (spark ? 28 : 220), intervals = spark ? 2 : phone ? 3 : H >= 200 ? 4 : 3;
      if (spark) { const auto = (o.yMin == null || o.yMin === "auto"); if (auto && mn < Infinity && mx > mn) { const pad = (mx - mn) * .18; return { min: Math.max(0, mn - pad), max: mx + pad, ticks: [] }; } return { min: 0, max: mx > 0 ? mx : 1, ticks: [] }; }
      const unit = stacked && o.share ? "percent" : o.unit, ymin = typeof o.yMin === "number" ? o.yMin : 0;
      const t = niceTicks(mx, intervals, unit, o.yMax != null ? +o.yMax : (unit === "percent" && mx <= 100 ? 100 : null));
      return { min: ymin, max: t.max, ticks: mx > 0 ? t.ticks : [0] };
    }
    /* the svg and its fixed layers are built once; series elements are kept by key */
    function ensure() {
      if (S.svg) return;
      const svg = S.svg = mk(null, "svg", { class: "efc-svg", role: "group", "aria-roledescription": "chart", tabindex: spark && C.o.hover === false ? "-1" : "0", "aria-describedby": C.tip.id });
      C.body.appendChild(svg);
      yax = yAxis(C, svg); xax = xAxis(svg);
      layers.closed = mk(svg, "g"); layers.band = mk(svg, "g"); layers.ser = mk(svg, "g");
      guide = mk(svg, "g", { class: "efc-guide" }); guideLine = mk(guide, "line", { y1: 0, y2: 10 }); layers.dots = mk(guide, "g");
      hit = mk(svg, "rect", { class: "efc-hit" + (C.o.onRange ? " zoom" : ""), fill: "transparent" });
      pointerWire(); keyWire();
    }
    function seriesEls(s) {
      let e = S.ser.get(s.key);
      if (e) { e.seen = true; return e; }
      const gEl = mk(layers.ser, "g", { class: "efc-ser" + (S.drawnData && !still() ? " pre" : "") }); gEl.style.setProperty("--c", s.css);
      e = { g: gEl, seen: true };
      if (flavor !== "line" && flavor !== "sparkline" || C.o.area || (spark && C.o.fill !== false)) e.area = mk(gEl, "path", { class: areaCls + (S.drawnData && !still() ? " pre" : "") });
      e.prev = mk(gEl, "path", { class: "efc-prev" }); e.line = mk(gEl, "path", { class: stacked ? "efc-edge" : "efc-line", pathLength: "1" });
      e.mk = []; e.end = mk(gEl, "circle", { class: "efc-mk", r: spark ? 3 : 4 }); e.end.setAttribute("display", "none");
      if (S.drawnData && !still()) raf(() => { gEl.setAttribute("class", "efc-ser"); if (e.area) e.area.setAttribute("class", areaCls); });
      S.ser.set(s.key, e); return e;
    }
    function dotsFor(d) {
      layers.dots.textContent = ""; S.dots = [];
      d.series.forEach(s => { const c = mk(layers.dots, "circle", { class: "efc-dot", r: 4.5, cx: 0, cy: 0 }); c.style.setProperty("--c", s.css); c.setAttribute("display", "none"); S.dots.push(c); });
    }
    /* painting one frame from the animated arrays */
    function paint(cur) {
      const d = S.d, g = S.g, yOf = v => g.t + g.ph * (1 - (v - cur.min) / (cur.max - cur.min || 1)), base = g.t + g.ph, xs = cur.px;
      const f = C.fmtTick(); void f;
      let below = null;
      d.series.forEach((s, si) => {
        const e = S.ser.get(s.key); if (!e) return;
        let ys = cur.ys[si], top = ys;
        if (stacked) { top = ys.map((v, i) => { let t = 0, any = false; for (let j = 0; j <= si; j++) { const w = cur.ys[j][i]; if (w != null) { t += w; any = true; } } return any ? (cur.share ? t / (cur.tot[i] || 1) * 100 : t) : null; }); }
        const pys = top.map(v => (v == null ? null : yOf(v))), rs = runs(xs, pys, d.closed);
        e.line.setAttribute("d", linePath(rs));
        if (e.area) {
          let dA = "";
          if (stacked) {
            const lowArr = below, lowRs = lowArr ? lowArr.map(v => (v == null ? null : yOf(v))) : null;
            for (const r of runs(xs, pys, d.closed)) {
              const idx = r.map(p => xs.indexOf(p[0])); const segsT = curveSegs(r);
              const lowPts = idx.map((ii, k) => [r[k][0], lowRs ? (lowRs[ii] == null ? base : lowRs[ii]) : base]);
              if (r.length === 1) { dA += "M" + pt2(r[0][0], r[0][1]) + "L" + pt2(r[0][0], lowPts[0][1]) + "Z"; continue; }
              dA += "M" + pt2(r[0][0], r[0][1]) + fwd(segsT) + "L" + pt2(lowPts[lowPts.length - 1][0], lowPts[lowPts.length - 1][1]) + back(curveSegs(lowPts)) + "Z";
            }
          } else for (const r of rs) { if (r.length < 2) continue; dA += "M" + pt2(r[0][0], r[0][1]) + fwd(curveSegs(r)) + "L" + pt2(r[r.length - 1][0], base) + "L" + pt2(r[0][0], base) + "Z"; }
          e.area.setAttribute("d", dA);
        }
        if (stacked) below = top;
        if (s.prev && !stacked) e.prev.setAttribute("d", linePath(runs(xs, cur.pv[si].map(v => (v == null ? null : yOf(v))), d.closed))); else e.prev.setAttribute("d", "");
        // markers: every point when there are few, the last known point always, a lone point (no neighbours) always
        const show = !stacked && (C.o.markers != null ? C.o.markers : d.n <= 16 && !spark);
        const idxs = []; if (show) for (let i = 0; i < d.n; i++) if (pys[i] != null) idxs.push(i);
        while (e.mk.length < idxs.length) { const c = mk(e.g, "circle", { class: "efc-mk", r: 2.6 }); e.g.insertBefore(c, e.end); e.mk.push(c); }
        e.mk.forEach((c, k) => { const i = idxs[k]; if (i == null) { c.setAttribute("display", "none"); return; } c.removeAttribute("display"); c.setAttribute("cx", pf(xs[i])); c.setAttribute("cy", pf(pys[i])); });
        let last = -1; for (let i = d.n - 1; i >= 0; i--) if (pys[i] != null) { last = i; break; }
        if (last >= 0 && !stacked && (spark || !show)) { e.end.removeAttribute("display"); e.end.setAttribute("cx", pf(xs[last])); e.end.setAttribute("cy", pf(pys[last])); } else e.end.setAttribute("display", "none");
      });
      if (d.band && bandEl) {
        const lo = cur.lo.map(v => (v == null ? null : yOf(v))), hi = cur.hi.map(v => (v == null ? null : yOf(v))); let dB = "";
        const both = hi.map((v, i) => (v == null || lo[i] == null ? null : i));
        let run = []; const flush = () => { if (run.length > 1) { const H2 = run.map(i => [xs[i], hi[i]]), L2 = run.map(i => [xs[i], lo[i]]); dB += "M" + pt2(H2[0][0], H2[0][1]) + fwd(curveSegs(H2)) + "L" + pt2(L2[L2.length - 1][0], L2[L2.length - 1][1]) + back(curveSegs(L2)) + "Z"; } run = []; };
        both.forEach((i, k) => { if (i == null) { if (!(d.closed && d.closed[k])) flush(); } else run.push(i); }); flush();
        bandEl.setAttribute("d", dB);
      }
      if (!spark) {
        // closed days (weekends, holidays) are a faint stripe; the line goes across them
        const stripes = []; if (d.closed) { let a = -1; for (let i = 0; i <= d.n; i++) { const c = i < d.n && d.closed[i]; if (c && a < 0) a = i; if (!c && a >= 0) { stripes.push([a, i - 1]); a = -1; } } }
        const half = d.n > 1 ? g.pw / (d.n - 1) / 2 : 0;
        S.stripes = S.stripes || [];
        while (S.stripes.length < stripes.length) S.stripes.push(mk(layers.closed, "rect", { class: "efc-closed" }));
        S.stripes.forEach((r, k) => { const z = stripes[k]; if (!z) { r.setAttribute("display", "none"); return; } r.removeAttribute("display"); const x0 = Math.max(g.l, xs[z[0]] - half), x1 = Math.min(g.l + g.pw, xs[z[1]] + half); r.setAttribute("x", pf(x0)); r.setAttribute("width", pf(Math.max(1, x1 - x0))); r.setAttribute("y", g.t); r.setAttribute("height", g.ph); });
      }
      if (yax && !spark) yax.place(g, yOf);
      S.yOf = yOf;
      if (S.hover != null) moveGuide(S.hover, true);
    }
    /* arrays: from → to */
    function targetOf(d, g, sc) {
      const px = []; for (let i = 0; i < d.n; i++) px.push(g.xOf(i));
      let tot = null;
      if (stacked && C.o.share) { tot = []; for (let i = 0; i < d.n; i++) { let t = 0; for (const s of d.series) t += s.values[i] || 0; tot.push(t); } }
      return { px, ys: d.series.map(s => s.values.slice()), pv: d.series.map(s => (s.prev ? s.prev.slice() : new Array(d.n).fill(null))), lo: d.band ? d.band.lo.slice() : [], hi: d.band ? d.band.hi.slice() : [], min: sc.min, max: sc.max, share: !!(stacked && C.o.share), tot };
    }
    /** What the old drawing looks like at the new points: by x key when the points overlap, else by position along the curve. */
    function startFrom(old, oldD, d, g) {
      const oldIdx = new Map(oldD.xs.map((x, i) => [String(x), i])), n = d.n, m = oldD.n;
      const sameKeys = oldD.n === d.n && oldD.xs.every((x, i) => String(x) === String(d.xs[i]));
      const out = { px: [], ys: [], pv: [], lo: [], hi: [], min: old.min, max: old.max, share: old.share, tot: old.tot };
      const pick = (arr, i) => { // old arrays value for new index i
        if (sameKeys) return arr[i];
        let j = oldIdx.get(String(d.xs[i]));
        if (j == null) { if (d.kind === "day" || d.kind === "week") { const t = dayT(d.xs[i]); j = t < dayT(oldD.xs[0]) ? 0 : m - 1; } else j = m <= 1 ? 0 : Math.round(i / Math.max(1, n - 1) * (m - 1)); }
        return arr[j];
      };
      for (let i = 0; i < n; i++) {
        let jx = oldIdx.get(String(d.xs[i]));
        out.px.push(sameKeys ? old.px[i] : jx != null ? old.px[jx] : (d.kind === "day" || d.kind === "week") ? (dayT(d.xs[i]) < dayT(oldD.xs[0]) ? old.px[0] : old.px[m - 1]) : g.l + g.pw * (m <= 1 ? .5 : Math.round(i / Math.max(1, n - 1) * (m - 1)) / (m - 1)));
      }
      d.series.forEach((s, si) => {
        const oi = oldD.series.findIndex(x => x.key === s.key);
        out.ys.push(oi < 0 ? new Array(n).fill(null).map((_, i) => (s.values[i] == null ? null : 0)) : out.px.map((_, i) => pick(old.ys[oi], i)));
        out.pv.push(oi < 0 ? s.prev ? s.prev.slice() : new Array(n).fill(null) : out.px.map((_, i) => pick(old.pv[oi], i)));
      });
      if (d.band) { out.lo = oldD.band ? out.px.map((_, i) => pick(old.lo, i)) : d.band.lo.slice(); out.hi = oldD.band ? out.px.map((_, i) => pick(old.hi, i)) : d.band.hi.slice(); }
      return out;
    }
    const mix = (a, b, k) => (b == null ? null : a == null ? b : a + (b - a) * k);
    function morph(from, to, ms) {
      if (S.stopM) { S.stopM(); S.stopM = null; }
      const frame = k => {
        S.cur = { px: to.px.map((b, i) => (from.px[i] == null ? b : from.px[i] + (b - from.px[i]) * k)), ys: to.ys.map((arr, si) => arr.map((b, i) => mix(from.ys[si] && from.ys[si][i], b, k))), pv: to.pv.map((arr, si) => arr.map((b, i) => mix(from.pv[si] && from.pv[si][i], b, k))), lo: to.lo.map((b, i) => mix(from.lo[i], b, k)), hi: to.hi.map((b, i) => mix(from.hi[i], b, k)), min: from.min + (to.min - from.min) * k, max: from.max + (to.max - from.max) * k, share: to.share, tot: to.tot };
        paint(S.cur);
      };
      S.stopM = C.tween(ms, frame, () => { S.cur = to; paint(to); S.stopM = null; });
    }
    /* layout: sizes, axes, hit area */
    function layout(d, sc) {
      const g = S.g = geometry(d, sc.ticks), svg = S.svg; C.W = g.W; C.H = g.H;
      svg.setAttribute("width", g.W); svg.setAttribute("height", g.H); svg.setAttribute("viewBox", "0 0 " + g.W + " " + g.H);
      hit.setAttribute("x", spark ? 0 : g.l - 4); hit.setAttribute("y", 0); hit.setAttribute("width", spark ? g.W : g.pw + g.r + 4); hit.setAttribute("height", g.H);
      guideLine.setAttribute("y1", g.t - 2); guideLine.setAttribute("y2", g.t + g.ph);
      if (!spark) {
        yax.layer.removeAttribute("display");
        const yOf0 = v => g.t + g.ph * (1 - (v - sc.min) / (sc.max - sc.min || 1));
        yax.set(sc.ticks, C.fmtTick(), g); yax.place(g, yOf0);
        const slot = d.n > 1 ? g.pw / (d.n - 1) : g.pw; xax.set(xTicks(d, slot, g.W < 520), g.xOf, g, d.hiIdx);
      } else if (yax) yax.layer.setAttribute("display", "none");
      return g;
    }
    function build(d, o) {
      ensure(); colorsOf(d);
      for (const e of S.ser.values()) e.seen = false;
      d.series.forEach(seriesEls);
      for (const [key, e] of S.ser) if (!e.seen) { S.ser.delete(key); e.g.setAttribute("class", "efc-ser out"); const gone = e.g; C.later(() => gone.remove(), 450); }
      // keep the drawing order of the series
      d.series.forEach(s => layers.ser.appendChild(S.ser.get(s.key).g));
      if (d.band) { if (!bandEl) { bandEl = mk(layers.band, "path", { class: "efc-band" }); } } else if (bandEl) { bandEl.remove(); bandEl = null; }
      const dsig = d.series.map(s => s.key + s.css).join("|"); if (dsig !== S.dsig) { S.dsig = dsig; dotsFor(d); }
    }
    function sigOf(d) { return JSON.stringify([d.kind, d.xs, d.series.map(s => [s.key, s.values, s.prev, s.css]), d.band]); }
    function update(data, opts) {
      if (C.dead) return;
      const mode = intake(C, data, opts); if (mode === "wait") return;
      if (data === undefined && !S.d) return;
      const o = C.o;
      if (data !== undefined) S.raw = data;
      const d = shape(data === undefined ? S.raw : data, o, flavor);
      d.hiIdx = (() => { const hx = d.hi; if (hx == null) return -1; if (typeof hx === "number") return hx; const j = d.xs.indexOf(hx); return j; })();
      d.hasData = hasPositive(d);
      C.hasData = d.hasData;
      const prevD = S.d, prevCur = S.cur;
      ensure(); S.d = d;
      const sc = scale(d), sig = sigOf(d) + "|" + C.width() + "|" + (o.height || 0) + "|" + (o.share ? 1 : 0) + "|" + o.unit + "|" + (o.area ? 1 : 0);
      if (data !== undefined && sig === S.sig && !(opts && (opts.loading != null))) { /* nothing moved: the card may need new text */ if (S.hover != null) show(S.hover, S.hoverSrc); C.setState(d.hasData ? "ready" : "empty"); return; }
      S.sig = sig; S.raw = data === undefined ? S.raw : data;
      if (!d.hasData) { C.setState("empty"); } else C.setState("ready");
      { const items = []; if (d.series.length > 1) d.series.forEach((x, i) => items.push({ label: x.label, css: colorFor(x.key, i, x.color), kind: stacked ? "sq" : "line" })); if (!stacked && d.series.some(x => x.prev)) items.push({ label: o.prevLabel || "Previous period", kind: "dash" }); if (d.band) items.push({ label: d.band.label, kind: "band" }); C.legend(d.hasData ? items : []); }
      build(d, o); const g = layout(d, sc), to = targetOf(d, g, sc);
      const firstDraw = !prevD || !prevD.hasData || !prevCur || !S.drawnData, snap = S.snap; S.snap = false;
      // keep the hover on the same x
      let keepIdx = null;
      if (S.hover != null) { const hx = prevD ? prevD.xs[S.hover] : null; const j = hx != null ? d.xs.findIndex(x => String(x) === String(hx)) : -1; keepIdx = j >= 0 ? j : Math.min(S.hover, d.n - 1); }
      if (!d.hasData) { S.cur = to; S.drawnData = false; paint(to); C.drawn = true; if (S.hover != null) hide(); return; }
      if (firstDraw || still() || snap) {
        S.cur = to; paint(to); C.drawn = true;
        if (!still() && !snap) entrance(d);
        S.drawnData = true; S.first = false;
      } else {
        const from = startFrom(prevCur, prevD, d, S.g); S.drawnData = true; C.drawn = true;
        const rangeChange = !(prevD.xs.length === d.xs.length && prevD.xs.every((x, i) => String(x) === String(d.xs[i])));
        morph(from, to, rangeChange ? 620 : 420);
      }
      if (keepIdx != null) show(keepIdx, S.hoverSrc || "ptr", true);
    }
    function entrance(d) {
      // the line draws itself in; the wash and markers fade up (all CSS transitions on stroke-dashoffset / opacity)
      for (const e of S.ser.values()) { e.line.setAttribute("class", (stacked ? "efc-edge" : "efc-line") + (stacked ? "" : " draw")); }
      raf(() => raf(() => { for (const e of S.ser.values()) e.line.setAttribute("class", stacked ? "efc-edge" : "efc-line"); }));
      for (const e of S.ser.values()) { if (e.area) { e.area.setAttribute("class", areaCls + " pre"); raf(() => raf(() => e.area.setAttribute("class", areaCls))); } }
      if (bandEl) { bandEl.setAttribute("class", "efc-band pre"); raf(() => raf(() => bandEl.setAttribute("class", "efc-band"))); }
      fade(xax.layer, 400);
    }
    /* the card */
    function pointOf(i) {
      const d = S.d, values = {}, prev = {}, ser = [];
      d.series.forEach((s, si) => { const v = s.values[i], p = s.prev ? s.prev[i] : null; values[s.key] = v; prev[s.key] = p; ser.push({ key: s.key, label: s.label, value: v, prev: p, delta: delta(v, p) }); });
      const p = { index: i, x: d.xs[i], label: defaultLabel(d, C.o, i), values, prev, meta: d.meta ? d.meta[i] : null, series: ser };
      if (typeof C.o.label === "function") { const l = C.o.label(p); if (l != null) p.label = String(l); }
      return p;
    }
    function toneOf(dl) { const b = C.o.better === undefined ? "up" : C.o.better; if (!dl.dir || dl.dir === "flat" || b == null) return ""; return (dl.dir === "up") === (b === "up") ? "good" : "bad"; }
    function cardOf(i) {
      const d = S.d, o = C.o, p = pointOf(i), rows = [], fl = C.fmtLong(), card = { title: p.label, rows };
      const single = d.series.length === 1, unitTxt = u => u;
      const fv = (s, v) => (s.fmt ? s.fmt(v) : fl(v));
      if (single) {
        const s = d.series[0], v = s.values[i];
        if (v == null) card.big = { text: d.closed && d.closed[i] ? (o.closedText || "Closed") : (o.nullText || "Nothing logged") };
        else card.big = splitBig(fv(s, v));
        if (s.prev && s.prev[i] != null && v != null) { const dl = delta(v, s.prev[i]); rows.push({ k: o.prevLabel || "Previous period", v: fv(s, s.prev[i]), muted: true, dashed: true, delta: dl.text ? { text: dl.text, tone: toneOf(dl) } : null }); }
        else if (s.prev && v != null) rows.push({ k: o.prevLabel || "Previous period", v: "—", muted: true, dashed: true });
        if (s.def && !o.def) card.foot = s.def;
      } else {
        let tot = 0, any = false;
        d.series.forEach(s => { const v = s.values[i]; if (v != null) { tot += v; any = true; } });
        if (!any) { card.big = { text: d.closed && d.closed[i] ? (o.closedText || "Closed") : (o.nullText || "Nothing logged") }; if (o.def) card.foot = o.def; return card; }
        if (stacked && C.o.share && any && tot > 0) card.big = { text: fl(tot) };
        d.series.forEach(s => {
          const v = s.values[i], row = { k: s.label, v: v == null ? "—" : fv(s, v), color: s.css, sq: stacked };
          if (stacked && C.o.share && v != null && tot > 0) row.v += "  ·  " + percent(v / tot * 100);
          if (s.prev && s.prev[i] != null && v != null) { const dl = delta(v, s.prev[i]); if (dl.text) row.delta = { text: dl.text, tone: toneOf(dl) }; }
          rows.push(row);
        });
        if (stacked && any && !(C.o.share)) rows.push({ k: "Total", v: fl(tot), strong: true });
        else if (!stacked) {
          const withPrev = d.series.filter(s => s.prev && s.prev[i] != null && s.values[i] != null);
          withPrev.forEach(s => rows.push({ k: (o.prevLabel || "Previous period") + (d.series.length > 1 ? " · " + s.label : ""), v: fv(s, s.prev[i]), muted: true, dashed: true }));
        }
      }
      if (d.band && d.band.lo[i] != null && d.band.hi[i] != null) rows.push({ k: d.band.label, v: C.fmtTick()(d.band.lo[i]) + " – " + C.fmtLong()(d.band.hi[i]), muted: true });
      if (typeof o.tipRows === "function") { const extra = o.tipRows(p) || []; for (const r of extra) rows.push(Array.isArray(r) ? { k: String(r[0]), v: String(r[1]) } : { k: String(r.k), v: String(r.v), delta: r.tone ? { text: r.text || "", tone: r.tone } : null, muted: r.muted }); }
      if (o.def) card.foot = o.def;
      void unitTxt; return card;
    }
    function moveGuide(i, quiet) {
      const g = S.g, d = S.d, cur = S.cur; if (!g || !d || !cur) return;
      const x = cur.px[i] != null ? cur.px[i] : g.xOf(i), yOf = S.yOf || (v => g.t + g.ph * (1 - v / (S.cur.max || 1)));
      if (!guide.classList.contains("show")) guide.classList.add("fresh");
      guide.style.transform = "translate(" + pf(x) + "px,0)"; guideLine.setAttribute("x1", 0); guideLine.setAttribute("x2", 0);
      guide.classList.add("show"); if (guide.classList.contains("fresh")) raf(() => guide.classList.remove("fresh"));
      let acc = 0;
      d.series.forEach((s, si) => {
        const dot = S.dots[si]; let v = cur.ys[si] ? cur.ys[si][i] : null;
        if (v == null) { dot.setAttribute("display", "none"); return; }
        if (stacked) { acc += v; dot.setAttribute("cy", 0); dot.style.transform = "translate(0," + pf(yOf(cur.share ? acc / (cur.tot[i] || 1) * 100 : acc)) + "px)"; }
        else dot.style.transform = "translate(0," + pf(yOf(v)) + "px)";
        dot.removeAttribute("display");
      });
      void quiet;
    }
    function show(i, src, keep) {
      const d = S.d; if (!d || !d.n || !d.hasData) return;
      i = clamp(Math.round(i), 0, d.n - 1);
      if (!guide.classList.contains("show")) { for (const dot of S.dots) dot.style.transition = "none"; raf(() => { for (const dot of S.dots) dot.style.transition = ""; }); }
      S.hover = i; S.hoverSrc = src || "ptr"; C.idx = i;
      moveGuide(i);
      const g = S.g, x = (S.cur && S.cur.px[i] != null) ? S.cur.px[i] : g.xOf(i);
      let ay = g.t; if (src === "touch") ay = g.t;
      C.showTip(cardOf(i), x, ay, "side");
      void keep;
    }
    function hide() { S.hover = null; S.hoverSrc = null; C.idx = -1; if (guide) guide.classList.remove("show"); C.hideTip(); }
    function indexAt(e) { const r = S.svg.getBoundingClientRect(), x = (e.clientX - r.left) * ((S.g.W) / (r.width || S.g.W)), d = S.d; if (d.n <= 1) return 0; return clamp(Math.round((x - S.g.l) / S.g.pw * (d.n - 1)), 0, d.n - 1); }
    function pointerWire() {
      const svg = S.svg; let down = null;
      C.on(hit, "pointerenter", e => { if (e.pointerType === "touch") return; S.ptr = true; });
      C.on(hit, "pointermove", e => {
        if (!S.d || !S.d.hasData) return;
        if (e.pointerType === "touch") { if (down) show(indexAt(e), "touch"); return; }
        if (down && C.o.onRange) { brushMove(e, down); return; }
        S.ptr = true; if (C.o.hover === false) return; show(indexAt(e), "ptr");
      });
      C.on(hit, "pointerdown", e => {
        if (!S.d || !S.d.hasData) return;
        down = { x: e.clientX, i: indexAt(e), touch: e.pointerType === "touch", moved: false };
        if (e.pointerType === "touch") { show(down.i, "touch"); try { hit.setPointerCapture(e.pointerId); } catch (_) {} }
        else if (C.o.onRange) { try { hit.setPointerCapture(e.pointerId); } catch (_) {} }
      });
      const up = e => {
        const dn = down; down = null; if (!dn) return;
        if (dn.moved && C.o.onRange) { brushEnd(e, dn); return; }
        const i = indexAt(e);
        if (typeof C.o.onPoint === "function" && S.d && S.d.hasData && (!dn.touch || S.tapped === i)) { C.o.onPoint(pointOf(i)); }
        S.tapped = dn.touch ? i : null;
      };
      C.on(hit, "pointerup", up); C.on(hit, "pointercancel", () => { down = null; if (brush) { brush.remove(); brush = null; } });
      C.on(svg, "pointerleave", e => { S.ptr = false; if (e.pointerType === "touch") return; if (!down || !C.o.onRange) hide(); });
      C.on(doc, "pointerdown", e => { if (S.hover != null && !C.root.contains(e.target)) hide(); });
    }
    function brushMove(e, dn) {
      if (Math.abs(e.clientX - dn.x) < 6 && !dn.moved) return; dn.moved = true;
      const r = S.svg.getBoundingClientRect(), s = (S.g.W) / (r.width || S.g.W), x0 = clamp((dn.x - r.left) * s, S.g.l, S.g.l + S.g.pw), x1 = clamp((e.clientX - r.left) * s, S.g.l, S.g.l + S.g.pw);
      if (!brush) { brush = mk(S.svg, "rect", { class: "efc-brush" }); S.svg.insertBefore(brush, hit); }
      brush.setAttribute("x", Math.min(x0, x1)); brush.setAttribute("width", Math.abs(x1 - x0)); brush.setAttribute("y", S.g.t); brush.setAttribute("height", S.g.ph); hide();
    }
    function brushEnd(e, dn) {
      if (brush) { brush.remove(); brush = null; }
      const i1 = indexAt(e), a = Math.min(dn.i, i1), b = Math.max(dn.i, i1);
      if (b > a && typeof C.o.onRange === "function") C.o.onRange(S.d.xs[a], S.d.xs[b], { from: a, to: b });
    }
    function keyWire() {
      const svg = S.svg;
      C.on(svg, "focus", () => { if (S.ptr || !S.d || !S.d.hasData || C.o.hover === false) return; if (S.hover == null) { let last = S.d.n - 1; const s0 = S.d.series[0]; while (last > 0 && s0.values[last] == null) last--; show(last, "key"); } });
      C.on(svg, "blur", () => { if (!S.ptr) hide(); });
      C.on(svg, "keydown", e => {
        const d = S.d; if (!d || !d.hasData) return; const k = e.key;
        if (!["ArrowLeft", "ArrowRight", "Home", "End", "Escape", "Enter", " "].includes(k)) return;
        e.preventDefault(); if (k === "Escape") return hide();
        const cur = S.hover == null ? d.n - 1 : S.hover;
        if (k === "Enter" || k === " ") { if (typeof C.o.onPoint === "function") C.o.onPoint(pointOf(cur)); return; }
        const step = e.shiftKey ? 7 : 1; show(k === "Home" ? 0 : k === "End" ? d.n - 1 : cur + (k === "ArrowRight" ? step : -step), "key");
      });
    }
    C.watch(() => { if (S.d) { S.sig = ""; S.snap = true; const keep = S.hover; update(undefined); if (keep != null && S.hover == null) show(keep, S.hoverSrc || "ptr"); } });
    const api = publish(C, { update, destroy: () => C.destroy(), show: i => show(i, "api"), hide, setLoading: (on, label) => C.setLoading(on, label), get el() { return C.root; } });
    if (o.data) update(o.data); else if (o.loading) C.setLoading(true); else C.setState("loading");
    return api;
  }

  /* ═════════════ bars: grouped, stacked, the current bar in gold ═════════════ */
  function barsChart(el, o0) {
    const C = core(el, "bars", o0), S = { d: null, cur: null, svg: null, g: null, hover: null, sig: "", stopM: null, ptr: false, bars: [], drawnData: false };
    let yax, xax, hit, layers = {}, slotEls = [], pvEls = [], vals = [];
    const stackOn = () => !!C.o.stack;
    function ensure() {
      if (S.svg) return;
      const svg = S.svg = mk(null, "svg", { class: "efc-svg", role: "group", "aria-roledescription": "chart", tabindex: "0", "aria-describedby": C.tip.id });
      C.body.appendChild(svg); yax = yAxis(C, svg); xax = xAxis(svg);
      layers.slot = mk(svg, "g"); layers.bars = mk(svg, "g"); layers.pv = mk(svg, "g"); layers.val = mk(svg, "g");
      hit = mk(svg, "rect", { class: "efc-hit", fill: "transparent" });
      wire();
    }
    function scale(d) {
      const o = C.o, phone = C.width() < 520; let mx = 0;
      for (let i = 0; i < d.n; i++) {
        if (stackOn()) { let t = 0; for (const s of d.series) t += s.values[i] || 0; mx = Math.max(mx, t); }
        else for (const s of d.series) { if (s.values[i] != null) mx = Math.max(mx, s.values[i]); if (s.prev && s.prev[i] != null) mx = Math.max(mx, s.prev[i]); }
      }
      const H = o.height || 200, intervals = phone ? 3 : H >= 190 ? 4 : 3;
      const t = niceTicks(mx, intervals, o.unit, o.yMax != null ? +o.yMax : (o.unit === "percent" && mx <= 100 ? 100 : null));
      return mx > 0 ? t : { max: t.max, step: t.step, ticks: [0] };
    }
    function geometry(d, ticks) {
      const W = C.width(), phone = W < 520, o = C.o; let H = o.height || 200; if (phone) H = o.heightSm || Math.round(H * .88);
      const f = C.fmtTick(), lw = Math.max(...ticks.map(v => f(v).length), 2) * 6 + 12;
      const g = { W, H, l: Math.max(32, lw), r: 8, t: 16, b: 24 }; g.pw = Math.max(10, W - g.l - g.r); g.ph = Math.max(10, H - g.t - g.b);
      g.slot = g.pw / Math.max(1, d.n); g.base = g.t + g.ph; g.xc = i => g.l + g.slot * (i + .5);
      const S2 = Math.max(1, d.series.length), maxBar = o.maxBar || 24;
      if (stackOn() || S2 === 1) { g.gw = clamp(g.slot - Math.min(3, g.slot * .3), 2, maxBar); g.bw = g.gw; g.nb = 1; }
      else { g.gw = clamp(g.slot - Math.min(3, g.slot * .3), 2, maxBar * S2 * .8); g.bw = Math.max(2, (g.gw - (S2 - 1) * 2) / S2); g.nb = S2; }
      return g;
    }
    const bar = (x, y0, y1, w, r) => { const hgt = y0 - y1; if (hgt <= 0.05) return ""; r = Math.min(r, hgt, w / 2); return r > 0.5 ? "M" + pf(x) + "," + pf(y0) + "V" + pf(y1 + r) + "Q" + pf(x) + "," + pf(y1) + " " + pf(x + r) + "," + pf(y1) + "H" + pf(x + w - r) + "Q" + pf(x + w) + "," + pf(y1) + " " + pf(x + w) + "," + pf(y1 + r) + "V" + pf(y0) + "Z" : "M" + pf(x) + "," + pf(y0) + "V" + pf(y1) + "H" + pf(x + w) + "V" + pf(y0) + "Z"; };
    function highlightIdx(d) { const hx = d.hi, o = C.o; let h2 = o.highlight != null ? o.highlight : hx; if (typeof h2 === "function") { for (let i = 0; i < d.n; i++) if (h2(d.xs[i], i)) return i; return -1; } if (h2 == null) return -1; if (typeof h2 === "number" && !(d.kind === "day" || d.kind === "week") ) return h2; const j = d.xs.findIndex(x => String(x) === String(h2)); return j >= 0 ? j : typeof h2 === "number" ? h2 : -1; }
    function paint(cur) {
      const d = S.d, g = S.g, yOf = v => g.base - g.ph * (v / (cur.max || 1)), single = d.series.length === 1 && !stackOn(), hi = S.hi;
      for (let i = 0; i < d.n; i++) {
        const x0 = g.l + g.slot * i + (g.slot - g.gw) / 2;
        if (stackOn()) {
          let acc = 0, lastNZ = -1; d.series.forEach((s, si) => { if ((cur.ys[si][i] || 0) > 0) lastNZ = si; });
          d.series.forEach((s, si) => {
            const v = cur.ys[si][i] || 0, el2 = S.bars[si * d.n + i], y0 = yOf(acc), y1 = yOf(acc + v); acc += v;
            const gap = si === 0 || v <= 0 ? 0 : 2; el2.setAttribute("d", v > 0 ? bar(x0, y0 - gap, y1, g.bw, si === lastNZ ? 4 : 0) : "");
          });
        } else d.series.forEach((s, si) => {
          const v = cur.ys[si][i], el2 = S.bars[si * d.n + i], x = x0 + si * (g.bw + 2);
          el2.setAttribute("d", v != null && v > 0 ? bar(x, g.base, Math.min(yOf(v), g.base - 1.5), g.bw, 4) : "");
          const pv = pvEls[si * d.n + i]; const p = cur.pv[si][i];
          if (pv) { if (p != null && p > 0 && !stackOn()) { pv.removeAttribute("display"); pv.setAttribute("x1", pf(x - 2)); pv.setAttribute("x2", pf(x + g.bw + 2)); pv.setAttribute("y1", pf(yOf(p))); pv.setAttribute("y2", pf(yOf(p))); } else pv.setAttribute("display", "none"); }
        });
      }
      yax.place(g, yOf); S.yOf = yOf;
      // value labels: the highlighted bar and the peak only (a single series)
      if (single && C.o.labelPeak !== false) {
        const s0 = d.series[0], pk = (() => { let b = -1; s0.values.forEach((v, i) => { if (v != null && v > 0 && (b < 0 || v > s0.values[b])) b = i; }); return b; })();
        const place = (t, i) => { if (i < 0 || !(s0.values[i] > 0) || g.slot < 6) { t.setAttribute("display", "none"); return; } const y = yOf(cur.ys[0][i] || 0); t.removeAttribute("display"); t.setAttribute("x", pf(g.xc(i))); t.setAttribute("y", pf(Math.max(10, y - 5))); setText(t, C.fmtTick()(s0.values[i])); };
        place(vals[0], hi >= 0 ? hi : pk); place(vals[1], hi >= 0 && pk !== hi ? pk : -1);
      } else vals.forEach(t => t.setAttribute("display", "none"));
    }
    function target(d, sc) { return { ys: d.series.map(s => s.values.map(v => v)), pv: d.series.map(s => (s.prev ? s.prev.slice() : new Array(d.n).fill(null))), max: sc.max }; }
    function build(d) {
      layers.bars.textContent = ""; layers.pv.textContent = ""; layers.slot.textContent = ""; layers.val.textContent = ""; S.bars = []; pvEls = []; slotEls = []; vals = [];
      colorsOf(d); const hi = S.hi = highlightIdx(d), single = d.series.length === 1 && !stackOn();
      for (let si = 0; si < d.series.length; si++) for (let i = 0; i < d.n; i++) {
        const p = mk(layers.bars, "path", { class: "efc-bar" + (single && i === hi ? " cur" : "") }); p.style.setProperty("--c", single ? "var(--efc-bar)" : d.series[si].css); S.bars.push(p);
        pvEls.push(d.series[si].prev && !stackOn() ? mk(layers.pv, "line", { class: "efc-pv", display: "none" }) : null);
      }
      for (let i = 0; i < d.n; i++) slotEls.push(mk(layers.slot, "rect", { class: "efc-slot" + (d.closed && d.closed[i] ? " cl" : "") }));
      vals = [mk(layers.val, "text", { class: "efc-val", "text-anchor": "middle", display: "none" }), mk(layers.val, "text", { class: "efc-val soft", "text-anchor": "middle", display: "none" })];
    }
    function layout(d, sc) {
      const g = S.g = geometry(d, sc.ticks), svg = S.svg; C.W = g.W; C.H = g.H;
      svg.setAttribute("width", g.W); svg.setAttribute("height", g.H); svg.setAttribute("viewBox", "0 0 " + g.W + " " + g.H);
      hit.setAttribute("x", g.l); hit.setAttribute("y", 0); hit.setAttribute("width", g.pw); hit.setAttribute("height", g.H);
      slotEls.forEach((r, i) => { r.setAttribute("x", pf(g.l + g.slot * i)); r.setAttribute("y", g.t); r.setAttribute("width", pf(g.slot)); r.setAttribute("height", g.ph); });
      yax.set(sc.ticks, C.fmtTick(), g); yax.place(g, v => g.base - g.ph * (v / (sc.max || 1)));
      xax.set(xTicks(d, g.slot, g.W < 520), g.xc, g, S.hi < 0 ? -1 : S.hi);
      layers.slot.parentNode.classList.toggle("efc-dimmed", false);
    }
    function sigOf(d) { return JSON.stringify([d.kind, d.xs, d.series.map(s => [s.key, s.values, s.prev, s.css]), S.hiRaw]); }
    function update(data, opts) {
      if (C.dead) return;
      if (intake(C, data, opts) === "wait") return;
      if (data === undefined && !S.d) return;
      const o = C.o; if (data !== undefined) S.raw = data;
      const d = shape(data === undefined ? S.raw : data, o, "bars"); d.hasData = hasPositive(d); C.hasData = d.hasData;
      const prevD = S.d, prevCur = S.cur; ensure();
      S.hiRaw = o.highlight != null ? String(o.highlight) : d.hi != null ? String(d.hi) : "";
      const sc = scale(d), sig = sigOf(d) + "|" + C.width() + "|" + (o.height || 0) + "|" + o.unit + "|" + (o.stack ? 1 : 0);
      if (data !== undefined && sig === S.sig) { C.setState(d.hasData ? "ready" : "empty"); if (S.hover != null) show(S.hover, S.hoverSrc); return; }
      S.sig = sig; S.d = d;
      C.setState(d.hasData ? "ready" : "empty");
      { const items = []; if (d.series.length > 1) d.series.forEach((x, i) => items.push({ label: x.label, css: colorFor(x.key, i, x.color), kind: "sq" })); if (!stackOn() && d.series.some(x => x.prev)) items.push({ label: o.prevLabel || "Previous period", kind: "tick" }); C.legend(d.hasData ? items : []); }
      const structural = !prevD || prevD.n !== d.n || prevD.series.length !== d.series.length || prevD.series.some((s, i) => s.key !== d.series[i].key) || (prevD.series[0] && !!prevD.series[0].prev) !== (d.series[0] && !!d.series[0].prev) || !S.bars.length;
      let keepIdx = null;
      if (S.hover != null && prevD) { const hx = prevD.xs[S.hover], j = d.xs.findIndex(x => String(x) === String(hx)); keepIdx = j >= 0 ? j : Math.min(S.hover, d.n - 1); }
      if (structural) build(d); else { slotEls.forEach((r, i) => r.setAttribute("class", "efc-slot" + (d.closed && d.closed[i] ? " cl" : ""))); S.hi = highlightIdx(d); S.bars.forEach((p, k) => p.setAttribute("class", "efc-bar" + (d.series.length === 1 && !stackOn() && k % d.n === S.hi ? " cur" : ""))); }
      layout(d, sc);
      const to = target(d, sc);
      if (S.stopM) { S.stopM(); S.stopM = null; }
      if (!d.hasData) { S.cur = to; paint(to); C.drawn = true; hide(); return; }
      let from;
      const reuse = prevCur && !structural && prevD && prevD.hasData;
      if (reuse) from = prevCur;
      else {
        // first draw, a new range or new series: grow from the baseline; a range change starts from what was drawn, stretched
        from = { ys: to.ys.map(a => a.map(v => (v == null ? null : 0))), pv: to.pv, max: to.max };
        if (prevCur && prevD && prevD.hasData && prevD.series.length === d.series.length) { from = { ys: to.ys.map((a, si) => a.map((v, i) => { const m = prevD.n, j = m <= 1 ? 0 : Math.round(i / Math.max(1, d.n - 1) * (m - 1)); return v == null ? null : prevCur.ys[si][j]; })), pv: to.pv, max: prevCur.max }; }
      }
      const delayed = !reuse && !prevCur; C.drawn = true;
      const ms = reuse ? 420 : delayed ? 650 : 560;
      S.stopM = C.tween(ms, k => {
        S.cur = { ys: to.ys.map((a, si) => a.map((b, i) => { const f0 = from.ys[si][i], kk = delayed ? clamp(k * 1.4 - (i / Math.max(1, d.n)) * .4, 0, 1) : k; return b == null ? null : (f0 == null ? 0 : f0) + (b - (f0 == null ? 0 : f0)) * (delayed ? ease(kk) : kk); })), pv: to.pv, max: from.max + (to.max - from.max) * k };
        paint(S.cur);
      }, () => { S.cur = to; paint(to); S.stopM = null; });
      if (keepIdx != null) show(keepIdx, S.hoverSrc || "ptr");
    }
    function pointOf(i) {
      const d = S.d, values = {}, prev = {}, ser = [];
      d.series.forEach(s => { const v = s.values[i], p = s.prev ? s.prev[i] : null; values[s.key] = v; prev[s.key] = p; ser.push({ key: s.key, label: s.label, value: v, prev: p, delta: delta(v, p) }); });
      const p = { index: i, x: d.xs[i], label: defaultLabel(d, C.o, i), values, prev, meta: d.meta ? d.meta[i] : null, series: ser };
      if (typeof C.o.label === "function") { const l = C.o.label(p); if (l != null) p.label = String(l); }
      return p;
    }
    const tone = dl => { const b = C.o.better === undefined ? "up" : C.o.better; if (!dl.dir || dl.dir === "flat" || b == null) return ""; return (dl.dir === "up") === (b === "up") ? "good" : "bad"; };
    function cardOf(i) {
      const d = S.d, o = C.o, p = pointOf(i), fl = C.fmtLong(), rows = [], card = { title: p.label, rows };
      if (d.series.length === 1 && !stackOn()) {
        const s = d.series[0], v = s.values[i];
        if (v == null) card.big = { text: d.closed && d.closed[i] ? (o.closedText || "Closed") : (o.nullText || "Nothing logged") }; else card.big = splitBig(fl(v));
        if (s.prev && v != null) { const pv = s.prev[i]; if (pv != null) { const dl = delta(v, pv); rows.push({ k: o.prevLabel || "Previous period", v: fl(pv), muted: true, dashed: true, delta: dl.text ? { text: dl.text, tone: tone(dl) } : null }); } else rows.push({ k: o.prevLabel || "Previous period", v: "—", muted: true, dashed: true }); }
        if (s.def && !o.def) card.foot = s.def;
      } else {
        let tot = 0, any = false;
        if (!d.series.some(s => s.values[i] != null)) { card.big = { text: d.closed && d.closed[i] ? (o.closedText || "Closed") : (o.nullText || "Nothing logged") }; if (o.def) card.foot = o.def; return card; }
        d.series.forEach(s => { const v = s.values[i]; if (v != null) { tot += v; any = true; } rows.push({ k: s.label, v: v == null ? "—" : fl(v), color: s.css, sq: true, delta: (() => { if (!s.prev || s.prev[i] == null || v == null) return null; const dl = delta(v, s.prev[i]); return dl.text ? { text: dl.text, tone: tone(dl) } : null; })() }); });
        if (stackOn() && any) rows.push({ k: "Total", v: fl(tot), strong: true });
      }
      if (typeof o.tipRows === "function") for (const r of o.tipRows(p) || []) rows.push(Array.isArray(r) ? { k: String(r[0]), v: String(r[1]) } : { k: String(r.k), v: String(r.v), muted: r.muted, delta: r.tone ? { text: r.text || "", tone: r.tone } : null });
      if (o.def) card.foot = o.def;
      return card;
    }
    function topOf(i) { const d = S.d, g = S.g, cur = S.cur || { ys: d.series.map(s => s.values), max: 1 }; let v = 0; if (stackOn()) d.series.forEach((s, si) => { v += cur.ys[si][i] || 0; }); else d.series.forEach((s, si) => { v = Math.max(v, cur.ys[si][i] || 0, s.prev && s.prev[i] ? s.prev[i] : 0); }); return g.base - g.ph * (v / ((S.yOf ? 1 : 1) * cur.max || 1)); }
    function show(i, src) {
      const d = S.d; if (!d || !d.n || !d.hasData) return;
      i = clamp(Math.round(i), 0, d.n - 1);
      S.hover = i; S.hoverSrc = src || "ptr"; C.idx = i;
      slotEls.forEach((r, k) => r.classList.toggle("on", k === i));
      S.svg.classList.add("efc-dimmed");
      S.bars.forEach((p, k) => p.classList.toggle("on", k % d.n === i)); pvEls.forEach((p, k) => { if (p) p.classList.toggle("on", k % d.n === i); });
      C.showTip(cardOf(i), S.g.xc(i), topOf(i), "above", S.g.gw / 2);
    }
    function hide() { S.hover = null; S.hoverSrc = null; C.idx = -1; if (S.svg) S.svg.classList.remove("efc-dimmed"); slotEls.forEach(r => r.classList.remove("on")); S.bars.forEach(p => p.classList.remove("on")); pvEls.forEach(p => p && p.classList.remove("on")); C.hideTip(); }
    function indexAt(e) { const r = S.svg.getBoundingClientRect(), x = (e.clientX - r.left) * (S.g.W / (r.width || S.g.W)); return clamp(Math.floor((x - S.g.l) / S.g.slot), 0, S.d.n - 1); }
    function wire() {
      const svg = S.svg; let down = null;
      C.on(hit, "pointermove", e => { if (!S.d || !S.d.hasData) return; if (e.pointerType === "touch") { if (down) show(indexAt(e), "touch"); return; } S.ptr = true; show(indexAt(e), "ptr"); });
      C.on(hit, "pointerdown", e => { if (!S.d || !S.d.hasData) return; down = { i: indexAt(e), touch: e.pointerType === "touch" }; if (down.touch) { show(down.i, "touch"); try { hit.setPointerCapture(e.pointerId); } catch (_) {} } });
      C.on(hit, "pointerup", e => { const dn = down; down = null; if (!dn || !S.d || !S.d.hasData) return; const i = indexAt(e); if (typeof C.o.onPoint === "function" && (!dn.touch || S.tapped === i)) C.o.onPoint(pointOf(i)); S.tapped = dn.touch ? i : null; });
      C.on(hit, "pointercancel", () => { down = null; });
      C.on(svg, "pointerleave", e => { S.ptr = false; if (e.pointerType !== "touch") hide(); });
      C.on(doc, "pointerdown", e => { if (S.hover != null && !C.root.contains(e.target)) hide(); });
      C.on(svg, "focus", () => { if (S.ptr || !S.d || !S.d.hasData) return; if (S.hover == null) show(S.hi >= 0 ? S.hi : S.d.n - 1, "key"); });
      C.on(svg, "blur", () => { if (!S.ptr) hide(); });
      C.on(svg, "keydown", e => {
        const d = S.d; if (!d || !d.hasData) return; const k = e.key; if (!["ArrowLeft", "ArrowRight", "Home", "End", "Escape", "Enter", " "].includes(k)) return;
        e.preventDefault(); if (k === "Escape") return hide();
        const cur = S.hover == null ? d.n - 1 : S.hover;
        if (k === "Enter" || k === " ") { if (typeof C.o.onPoint === "function") C.o.onPoint(pointOf(cur)); return; }
        show(k === "Home" ? 0 : k === "End" ? d.n - 1 : cur + (k === "ArrowRight" ? (e.shiftKey ? 7 : 1) : -(e.shiftKey ? 7 : 1)), "key");
      });
    }
    C.watch(() => { if (S.d) { S.sig = ""; const keep = S.hover; update(undefined); if (keep != null && S.hover == null) show(keep, S.hoverSrc || "ptr"); } });
    const api = publish(C, { update, destroy: () => C.destroy(), show: i => show(i, "api"), hide, setLoading: (on, label) => C.setLoading(on, label), get el() { return C.root; } });
    if (C.o.data) update(C.o.data); else if (C.o.loading) C.setLoading(true); else C.setState("loading");
    return api;
  }

  /* ═════════════ hour heatmap: 24 hours × days ═════════════ */
  function heatChart(el, o0) {
    const C = core(el, "heat", o0), S = { rows: [], svg: null, cells: [], hover: null, sig: "", g: null, max: 0 };
    let hit;
    function rowsOf(data) {
      data = data || {};
      let rows = Array.isArray(data.rows) ? data.rows : Array.isArray(data.days) ? data.days.map(x => ({ key: x.day, label: x.label || (isDay(x.day) ? dayTxt(x.day, "wd") : x.day), values: x.hours || x.values })) : Array.isArray(data.values) ? [{ key: "all", label: C.o.rowLabelAll || "", values: data.values }] : [];
      const lo = C.o.hourFrom != null ? C.o.hourFrom : 0, hi = C.o.hourTo != null ? C.o.hourTo : 23;
      rows = rows.map((r, i) => ({ key: r.key != null ? String(r.key) : "r" + i, label: r.label != null ? String(r.label) : "", values: Array.from({ length: 24 }, (_, k) => N((r.values || [])[k])), meta: r }));
      return { rows, lo: clamp(lo, 0, 23), hi: clamp(hi, 0, 23), single: rows.length === 1 };
    }
    function geometry(m) {
      const W = C.width(), phone = W < 520, ncol = m.hi - m.lo + 1, labW = m.single && !m.rows[0].label ? 0 : (phone ? 40 : 56);
      const gap = phone ? 1.5 : 2, cw = (W - labW - 2) / ncol, ch = m.single ? clamp(cw * (phone ? 1.4 : 1.2), 26, 44) : clamp(cw * (phone ? 1.5 : 1), phone ? 20 : 15, 26);
      const top = 4, axisH = 20, legH = 26, H = Math.round(top + m.rows.length * ch + axisH + legH);
      return { W, H, labW, cw, ch, gap, top, axisH, legH, ncol, phone };
    }
    const shade = (v, max) => (v == null || v <= 0 ? 0 : .16 + .84 * Math.pow(v / (max || 1), .8));
    function update(data, opts) {
      if (C.dead) return;
      if (intake(C, data, opts) === "wait") return;
      if (data === undefined && !S.m) return;
      const o = C.o; if (data !== undefined) S.raw = data;
      const m = rowsOf(data === undefined ? S.raw : data); S.m = m;
      let max = 0, any = false; for (const r of m.rows) for (let k = m.lo; k <= m.hi; k++) { const v = r.values[k]; if (v != null) { if (v > max) max = v; if (v > 0) any = true; } }
      C.hasData = any; S.max = max;
      const g = S.g = geometry(m); C.W = g.W; C.H = g.H; const sig = JSON.stringify([m.rows.map(r => [r.key, r.label, r.values]), m.lo, m.hi, g.W, o.nowHour, o.unit]);
      if (data !== undefined && sig === S.sig) { C.setState(any ? "ready" : "empty"); if (S.hover) show(S.hover.r, S.hover.c, S.hoverSrc); return; }
      S.sig = sig;
      C.setState(any ? "ready" : "empty");
      const keep = S.hover ? { r: S.hover.r, c: S.hover.c } : null, sz = [g.W, g.H, m.rows.length, m.lo, m.hi].join();
      let rebuilt = false;
      if (!S.svg || S.sizeSig !== sz) { buildSvg(m, g); S.sizeSig = sz; rebuilt = true; }
      if (rebuilt && !C.drawn && !still()) { S.cells.forEach(c => { c.style.opacity = "0"; }); const mm0 = m, gg = g; raf(() => raf(() => { if (!C.dead) paintCells(mm0, gg, true); })); }
      else paintCells(m, g, true);
      C.drawn = true;
      if (keep && keep.r < m.rows.length) show(keep.r, keep.c, S.hoverSrc); else if (S.hover) hide();
    }
    function buildSvg(m, g) {
      if (S.svg) S.svg.remove();
      const svg = S.svg = mk(null, "svg", { class: "efc-svg", role: "group", "aria-roledescription": "chart", tabindex: "0", width: g.W, height: g.H, viewBox: "0 0 " + g.W + " " + g.H, "aria-describedby": C.tip.id });
      C.body.appendChild(svg); S.cells = [];
      const x0 = g.labW;
      m.rows.forEach((r, ri) => { const y = g.top + ri * g.ch; if (g.labW) { const t = mk(svg, "text", { class: "efc-tick", x: g.labW - 8, y: y + g.ch / 2 + 3.5, "text-anchor": "end" }); t.textContent = r.label; }
        for (let c = m.lo; c <= m.hi; c++) { const x = x0 + (c - m.lo) * g.cw; const rc = mk(svg, "rect", { class: "efc-cell nul", x: pf(x + g.gap / 2), y: pf(y + g.gap / 2), width: pf(Math.max(1, g.cw - g.gap)), height: pf(Math.max(1, g.ch - g.gap)), rx: Math.min(4, g.cw / 3) }); S.cells.push(rc); } });
      const ay = g.top + m.rows.length * g.ch + 13, step = g.W < 520 ? 6 : g.W < 900 ? 3 : 2;
      for (let c = m.lo; c <= m.hi; c++) if ((c - m.lo) % step === 0 || c === m.hi && false) { const t = mk(svg, "text", { class: "efc-xl" + (c === C.o.nowHour ? " on" : ""), x: pf(x0 + (c - m.lo + .5) * g.cw), y: ay, "text-anchor": "middle" }); t.textContent = hourShort(c); }
      // the scale: fewer … more
      const ly = g.top + m.rows.length * g.ch + g.axisH + 8, sc = mk(svg, "g", { class: "efc-scale" }); const lbl = mk(svg, "text", { class: "efc-lg", x: g.W, y: ly + 8, "text-anchor": "end" }); lbl.textContent = "More";
      const sw = 5, sx = g.W - 36 - sw * 14; for (let k = 0; k < sw; k++) { const r = mk(sc, "rect", { x: sx + k * 14, y: ly, width: 11, height: 10, rx: 2 }); r.setAttribute("opacity", pf(.16 + .84 * k / (sw - 1))); }
      const l2 = mk(svg, "text", { class: "efc-lg", x: sx - 6, y: ly + 8, "text-anchor": "end" }); l2.textContent = "Fewer";
      hit = mk(svg, "rect", { class: "efc-hit can", x: x0, y: g.top, width: g.W - x0, height: m.rows.length * g.ch, fill: "transparent" });
      wire(m, g);
    }
    function paintCells(m, g, anim) {
      let k = 0; const reduce = still();
      m.rows.forEach((r, ri) => { for (let c = m.lo; c <= m.hi; c++, k++) {
        const v = r.values[c], e = S.cells[k]; if (!e) continue;
        const cls = "efc-cell" + (v == null ? " nul" : v <= 0 ? " z" : "") + (c === C.o.nowHour && ri === 0 ? " now" : "") + (S.hover && S.hover.r === ri && S.hover.c === c ? " hot" : "");
        e.setAttribute("class", cls);
        const op = v == null ? .45 : v <= 0 ? 1 : shade(v, S.max);
        if (anim && !reduce) e.style.transitionDelay = Math.min(260, (c - m.lo) * 7) + "ms"; else e.style.transitionDelay = "";
        e.style.opacity = String(op);
      } });
    }
    function cellInfo(r, c) { const m = S.m, row = m.rows[r], total = row.values.reduce((a, v) => a + (v || 0), 0), v = row.values[c]; return { row: row.meta, rowKey: row.key, rowLabel: row.label, hour: c, value: v, share: total > 0 && v != null ? v / total * 100 : null, rowTotal: total }; }
    function show(r, c, src) {
      const m = S.m, g = S.g; if (!m || !m.rows.length || !C.hasData) return;
      r = clamp(r, 0, m.rows.length - 1); c = clamp(c, m.lo, m.hi);
      const prev = S.hover; S.hover = { r, c }; S.hoverSrc = src || "ptr"; C.idx = r * 24 + c;
      if (prev) { const pk = S.cells[(prev.r) * (m.hi - m.lo + 1) + (prev.c - m.lo)]; if (pk) pk.classList.remove("hot"); }
      const e = S.cells[r * (m.hi - m.lo + 1) + (c - m.lo)]; if (e) e.classList.add("hot");
      const info = cellInfo(r, c), fl = C.fmtLong(), rows = [], row = m.rows[r];
      const title = typeof C.o.label === "function" ? String(C.o.label(info)) : (row.label ? row.label + " · " : "") + hourTxt(c) + " – " + hourTxt(c + 1);
      const card = { title, rows };
      if (info.value == null) card.big = { text: C.o.nullText || "Nothing logged" }; else card.big = splitBig(fl(info.value));
      if (info.share != null && info.rowTotal > 0 && m.rows.length >= 1) rows.push({ k: m.single ? "Share of the total" : "Share of the day", v: percent(info.share), muted: true });
      if (typeof C.o.tipRows === "function") for (const x of C.o.tipRows(info) || []) rows.push(Array.isArray(x) ? { k: String(x[0]), v: String(x[1]) } : { k: String(x.k), v: String(x.v), muted: x.muted });
      if (C.o.def) card.foot = C.o.def;
      C.showTip(card, g.labW + (c - m.lo + .5) * g.cw, g.top + r * g.ch, "above", g.cw / 2);
    }
    function hide() { if (S.hover && S.m) { const e = S.cells[S.hover.r * (S.m.hi - S.m.lo + 1) + (S.hover.c - S.m.lo)]; if (e) e.classList.remove("hot"); } S.hover = null; C.idx = -1; C.hideTip(); }
    function wire(m, g) {
      const svg = S.svg; let down = false;
      const at = e => { const r = svg.getBoundingClientRect(), sx = g.W / (r.width || g.W), x = (e.clientX - r.left) * sx, y = (e.clientY - r.top) * sx; return { r: clamp(Math.floor((y - g.top) / g.ch), 0, m.rows.length - 1), c: clamp(m.lo + Math.floor((x - g.labW) / g.cw), m.lo, m.hi) }; };
      C.on(hit, "pointermove", e => { if (!C.hasData) return; if (e.pointerType === "touch" && !down) return; S.ptr = true; const p = at(e); show(p.r, p.c, e.pointerType === "touch" ? "touch" : "ptr"); });
      C.on(hit, "pointerdown", e => { if (!C.hasData) return; down = true; const p = at(e); show(p.r, p.c, e.pointerType === "touch" ? "touch" : "ptr"); if (e.pointerType === "touch") { try { hit.setPointerCapture(e.pointerId); } catch (_) {} } });
      C.on(hit, "pointerup", () => { down = false; });
      C.on(svg, "pointerleave", e => { S.ptr = false; if (e.pointerType !== "touch") hide(); });
      C.on(doc, "pointerdown", e => { if (S.hover && !C.root.contains(e.target)) hide(); });
      C.on(svg, "focus", () => { if (S.ptr || !C.hasData) return; if (!S.hover) { let best = { r: 0, c: S.m.lo, v: -1 }; S.m.rows.forEach((row, ri) => { for (let c = S.m.lo; c <= S.m.hi; c++) if ((row.values[c] || 0) > best.v) best = { r: ri, c, v: row.values[c] || 0 }; }); show(best.r, best.c, "key"); } });
      C.on(svg, "blur", () => { if (!S.ptr) hide(); });
      C.on(svg, "keydown", e => {
        if (!C.hasData) return; const k = e.key; if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Home", "End", "Escape"].includes(k)) return; e.preventDefault(); if (k === "Escape") return hide();
        const cur = S.hover || { r: 0, c: S.m.lo };
        show(k === "ArrowUp" ? cur.r - 1 : k === "ArrowDown" ? cur.r + 1 : cur.r, k === "ArrowLeft" ? cur.c - 1 : k === "ArrowRight" ? cur.c + 1 : k === "Home" ? S.m.lo : k === "End" ? S.m.hi : cur.c, "key");
      });
    }
    C.watch(() => { if (S.m) { S.sig = ""; update(undefined); } });
    const api = publish(C, { update, destroy: () => C.destroy(), show: (r, c) => show(r || 0, c == null ? 0 : c, "api"), hide, setLoading: (on, label) => C.setLoading(on, label), get el() { return C.root; } });
    if (C.o.data) update(C.o.data); else if (C.o.loading) C.setLoading(true); else C.setState("loading");
    return api;
  }

  /* ═════════════ calendar: days as worked / partial / off / future / closed ═════════════ */
  const STATE_TXT = { worked: "Worked", partial: "Short day", off: "Day off", future: "Not yet", closed: "Closed", before: "Before tracking began", pending: "Not signed in yet" };
  const STATE_DEF = { worked: "Signed in.", partial: "Signed in for much less than the rest of the team that day.", off: "A team working day with no sign-in.", future: "A day still to come.", closed: "The team did not work (weekend or holiday); never counted.", before: "Earlier than this person's first record; never counted.", pending: "Today, not signed in yet; counted only when the day is over." };
  function calChart(el, o0) {
    const C = core(el, "calendar", o0), S = { svg: null, days: new Map(), list: [], cells: [], hover: null, sig: "", g: null, order: [], first: true };
    function norm(data) {
      data = data || {}; const list = (Array.isArray(data.days) ? data.days : Array.isArray(data.calendar) ? data.calendar : []).filter(x => x && isDay(x.day)).map(x => Object.assign({}, x, { state: x.state || "worked" })).sort((a, b) => (a.day < b.day ? -1 : 1));
      return { list, today: data.today || C.o.today || "" };
    }
    function layoutKind(n, W) { const l = C.o.layout || "auto"; if (l !== "auto") return l; return n <= 35 ? "month" : "months"; }
    function geometry(list, kind, W) {
      const first = list[0].day, last = list[list.length - 1].day, mons = []; const phone = W < 520;
      // months covered (calendar months from first to last)
      let y = +first.slice(0, 4), m = mm(first); const ly = +last.slice(0, 4), lm = mm(last);
      while (y < ly || (y === ly && m <= lm)) { mons.push({ y, m, key: y + "-" + String(m).padStart(2, "0") }); m++; if (m > 12) { m = 1; y++; } if (mons.length > 40) break; }
      const gap = phone ? 3 : 4, head = 22;
      if (kind === "month" && mons.length === 1) {
        const cw = (W - gap * 6) / 7, ch = clamp(cw * .62, 34, 54), wd = 34;
        const wks = weeksOf(mons[0]); const H = wd + wks * (ch + gap) + 2;
        return { kind: "month", W, H, mons: [Object.assign(mons[0], { ox: 0, oy: 0, cw, ch, gap, wd, wks })], cw, ch, gap, legend: true };
      }
      const minW = phone ? 150 : 176, cols = clamp(Math.floor((W + 14) / (minW + 14)), 1, 4), mw = (W - (cols - 1) * 14) / cols, cw = Math.min(30, Math.floor((mw - 6 * 3) / 7)), ch = cw, g2 = 3;
      let yy = 0; const rowsH = []; mons.forEach((mo, k) => { const wks = weeksOf(mo), bh = head + 12 + wks * (ch + g2); const r = Math.floor(k / cols); rowsH[r] = Math.max(rowsH[r] || 0, bh); });
      mons.forEach((mo, k) => { const r = Math.floor(k / cols), c = k % cols; let y0 = 0; for (let q = 0; q < r; q++) y0 += rowsH[q] + 12; Object.assign(mo, { ox: c * (mw + 14), oy: y0, cw, ch, gap: g2, wd: head + 10, wks: weeksOf(mo), head, mw }); });
      yy = rowsH.reduce((a, b) => a + b + 12, 0) - 12;
      return { kind: "months", W, H: Math.max(yy, 10), mons, cw, ch, gap: g2, legend: true, cols };
    }
    function weeksOf(mo) { const first = new Date(Date.UTC(mo.y, mo.m - 1, 1)).getUTCDay(), off = (first + 6) % 7, dim = new Date(Date.UTC(mo.y, mo.m, 0)).getUTCDate(); return Math.ceil((off + dim) / 7); }
    function update(data, opts) {
      if (C.dead) return;
      if (intake(C, data, opts) === "wait") return;
      if (data === undefined && !S.n) return;
      const o = C.o; if (data !== undefined) S.raw = data;
      const n = norm(data === undefined ? S.raw : data); S.n = n; C.hasData = n.list.length > 0;
      const sig = JSON.stringify([n.list.map(x => [x.day, x.state, x.signedMs, x.parts, x.orders, x.firstIn, x.lastOut, x.note]), n.today, o.selected || "", o.layout || "", C.width()]);
      if (data !== undefined && sig === S.sig) { C.setState(C.hasData ? "ready" : "empty"); if (S.hover) show(S.hover, S.hoverSrc); return; }
      S.sig = sig; C.setState(C.hasData ? "ready" : "empty");
      if (S.legend) S.legend.hidden = !C.hasData;
      if (!C.hasData) { if (S.svg) { S.svg.remove(); S.svg = null; } S.layoutKey = ""; C.body.style.minHeight = "120px"; C.drawn = true; return; }
      C.body.style.minHeight = "";
      const keep = S.hover; draw(n); C.drawn = true; if (keep && S.days.has(keep)) show(keep, S.hoverSrc); else if (keep) hide();
    }
    function draw(n) {
      const W = C.width(), kind = layoutKind(n.list.length, W), g = S.g = geometry(n.list, kind, W); C.W = g.W; C.H = g.H;
      if (S.svg) S.svg.remove();
      const svg = S.svg = mk(null, "svg", { class: "efc-svg", role: "group", "aria-roledescription": "calendar", tabindex: "0", width: g.W, height: g.H, viewBox: "0 0 " + g.W + " " + g.H, "aria-describedby": C.tip.id });
      C.body.appendChild(svg);
      S.days = new Map(n.list.map(x => [x.day, x])); S.cells = new Map(); S.order = n.list.map(x => x.day);
      const reduce = still(), lk = JSON.stringify([n.list[0].day, n.list[n.list.length - 1].day, n.list.length, g.kind, g.W]), animate = !reduce && lk !== S.layoutKey; S.layoutKey = lk;
      let idx = 0;
      g.mons.forEach(mo => {
        const title = mk(svg, "text", { class: "efc-mt", x: mo.ox + 2, y: mo.oy + 13, "text-anchor": "start" }); title.textContent = dayTxt(mo.key + "-01", "monthLong");
        const top = mo.oy + mo.wd;
        const cw = g.kind === "month" ? mo.cw : g.cw, ch = g.kind === "month" ? mo.ch : g.ch, gp = g.kind === "month" ? mo.gap : g.gap;
        const wk = ["M", "T", "W", "T", "F", "S", "S"], names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
        for (let c = 0; c < 7; c++) { const t = mk(svg, "text", { class: "efc-wd", x: mo.ox + c * (cw + gp) + cw / 2, y: top - 4, "text-anchor": "middle" }); t.textContent = g.kind === "month" ? names[c] : wk[c]; }
        const dim = new Date(Date.UTC(mo.y, mo.m, 0)).getUTCDate(), off = (new Date(Date.UTC(mo.y, mo.m - 1, 1)).getUTCDay() + 6) % 7;
        for (let d = 1; d <= dim; d++) {
          const day = mo.key + "-" + String(d).padStart(2, "0"), item = S.days.get(day); if (!item) continue;
          const pos = off + d - 1, c = pos % 7, r = Math.floor(pos / 7), x = mo.ox + c * (cw + gp), y = top + r * (ch + gp);
          const st = item.state === "pending" || item.state === "worked" || item.state === "partial" || item.state === "off" || item.state === "future" || item.state === "closed" || item.state === "before" ? item.state : "worked";
          const can = typeof C.o.clickable === "function" ? !!C.o.clickable(item) : st !== "future" && st !== "before";
          const grp2 = mk(svg, "g", { class: "efc-day " + st + (can ? " can" : "") + (n.today === day ? " today" : "") + (C.o.selected === day ? " sel" : ""), "data-day": day, "data-state": st });
          if (animate) { grp2.classList.add("in"); grp2.style.animationDelay = Math.min(420, idx * 7) + "ms"; }
          mk(grp2, "rect", { class: "bg", x, y, width: cw, height: ch, rx: Math.min(g.kind === "month" ? 8 : 6, cw / 3) });
          if (n.today === day) mk(grp2, "rect", { class: "ring", x: x - 2, y: y - 2, width: cw + 4, height: ch + 4, rx: Math.min(9, cw / 3 + 2) });
          if (cw >= 20) { const t = mk(grp2, "text", { class: "n", x: g.kind === "month" ? x + 7 : x + cw / 2, y: g.kind === "month" ? y + 14 : y + ch / 2 + 3.5, "text-anchor": g.kind === "month" ? "start" : "middle" }); t.textContent = String(d); }
          if (g.kind === "month" && cw >= 62 && item.signedMs != null && (st === "worked" || st === "partial")) { const t2 = mk(grp2, "text", { class: "n", x: x + 7, y: y + ch - 8, "text-anchor": "start" }); t2.setAttribute("style", "font-weight:500;font-size:10px"); t2.textContent = hm(item.signedMs); }
          mk(grp2, "rect", { class: "hitc", x: x - gp / 2, y: y - gp / 2, width: cw + gp, height: ch + gp, fill: "transparent" });
          S.cells.set(day, { g: grp2, x, y, cw, ch, item }); idx++;
        }
      });
      S.first = false;
      wire(svg);
    }
    function infoCard(day) {
      const it = S.days.get(day), o = C.o, rows = []; if (!it) return null;
      const stTxt = it.stateLabel || STATE_TXT[it.state] || String(it.state), card = { title: dayTxt(day), big: { text: stTxt }, rows };
      if (it.signedMs != null && (it.state === "worked" || it.state === "partial")) rows.push({ k: "Signed in", v: hm(it.signedMs) });
      if (it.firstIn != null) rows.push({ k: "First in", v: typeof it.firstIn === "number" && it.firstIn > 1440 ? new Date(it.firstIn).toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZone: "America/New_York" }) : clockTxt(it.firstIn), muted: true });
      if (it.lastOut != null) rows.push({ k: "Last out", v: typeof it.lastOut === "number" && it.lastOut > 1440 ? new Date(it.lastOut).toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZone: "America/New_York" }) : clockTxt(it.lastOut), muted: true });
      if (it.parts != null && (it.state === "worked" || it.state === "partial")) rows.push({ k: "Pieces", v: fmt.int(it.parts) });
      if (it.orders != null && (it.state === "worked" || it.state === "partial")) rows.push({ k: "Orders", v: fmt.int(it.orders) });
      if (it.late) rows.push({ k: "Started late", v: "yes", muted: true });
      if (it.note) rows.push({ k: "Note", v: String(it.note), muted: true });
      if (typeof o.tipRows === "function") for (const x of o.tipRows(Object.assign({ day }, it)) || []) rows.push(Array.isArray(x) ? { k: String(x[0]), v: String(x[1]) } : { k: String(x.k), v: String(x.v), muted: x.muted });
      card.foot = o.def || STATE_DEF[it.state] || "";
      if (typeof o.label === "function") { const l = o.label(Object.assign({ index: S.order.indexOf(day), x: day, label: card.title }, it)); if (l != null) card.title = String(l); }
      return card;
    }
    function show(day, src) {
      const cell = S.cells.get(day); if (!cell) return;
      if (S.hover && S.hover !== day) { const p = S.cells.get(S.hover); if (p) p.g.classList.remove("hot"); }
      S.hover = day; S.hoverSrc = src || "ptr"; C.idx = S.order.indexOf(day); cell.g.classList.add("hot");
      C.showTip(infoCard(day), cell.x + cell.cw / 2, cell.y, "above", cell.cw / 2);
    }
    function hide() { if (S.hover) { const p = S.cells.get(S.hover); if (p) p.g.classList.remove("hot"); } S.hover = null; C.idx = -1; C.hideTip(); }
    function activate(day) { const it = S.days.get(day); if (!it) return; const can = typeof C.o.clickable === "function" ? !!C.o.clickable(it) : it.state !== "future" && it.state !== "before"; if (can && typeof C.o.onDay === "function") C.o.onDay(day, it); }
    function dayAt(e) { const t = e.target && e.target.closest ? e.target.closest(".efc-day") : null; return t ? t.getAttribute("data-day") : null; }
    function wire(svg) {
      let tapped = null;
      C.on(svg, "pointermove", e => { if (e.pointerType === "touch") return; S.ptr = true; const d = dayAt(e); if (d) { if (d !== S.hover) show(d, "ptr"); } else if (S.hover) hide(); });
      C.on(svg, "pointerleave", e => { S.ptr = false; if (e.pointerType !== "touch") hide(); });
      C.on(svg, "click", e => { const d = dayAt(e); if (!d) return; const touch = e.pointerType === "touch" || (e.sourceCapabilities && e.sourceCapabilities.firesTouchEvents); if (touch && tapped !== d) { tapped = d; show(d, "touch"); return; } tapped = d; show(d, "ptr"); activate(d); });
      C.on(svg, "pointerdown", e => { if (e.pointerType === "touch") { const d = dayAt(e); if (d) show(d, "touch"); } });
      C.on(doc, "pointerdown", e => { if (S.hover && !C.root.contains(e.target)) { hide(); tapped = null; } });
      C.on(svg, "focus", () => { if (S.ptr) return; if (!S.hover) { const t = S.n.today && S.days.has(S.n.today) ? S.n.today : S.order[S.order.length - 1]; show(t, "key"); } });
      C.on(svg, "blur", () => { if (!S.ptr) hide(); });
      C.on(svg, "keydown", e => {
        const k = e.key; if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Home", "End", "Enter", " ", "Escape"].includes(k)) return; e.preventDefault(); if (k === "Escape") return hide();
        const cur = S.hover || S.order[S.order.length - 1]; if (k === "Enter" || k === " ") return activate(cur);
        let i = S.order.indexOf(cur); const ds = k === "ArrowLeft" ? -1 : k === "ArrowRight" ? 1 : k === "ArrowUp" ? -7 : k === "ArrowDown" ? 7 : 0;
        if (k === "Home") i = 0; else if (k === "End") i = S.order.length - 1; else i = clamp(i + ds, 0, S.order.length - 1); show(S.order[i], "key");
      });
    }
    C.watch(() => { if (S.n) { S.sig = ""; update(undefined); } });
    const api = publish(C, { update, destroy: () => C.destroy(), show: day => show(day, "api"), hide, setLoading: (on, label) => C.setLoading(on, label), get el() { return C.root; } });
    // the legend sits under the drawing, built once
    const lg = h("div", "efc-legend"); 
    for (const [cls, t] of [["", "Worked"], ["gold", "Short day"], ["off", "Day off"], ["pen", "Today, not in yet"], ["fut", "Not yet"], ["clo", "Closed"]]) { const i = h("span", "efc-lgi"); const sw = h("i", "efc-sw " + cls); sw.style.setProperty("--c", cls === "gold" ? "var(--efc-gold2)" : "var(--efc-sage)"); i.appendChild(sw); i.appendChild(doc.createTextNode(t)); lg.appendChild(i); }
    C.root.appendChild(lg); S.legend = lg;
    if (C.o.data) update(C.o.data); else if (C.o.loading) C.setLoading(true); else C.setState("loading");
    return api;
  }

  /* ═════════════ donut: part of a whole (the station mix) ═════════════ */
  function donutChart(el, o0) {
    const C = core(el, "donut", o0), S = { d: null, cur: null, svg: null, arcs: new Map(), rows: new Map(), hover: null, sig: "", stopM: null, ptr: false, size: 0 };
    let gArcs, cv, cl;
    const list = h("div", "efc-dlist");
    C.root.appendChild(list);
    const sizeOf = () => clamp(Math.min(C.width(), C.o.size || 190), 120, 220);
    const pctTxt = x => percent(x, x < 10 && x % 1 ? 1 : 0);
    function shapeD(data) {
      data = data || {};
      let sl = (Array.isArray(data.slices) ? data.slices : []).map((x, i) => ({ key: x.key != null ? String(x.key) : "s" + i, label: x.label || x.key || "Item " + (i + 1), value: Math.max(0, N(x.value) || 0), color: x.color, meta: x.meta, def: x.def })).filter(x => x.value > 0);
      if (C.o.sort !== false) sl.sort((a, b) => b.value - a.value);
      const max = C.o.max || 7;
      if (sl.length > max) { const tail = sl.slice(max - 1); sl = sl.slice(0, max - 1); sl.push({ key: "other", label: C.o.otherLabel || "Other", value: tail.reduce((a, x) => a + x.value, 0), meta: tail }); }
      const total = sl.reduce((a, x) => a + x.value, 0);
      sl.forEach((x, i) => { x.css = colorFor(x.key, i, x.color); x.share = total > 0 ? x.value / total * 100 : 0; });
      return { slices: sl, total, center: data.center || null };
    }
    const TAU = Math.PI * 2;
    function targets(d) { let a = -Math.PI / 2; return d.slices.map(x => { const span = x.value / d.total * TAU, t = { a0: a, a1: a + span }; a += span; return t; }); }
    function arc(cx, cy, r, a0, a1, gap) {
      let span = a1 - a0; if (span <= 0.0004) return "";
      const g = Math.min(gap, span * .5); a0 += g / 2; a1 -= g / 2; span = a1 - a0; if (span <= 0.0004) return "";
      if (span >= TAU - 0.001) { const mx = cx + r * Math.cos(a0 + Math.PI), my = cy + r * Math.sin(a0 + Math.PI), sx = cx + r * Math.cos(a0), sy = cy + r * Math.sin(a0); return "M" + pt2(sx, sy) + "A" + r + " " + r + " 0 1 1 " + pt2(mx, my) + "A" + r + " " + r + " 0 1 1 " + pt2(sx, sy); }
      return "M" + pt2(cx + r * Math.cos(a0), cy + r * Math.sin(a0)) + "A" + r + " " + r + " 0 " + (span > Math.PI ? 1 : 0) + " 1 " + pt2(cx + r * Math.cos(a1), cy + r * Math.sin(a1));
    }
    function paint(cur) {
      const d = S.d, G = S.G; if (!d) return;
      for (const x of d.slices) { const a = S.arcs.get(x.key), t = cur.get(x.key); if (a && t) a.setAttribute("d", arc(G.cx, G.cy, G.r, t.a0, t.a1, d.slices.length > 1 ? 2.5 / G.r : 0)); }
    }
    const centerFmt = () => (typeof C.o.centerFmt === "function" ? C.o.centerFmt : C.fmtTick());
    function setCenter(v, label, animate) {
      if (v == null) cv.textContent = "";
      else if (animate) countUp(cv, null, v, centerFmt(), 600);
      else { if (cv._efcStop) { cv._efcStop(); cv._efcStop = null; } cv.dataset.v = String(v); cv.textContent = centerFmt()(v); cv._efcCur = v; }
      cl.textContent = label || "";
    }
    function build() {
      const D = S.size = sizeOf(), sw = Math.round(clamp(D * .1, 12, 20)), r = (D - sw) / 2 - 6, G = S.G = { D, sw, r, cx: D / 2, cy: D / 2 };
      if (S.svg) S.svg.remove();
      const svg = S.svg = mk(null, "svg", { class: "efc-svg", role: "group", "aria-roledescription": "chart", tabindex: "0", width: D, height: D, viewBox: "0 0 " + D + " " + D, "aria-describedby": C.tip.id });
      C.body.appendChild(svg); C.W = C.width(); C.H = D;
      const track = mk(svg, "circle", { cx: G.cx, cy: G.cy, r, fill: "none", "stroke-width": sw }); track.style.stroke = "var(--efc-line2)";
      gArcs = mk(svg, "g"); S.arcs = new Map();
      cv = mk(svg, "text", { class: "efc-ctr-v", x: G.cx, y: G.cy + 2, "text-anchor": "middle" }); cl = mk(svg, "text", { class: "efc-ctr-l", x: G.cx, y: G.cy + 19, "text-anchor": "middle" });
      wire(svg);
    }
    function syncArcs(d) {
      const G = S.G, keep = new Set(d.slices.map(x => x.key));
      for (const [k, a] of S.arcs) if (!keep.has(k)) { a.remove(); S.arcs.delete(k); }
      d.slices.forEach((x, i) => {
        let a = S.arcs.get(x.key);
        if (!a) { a = mk(gArcs, "path", { class: "efc-arc", "stroke-width": G.sw, "data-key": x.key }); a.style.transformOrigin = G.cx + "px " + G.cy + "px"; S.arcs.set(x.key, a); }
        a.style.setProperty("--c", x.css); a.dataset.i = String(i); gArcs.appendChild(a);
      });
      for (const [k, r] of S.rows) if (!keep.has(k)) { r.el.remove(); S.rows.delete(k); }
      const fs = C.fmtTick();
      d.slices.forEach(x => {
        let r = S.rows.get(x.key);
        if (!r) {
          const e = h("div", "efc-dli"); e.tabIndex = 0; e.dataset.key = x.key;
          const sw2 = h("i", "efc-sw"), em = h("em"), b = h("b"), sm = h("small"); e.append(sw2, em, b, sm);
          r = { el: e, sw: sw2, em, b, sm }; S.rows.set(x.key, r);
          const idx = () => S.d.slices.findIndex(q => q.key === x.key);
          e.addEventListener("pointerenter", () => { const j = idx(); if (j >= 0) show(j, "list"); });
          e.addEventListener("pointerleave", () => hide());
          e.addEventListener("focus", () => { const j = idx(); if (j >= 0) show(j, "key"); });
          e.addEventListener("blur", () => { if (!S.ptr) hide(); });
        }
        r.sw.style.setProperty("--c", x.css); setText(r.em, x.label); setText(r.b, fs(x.value)); setText(r.sm, pctTxt(x.share)); list.appendChild(r.el);
      });
      list.hidden = C.o.legend === false;
    }
    function update(data, opts) {
      if (C.dead) return;
      if (intake(C, data, opts) === "wait") return;
      if (data === undefined && !S.raw) return;
      if (data !== undefined) S.raw = data;
      const d = shapeD(data === undefined ? S.raw : data), prev = S.d, prevCur = S.cur;
      C.hasData = d.total > 0;
      const sig = JSON.stringify([d.slices.map(x => [x.key, x.value, x.label, x.css]), d.center, sizeOf(), C.o.unit, C.o.legend]);
      if (data !== undefined && sig === S.sig) { C.setState(C.hasData ? "ready" : "empty"); return; }
      S.sig = sig; S.d = d; C.setState(C.hasData ? "ready" : "empty");
      if (!S.svg || S.size !== sizeOf()) build();
      syncArcs(d);
      let keep = null;
      if (S.hover != null && prev && prev.slices[S.hover]) keep = d.slices.findIndex(x => x.key === prev.slices[S.hover].key);
      if (S.stopM) { S.stopM(); S.stopM = null; }
      if (!C.hasData) { S.cur = new Map(); paint(S.cur); setCenter(null, ""); C.drawn = true; hide(); return; }
      const to = targets(d), tmap = new Map(d.slices.map((x, i) => [x.key, to[i]])), first = !prevCur || !prev || !prev.total;
      const from = new Map(d.slices.map((x, i) => [x.key, first ? { a0: -Math.PI / 2, a1: -Math.PI / 2 } : (prevCur.get(x.key) || { a0: to[i].a0, a1: to[i].a0 })]));
      const c = d.center, tv = c && c.value != null ? c.value : d.total;
      if (keep == null || keep < 0) setCenter(tv, c && c.label != null ? c.label : (C.o.centerLabel || "Total"), first);
      C.drawn = true;
      S.stopM = C.tween(first ? 800 : 480, k => {
        const cur = new Map();
        for (const [key, t] of tmap) { const f = from.get(key); cur.set(key, { a0: f.a0 + (t.a0 - f.a0) * k, a1: f.a1 + (t.a1 - f.a1) * k }); }
        S.cur = cur; paint(cur);
      }, () => { S.cur = tmap; paint(tmap); S.stopM = null; });
      if (keep != null && keep >= 0) show(keep, S.hoverSrc || "ptr");
    }
    function card(i) {
      const d = S.d, x = d.slices[i], fl = C.fmtLong(), rows = [{ k: "Share", v: pctTxt(x.share), muted: true }];
      if (typeof C.o.tipRows === "function") for (const r of C.o.tipRows(Object.assign({ index: i }, x)) || []) rows.push(Array.isArray(r) ? { k: String(r[0]), v: String(r[1]) } : { k: String(r.k), v: String(r.v), muted: r.muted });
      let title = x.label;
      if (typeof C.o.label === "function") { const l = C.o.label(Object.assign({ index: i }, x)); if (l != null) title = String(l); }
      return { title, big: splitBig(fl(x.value)), rows, foot: x.def || C.o.def || "" };
    }
    function show(i, src) {
      const d = S.d; if (!d || !d.slices.length) return;
      i = clamp(Math.round(i), 0, d.slices.length - 1);
      S.hover = i; S.hoverSrc = src || "ptr"; C.idx = i;
      const x = d.slices[i], G = S.G;
      C.root.classList.add("efc-dimmed");
      d.slices.forEach((q, k) => { const a = S.arcs.get(q.key), r = S.rows.get(q.key); if (a) a.classList.toggle("on", k === i); if (r) r.el.classList.toggle("on", k === i); });
      setCenter(x.value, x.label + " · " + pctTxt(x.share), false);
      const t = (S.cur && S.cur.get(x.key)) || targets(d)[i], mid = (t.a0 + t.a1) / 2, rr = G.r + G.sw / 2 + 2;
      const bx = S.svg.getBoundingClientRect(), rb = C.root.getBoundingClientRect();
      C.showTip(card(i), (bx.left - rb.left) + G.cx + rr * Math.cos(mid), (bx.top - rb.top) + G.cy + rr * Math.sin(mid), "above");
    }
    function hide() {
      S.hover = null; C.idx = -1; C.root.classList.remove("efc-dimmed");
      for (const a of S.arcs.values()) a.classList.remove("on");
      for (const r of S.rows.values()) r.el.classList.remove("on");
      const d = S.d;
      if (d && C.hasData) { const c = d.center; setCenter(c && c.value != null ? c.value : d.total, c && c.label != null ? c.label : (C.o.centerLabel || "Total"), false); }
      C.hideTip();
    }
    function wire(svg) {
      const idxOf = e => { const t = e.target && e.target.closest ? e.target.closest(".efc-arc") : null; return t ? +t.dataset.i : -1; };
      C.on(svg, "pointermove", e => { if (e.pointerType === "touch") return; S.ptr = true; const i = idxOf(e); if (i >= 0) { if (i !== S.hover) show(i, "ptr"); } else if (S.hover != null) hide(); });
      C.on(svg, "pointerdown", e => { const i = idxOf(e); if (i >= 0) show(i, e.pointerType === "touch" ? "touch" : "ptr"); });
      C.on(svg, "pointerleave", e => { S.ptr = false; if (e.pointerType !== "touch") hide(); });
      C.on(svg, "click", e => { const i = idxOf(e); if (i >= 0 && typeof C.o.onSlice === "function") C.o.onSlice(S.d.slices[i], i); });
      C.on(doc, "pointerdown", e => { if (S.hover != null && !C.root.contains(e.target)) hide(); });
      C.on(svg, "focus", () => { if (S.ptr || !C.hasData) return; if (S.hover == null) show(0, "key"); });
      C.on(svg, "blur", () => { if (!S.ptr) hide(); });
      C.on(svg, "keydown", e => {
        const d = S.d; if (!d || !C.hasData) return;
        const k = e.key; if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Home", "End", "Escape", "Enter", " "].includes(k)) return;
        e.preventDefault(); if (k === "Escape") return hide();
        const cur = S.hover == null ? 0 : S.hover, n = d.slices.length;
        if (k === "Enter" || k === " ") { if (typeof C.o.onSlice === "function") C.o.onSlice(d.slices[cur], cur); return; }
        show(k === "Home" ? 0 : k === "End" ? n - 1 : (cur + (k === "ArrowRight" || k === "ArrowDown" ? 1 : -1) + n) % n, "key");
      });
    }
    C.watch(() => { if (S.d) { S.sig = ""; update(undefined); } });
    const api = publish(C, { update, destroy: () => C.destroy(), show: i => show(i, "api"), hide, setLoading: (on, label) => C.setLoading(on, label), get el() { return C.root; } });
    if (C.o.data) update(C.o.data); else if (C.o.loading) C.setLoading(true); else C.setState("loading");
    return api;
  }

  /* ── the public object ── */
  const api = {
    version: "2026-10-05.1", fmt, countUp, color: key => colorFor(key, 0), niceTicks,
    line: (el, o) => lineChart(el, o, "line"),
    area: (el, o) => lineChart(el, Object.assign({}, o, { area: true }), "line"),
    sparkline: (el, o) => lineChart(el, Object.assign({ height: 28 }, o), "sparkline"),
    stacked: (el, o) => ((o && o.mode === "bars") ? barsChart(el, Object.assign({}, o, { stack: true })) : lineChart(el, o, "stacked")),
    donut: (el, o) => donutChart(el, o),
    bars: (el, o) => barsChart(el, o),
    hourHeatmap: (el, o) => heatChart(el, o),
    calendarHeat: (el, o) => calChart(el, o)
  };
  root.EfficiencyCharts = api;
})(typeof window !== "undefined" ? window : globalThis);
