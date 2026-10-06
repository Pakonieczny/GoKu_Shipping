/*  charm-nest-efficiency-person.js — one employee, full page (Paul, 5 Oct 2026; plans/employee-hr/plan.md, worker E5).
 *  window.EfficiencyEmployee.mount(el, { name, onBack, range?, day?, custom?, call?, mode?, now?, onLive?, live?, onState?, onAuth? })
 *      -> { unmount, destroy, refresh, state, go, el }       (the console, charm-nest-efficiency.js, mounts it at #efficiency/person/<name>)
 *  A page, not a pop-up: header (who, where signed in now or last seen, stations), date chips (Day | Week | Month | 3 months | Year | Custom, with
 *  earlier/later arrows), a live "Now working on" card, KPI cards (grouped Production / Speed / Time / Attendance / Quality / Contact) with the
 *  change against the period before, sparklines and a hover card that says how each number is worked out (and whether it is estimated), charts
 *  with crosshair read-outs (throughput, speed, active vs signed in, busiest hours, station mix), a days-worked calendar (a click on a day opens
 *  that Day), issues grouped by kind, success and contact rates, and a real-time order search over everything this person handled.
 *  PARTS: this file is the shell (header, chips, KPIs, layout, the live card, issues, rates). The charts are E7's EfficiencyCharts
 *  (charm-nest-efficiency-charts.js: bars, line, calendarHeat, donut, hourHeatmap, sparkline), which this file only hands the data to; the live
 *  card is E6's EfficiencyStations.orderCard when that file is loaded, else a small card of its own; the order list is EfficiencyOrders.mount
 *  (E8, charm-nest-efficiency-orders.js) when loaded, else a small list of its own here (see plans/employee-hr/api.md, section E5).
 *  Reads (op names of plans/employee-hr/api.md; the console's api adds the manager passcode and Real | Sandbox; never stored here):
 *    person        { name, range, day, compare:true }   a window of 1 / 7 / 30 / 90 / 365 days ending on day (or { from, to }): kpis, series,
 *                  hours, stations, calendar, attendance, issues, rates, contact, cannotTell, notes. Each figure carries prev and delta.
 *    personOrders  { name, q, cursor, limit }           (only for this file's own list) newest first; `next` is the cursor of the next page
 *    live          {}                                    stations[].current[] of this person (the "now" card) and signedIn[] (where, since, last seen)
 *  Live: the console's own live read when it offers one, else `live` about every 3 s; the period about every 15 s; the month behind the calendar
 *  about every 60 s (ranges under 28 days); only while the page is in sight. Nothing is read while it is hidden or its tab is left; one honest
 *  "Reconnecting" line on failure; a figure the data does not know is a dash, never a zero.
 *  Motion: numbers count to their new value, charts morph from where they were, nothing flickers; transform and opacity only; reduced motion is
 *  honoured. Thumbnails zoom in place (the shared data-zoom-dot engine of charm-nest-motion.js), never full screen. "Pieces", never "lines".  */
(function (root) {
  "use strict";
  if (root.EfficiencyEmployee) return;
  const doc = root.document, TZ = "America/New_York", DAY_MS = 86400000, HOUR_MS = 3600000;
  const options = { liveMs: 3000, rangeMs: 15000, calMs: 60000, ordersMs: 15000, tickMs: 1000, growMs: 480, debounceMs: 260, pageSize: 25, maxBackoffMs: 30000, cacheMax: 14 };
  const KEY_STORE = "cn.eff.key", RANGE_STORE = "cn.eff.p.range";
  const NAMES = { shipping: "Shipping", assembly: "Assembly", welding: "Welding", sorting: "Sorting", design: "Design", laser: "Laser", inbox: "Inbox" };
  const RANGES = [["day", "Day"], ["week", "Week"], ["month", "Month"], ["quarter", "3 months"], ["year", "Year"], ["custom", "Custom"]];
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const el = (tag, cls, html) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const setText = (e, s) => { if (e && e.textContent !== s) e.textContent = s; };
  const still = () => !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches);
  const raf = f => (root.requestAnimationFrame ? root.requestAnimationFrame(f) : setTimeout(() => f(Date.now()), 16));
  const store = { get(k) { try { return root.sessionStorage.getItem(k) || ""; } catch (_) { return ""; } }, set(k, v) { try { v ? root.sessionStorage.setItem(k, v) : root.sessionStorage.removeItem(k); } catch (_) {} } };
  const slug = s => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();

  /* ── New York days (the shop day) ── */
  const partsFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric" });
  function nyParts(t) { const o = {}; for (const p of partsFmt.formatToParts(new Date(t))) if (p.type !== "literal") o[p.type] = +p.value; return o; }
  const pad = n => String(n).padStart(2, "0");
  const nyDay = t => { const p = nyParts(t); return `${p.year}-${pad(p.month)}-${pad(p.day)}`; };
  const addDays = (day, n) => { const [y, m, d] = String(day).split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
  const dayDate = day => { const [y, m, d] = String(day).split("-").map(Number); return new Date(Date.UTC(y, m - 1, d, 12)); };
  const diffDays = (a, b) => Math.round((dayDate(b) - dayDate(a)) / DAY_MS);
  const isDay = s => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
  const dowMon = day => (dayDate(day).getUTCDay() + 6) % 7;              // Monday 0 .. Sunday 6
  const mondayOf = day => addDays(day, -dowMon(day));
  const monthStart = day => day.slice(0, 8) + "01";
  const addMonths = (day, n) => { const [y, m] = day.split("-").map(Number), t = new Date(Date.UTC(y, m - 1 + n, 1)); return t.toISOString().slice(0, 8) + "01"; };
  const monthEnd = day => addDays(addMonths(day, 1), -1);
  const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hour: "numeric", minute: "2-digit" });
  const clock = t => clockFmt.format(new Date(t));
  const F = o => new Intl.DateTimeFormat("en-US", Object.assign({ timeZone: "UTC" }, o));
  const dayFmt = F({ weekday: "short", month: "short", day: "numeric" }), mdFmt = F({ month: "short", day: "numeric" }), monFmt = F({ month: "long", year: "numeric" }), monShort = F({ month: "short" });
  const wdShort = F({ weekday: "short" }), wdNum = F({ weekday: "short", day: "numeric" }), fullFmt = F({ month: "short", day: "numeric", year: "numeric" }), wdLong = F({ weekday: "long" });
  const dayLbl = day => dayFmt.format(dayDate(day)), mdLbl = day => mdFmt.format(dayDate(day));
  const hourLabel = h => `${h % 12 || 12} ${h < 12 ? "AM" : "PM"}`, hourShort = h => `${h % 12 || 12}${h < 12 ? "a" : "p"}`;

  /* ── numbers: null is "not known", never a zero that would read as "nothing happened" ── */
  const num = v => { if (v == null || v === "" || typeof v === "boolean") return null; v = +v; return Number.isFinite(v) ? v : null; };
  const nz = v => (num(v) == null ? 0 : num(v));
  const nf = n => Math.round(nz(n)).toLocaleString("en-US");
  const nf1 = n => { n = nz(n); return n >= 100 ? nf(n) : (Math.round(n * 10) / 10).toLocaleString("en-US"); };
  const minTxt = mn => `${mn < 9.95 ? Math.round(mn * 10) / 10 : Math.round(mn)} min`;
  const durMs = ms => { ms = num(ms); if (ms == null) return "—"; ms = Math.max(0, ms); if (ms < 1000) return ms > 0 ? "<1 s" : "0 min"; if (ms < 60000) return `${Math.round(ms / 1000)} s`; if (ms < 3570000) return minTxt(ms / 60000); const m = Math.round(ms / 60000), h = Math.floor(m / 60); return `${h} h${m % 60 ? ` ${m % 60} m` : ""}`; };
  const hoursTxt = ms => { ms = num(ms); if (ms == null) return "—"; const h = ms / HOUR_MS; return h >= 100 ? `${nf(h)} h` : h >= 10 ? `${nf1(h)} h` : h > 0 ? durMs(ms) : "0 min"; };
  const secTxt = s => { s = num(s); if (s == null || s <= 0) return "—"; return s < 90 ? `${Math.round(s)} s` : durMs(s * 1000); };
  const rateTxt = v => { v = num(v); return v == null || v <= 0 ? "—" : v >= 10 ? nf(v) : (Math.round(v * 10) / 10).toString(); };
  const pctTxt = f => (num(f) == null ? "—" : `${Math.round(f * 100)}%`);
  const ago = s => (s < 2 ? "just now" : s < 60 ? `${Math.round(s)}s ago` : s < 3600 ? `${Math.floor(s / 60)}m ago` : s < 86400 ? `${Math.floor(s / 3600)}h ago` : `${Math.floor(s / 86400)}d ago`);
  /* ONE Sorting station: a stored "sorter" or "qr" key is shown as Sorting (EfficiencyStations.displayStation, the same rule as the server's) */
  const dispSt = k => { const f = root.EfficiencyStations && root.EfficiencyStations.displayStation; return typeof f === "function" ? f(k) : (k === "sorter" || k === "qr" ? "sorting" : k); };
  const stName = s => NAMES[dispSt(String(s || "").toLowerCase())] || (s ? String(s).charAt(0).toUpperCase() + String(s).slice(1) : "Station");
  const initials = name => { const w = String(name || "?").replace(/[^\p{L}\p{N} ._-]/gu, "").split(/[ ._-]+/).filter(Boolean); return ((w[0] || "?").charAt(0) + (w.length > 1 ? w[w.length - 1].charAt(0) : "")).toUpperCase(); };
  const tint = name => { let h = 0; for (const c of String(name)) h = (h * 31 + c.charCodeAt(0)) >>> 0; return h % 4; };
  const stamp = (t, today) => (nyDay(t) === today ? clock(t) : `${mdLbl(nyDay(t))}, ${clock(t)}`);
  const safeUrl = u => { u = String(u || ""); return /^(https?:\/\/|data:image\/|blob:|\/(?!\/))/i.test(u) ? u : ""; };

  /* ── motion: one tween for numbers and chart geometry (instant under reduced motion) ── */
  function tween(ms, step, done) {
    if (still() || !(ms > 0)) { step(1); if (done) done(); return () => {}; }
    let t0 = null, dead = false;
    const f = ts => { if (dead) return; if (t0 == null) t0 = ts; const k = Math.min(1, (ts - t0) / ms); step(1 - Math.pow(1 - k, 3)); if (k < 1) raf(f); else if (done) done(); };
    raf(f);
    return () => { dead = true; };
  }
  /** A figure that counts to its new value (and reads as data-v at once). Unknown (null) is a dash. */
  function setNum(e, to, fmt, from0) {
    if (!e) return; fmt = fmt || nf; to = num(to);
    if (to == null) { if (e._stop) e._stop(); e._v = null; e._cur = null; delete e.dataset.v; setText(e, "—"); return; }
    if (e._v === to) return;
    const was = e._v == null ? (from0 ? 0 : to) : e._cur != null ? e._cur : e._v;
    if (e._stop) e._stop(); e._v = to; e.dataset.v = to;
    if (was === to) { e._cur = null; setText(e, fmt(to)); return; }
    e._stop = tween(options.growMs, k => { e._cur = was + (to - was) * k; setText(e, fmt(e._cur)); }, () => { e._cur = null; setText(e, fmt(to)); });
  }

  /* ── the periods: the server's own, rolling windows that END on the anchor day (day 1, week 7, month 30, 3 months 90, year 365 days) ── */
  const RANGE_DAYS = { day: 1, week: 7, month: 30, quarter: 90, year: 365 };
  function periodOf(range, anchor, custom) {
    if (RANGE_DAYS[range]) return { from: addDays(anchor, 1 - RANGE_DAYS[range]), to: anchor };
    const c = custom || {}; return isDay(c.from) && isDay(c.to) ? { from: c.from, to: c.to } : { from: addDays(anchor, -29), to: anchor };
  }
  /** The anchor (last day) of the window before (dir -1) or after (+1), the same length. */
  function shiftAnchor(range, anchor, dir, p) {
    const n = RANGE_DAYS[range] || diffDays(p.from, p.to) + 1; return addDays(anchor, n * dir);
  }
  function periodLabel(range, p, today) {
    if (range === "day") { const d = dayLbl(p.from); return p.from === today ? `Today · ${d}` : p.from === addDays(today, -1) ? `Yesterday · ${d}` : d; }
    const n = diffDays(p.from, p.to) + 1, sameYear = p.from.slice(0, 4) === p.to.slice(0, 4), a = sameYear ? mdLbl(p.from) : fullFmt.format(dayDate(p.from)), b = p.to === today ? "today" : fullFmt.format(dayDate(p.to));
    return `${n} days · ${a} – ${p.to === today || !sameYear || n > 300 ? b : p.from.slice(5, 7) === p.to.slice(5, 7) ? +p.to.slice(8) : mdLbl(p.to)}`;
  }
  const PREV_WORD = { day: "day before", week: "prev. 7 days", month: "prev. 30 days", quarter: "prev. 90 days", year: "prev. 365 days", custom: "prev. period" };

  /* ── styles: the console's tokens (calm paper, thin lines, one gold) ── */
  function style() {
    if (doc.getElementById("efpStyle")) return;
    const s = doc.createElement("style"); s.id = "efpStyle";
    s.textContent = `
.efp{container-type:inline-size;container-name:efp;position:relative;display:grid;grid-template-columns:minmax(0,1fr);gap:12px;min-width:0;color:var(--ink);font-size:12.5px;padding-bottom:8px;align-content:start}
.efp,.efp *{box-sizing:border-box}.efp .hidden{display:none!important}
.efp button{font-family:inherit}
.efp .spin{width:11px;height:11px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:spin .7s linear infinite;flex:0 0 11px;display:inline-block}
.efp.in>:not(.efpHC){animation:efpIn .42s cubic-bezier(.2,.8,.2,1) both}.efp.in>:nth-child(2){animation-delay:.04s}.efp.in>:nth-child(3){animation-delay:.08s}.efp.in>:nth-child(4){animation-delay:.12s}.efp.in>:nth-child(n+5){animation-delay:.16s}
@keyframes efpIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.efpTop{display:flex;align-items:center;gap:10px;min-height:30px}
.efpBack{display:inline-flex;align-items:center;gap:5px;border:0;background:transparent;border-radius:8px;padding:5px 10px 5px 5px;font:700 12px var(--sans);color:var(--ink70);cursor:pointer}
.efpBack svg{width:16px;height:16px;transition:transform .2s ease}.efpBack:hover{background:var(--paper2);color:var(--ink)}.efpBack:hover svg{transform:translateX(-2px)}
.efpHead{display:flex;align-items:center;gap:8px 16px;flex-wrap:wrap;min-width:0}
.efpAv{position:relative;width:48px;height:48px;flex:0 0 48px;border-radius:50%;display:grid;place-items:center;font:700 16px var(--sans);letter-spacing:.02em;border:1px solid var(--goldLine);background:var(--goldSoft);color:#7a5a1d}
.efpAv[data-t="1"]{background:var(--sageSoft);border-color:#cfdcc6;color:#3f5a3b}.efpAv[data-t="2"]{background:var(--slateSoft);border-color:#c5d6db;color:#35525d}.efpAv[data-t="3"]{background:var(--claySoft);border-color:#ebcdc2;color:#8a3f2b}
.efpAv:after{content:"";position:absolute;right:-1px;bottom:-1px;width:12px;height:12px;border-radius:50%;background:var(--ink25);border:2px solid var(--paper);transition:background .3s}.efpAv[data-on]:after{background:var(--sage)}
.efpWho{display:grid;gap:2px;min-width:0;flex:1 1 220px}
.efpName{margin:0;font:700 21px/1.15 var(--sans);letter-spacing:-.012em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.efpWhere{color:var(--ink45);font-size:12px;min-height:16px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.efpWhere b{color:var(--ink70);font-weight:650}
.efpChips{display:flex;flex-wrap:wrap;gap:4px 5px;min-width:0}
.efpChip{display:inline-flex;gap:5px;align-items:baseline;border:1px solid var(--line);border-radius:999px;padding:2px 9px;font-size:11px;color:var(--ink70);white-space:nowrap;background:var(--card2)}
.efpChip b{font-weight:700;color:var(--ink)}.efpChip.now{border-color:var(--sage);background:var(--sageSoft)}
.efpLive{display:inline-flex;align-items:center;gap:7px;font-size:11.5px;color:var(--ink45);white-space:nowrap;min-width:0}
.efpDot{position:relative;width:7px;height:7px;border-radius:50%;background:var(--ink25);flex:0 0 7px}.efpDot:after{content:"";position:absolute;inset:0;border-radius:50%;background:inherit;opacity:0;pointer-events:none}
.efpLive[data-s=live] .efpDot{background:var(--sage)}.efpLive[data-s=live] .efpDot:after{animation:efpPulse 2.4s ease-out infinite}.efpLive[data-s=slow] .efpDot{background:var(--gold2)}.efpLive[data-s=load] .efpDot{display:none}
.efpLive .spin{display:none}.efpLive[data-s=load] .spin{display:inline-block}
@keyframes efpPulse{0%{opacity:.4;transform:scale(1)}70%,100%{opacity:0;transform:scale(2.7)}}
.efpBar{position:sticky;top:var(--efp-top,4px);z-index:6;display:flex;align-items:center;gap:6px 12px;flex-wrap:wrap;min-height:42px;padding:6px 10px 6px 12px;background:rgba(255,254,251,.93);backdrop-filter:blur(10px);border:1px solid var(--line);border-radius:12px;box-shadow:var(--sh)}
.efpSeg button{padding:4px 11px;font-size:11px;transition:background .2s,color .2s}.efpSeg button:not(.on):hover{background:var(--card2);color:var(--ink)}
.efpNav{display:inline-flex;align-items:center;gap:2px;min-width:0}
.efpDay{min-width:156px;text-align:center;font-weight:650;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efpIcon{border:0;background:transparent;border-radius:6px;width:26px;height:26px;color:var(--ink70);font-size:16px;line-height:1;padding:0;cursor:pointer}
.efpIcon:hover:not(:disabled){background:var(--paper2);color:var(--ink)}.efpIcon:disabled{opacity:.3;cursor:default}
.efpToday{border:0;background:transparent;font:700 11px var(--sans);color:var(--gold);padding:4px 6px;border-radius:6px;cursor:pointer}.efpToday:hover{background:var(--goldSoft)}
.efpGrow{flex:1 1 0}
.efpCustom{display:inline-flex;align-items:center;gap:6px;font-size:11px;color:var(--ink45)}
.efpCustom input{border:1px solid var(--line);background:var(--card2);border-radius:8px;padding:3px 7px;font:500 11.5px var(--sans);color:var(--ink)}.efpCustom input:focus{outline:2px solid var(--gold);outline-offset:1px;background:#fff}
.efpCustom .btn{padding:4px 10px;font-size:11px}
.efpBusy{display:inline-flex;align-items:center;gap:7px;color:var(--ink45);font-size:11.5px;opacity:0;transition:opacity .2s;pointer-events:none}.efpBusy.on{opacity:1}
.efpMsg{display:flex;align-items:center;gap:9px;padding:9px 14px;border:1px solid var(--goldLine);background:var(--goldSoft);border-radius:10px;color:#6a4d17;font-size:12px}
.efpMsg.bad{border-color:#ebcdc2;background:var(--claySoft);color:#8a3f2b}.efpMsg .btn{margin-left:auto}
.efpWait{display:flex;align-items:center;justify-content:center;gap:9px;padding:56px 0;color:var(--ink70);font-size:12.5px}
.efpBody{display:grid;gap:12px;min-width:0;transition:opacity .25s ease;align-content:start}.efpBody.dim{opacity:.5}
.efpNote{margin:0;display:grid;gap:2px;color:var(--ink70);font-size:12px;padding:0 2px}
.efpNote span:before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--gold2);margin-right:8px;vertical-align:1px}
.efpLabel{font-size:10.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--ink45);font-weight:750;display:flex;align-items:center;gap:8px;margin:0 2px 9px}
.efpLabel:after{content:"";flex:1;height:1px;background:var(--line);order:1}.efpLabel b{color:var(--ink70);letter-spacing:0;font-weight:700}.efpLabel .efpLr,.efpLabel .efpWR{order:2;letter-spacing:0;text-transform:none;font-weight:500;font-size:11.5px}
.efpCard{background:var(--card);border:1px solid var(--line);border-radius:12px;min-width:0}
.efpNow{display:grid;gap:10px}
.efpNowIdle{display:flex;align-items:center;gap:10px;padding:11px 16px;color:var(--ink45);font-size:12px}.efpNowIdle b{color:var(--ink70);font-weight:650}.efpNowIdle i{width:7px;height:7px;border-radius:50%;background:var(--ink25);flex:0 0 7px}
.efpNowCard{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:14px;align-items:center;padding:12px 16px;position:relative;overflow:visible}
.efpNowCard:before{content:"";position:absolute;left:0;top:12px;bottom:12px;width:3px;border-radius:0 3px 3px 0;background:var(--sage)}
.efpNowInfo{display:grid;gap:4px;min-width:0}.efpNowTop{display:flex;align-items:baseline;flex-wrap:wrap;gap:4px 10px;min-width:0}
.efpNowSt{font:700 10.5px var(--sans);letter-spacing:.08em;text-transform:uppercase;color:var(--sage)}.efpNowCust{color:var(--ink70)}
.efpNowSince{font-size:12px;color:var(--ink45)}.efpNowSince b{color:var(--ink);font-weight:650;font-variant-numeric:tabular-nums}
.efpPieces{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.efpPlus{font-size:11px;color:var(--ink45);padding:0 4px}
.efpTh{position:relative;width:42px;height:42px;flex:0 0 42px;padding:0;border:1px solid var(--line2);border-radius:8px;background:#fff;display:grid;place-items:center;overflow:visible;cursor:pointer;color:var(--ink25);font:600 10px var(--sans)}
.efpTh.big{width:64px;height:64px;flex-basis:64px;border-radius:10px}
.efpTh img{display:block;width:100%;height:100%;object-fit:contain;border-radius:inherit;pointer-events:none}
.efpTh:focus-visible{outline:2px solid var(--gold);outline-offset:2px}
.efpTh.sealZoomed{box-shadow:0 0 0 2px var(--gold),0 8px 22px rgba(30,26,20,.28)}
.efpTh.none{background:var(--card2);border-style:dashed}
.efpQr{width:48px;height:48px;flex:0 0 48px;border:1px solid var(--line2);border-radius:7px;background:#fff;display:grid;place-items:center;padding:2px;overflow:visible;cursor:pointer}
.efpQr img{display:block;width:100%;height:100%;image-rendering:pixelated;pointer-events:none}
.efpQr.sealZoomed{box-shadow:0 0 0 2px var(--gold),0 8px 22px rgba(30,26,20,.28)}
.efpOid{border:0;background:transparent;padding:2px 6px;margin:0 -6px;border-radius:6px;font:650 12px var(--mono);color:var(--ink);text-align:left;white-space:nowrap;cursor:pointer}
.efpOid:hover{background:var(--goldSoft);text-decoration:underline;text-decoration-color:var(--gold2);text-underline-offset:3px}
.efpKGroups{display:grid;gap:22px}.efpGroup{display:grid;gap:0}.efpBody>section,.efpBody>.efpGrid.g57{margin-top:10px}
.efpKpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.efpK{position:relative;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 16px 10px;display:grid;grid-template-rows:auto auto auto 1fr;align-content:start;gap:2px;min-width:0;outline:none;transition:transform .2s ease,box-shadow .2s ease,border-color .2s ease;cursor:default;overflow:hidden}
.efpK:hover,.efpK:focus-visible,.efpK.hov{transform:translateY(-1px);box-shadow:var(--sh);border-color:var(--ink25)}
.efpK:focus-visible{outline:2px solid var(--gold);outline-offset:2px}
.efpKL{font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;display:flex;gap:6px;align-items:flex-start;line-height:1.3}
.efpKL .t{min-width:0;overflow:hidden;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;line-clamp:2}
.efpKR{display:flex;align-items:center;justify-content:space-between;gap:8px;min-width:0}
.efpKL i{font-style:normal;font-weight:700;font-size:8.5px;letter-spacing:.04em;padding:1px 4px;border-radius:4px;background:var(--goldSoft);color:#7a5a1d}
.efpKV{font:600 29px/1.1 var(--sans);letter-spacing:-.022em;font-variant-numeric:tabular-nums;white-space:nowrap}.efpKV small{font-size:13px;font-weight:600;color:var(--ink45);letter-spacing:0;margin-left:3px}
.efpKD{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--ink45);min-height:15px;flex-wrap:wrap;gap:1px 6px}.efpKD span{white-space:nowrap}
.efpKD b{display:inline-flex;align-items:center;gap:2px;font-weight:700;font-variant-numeric:tabular-nums;padding:0 5px;border-radius:5px;background:var(--paper2);color:var(--ink70)}
.efpKD b.up{background:var(--sageSoft);color:#46623f}.efpKD b.down{background:var(--claySoft);color:#8a3f2b}
.efpKF{display:flex;align-items:flex-end;align-self:end;gap:8px;min-height:14px;min-width:0}.efpKS{flex:1 1 auto;min-width:0;font-size:11px;line-height:1.35;color:var(--ink45);min-height:14px}
.efpKsp{flex:0 1 76px;min-width:24px;height:26px;pointer-events:none;opacity:.95}
.efpHC{position:absolute;z-index:12;pointer-events:none;width:max-content;max-width:min(272px,calc(100% - 16px));background:var(--card);color:var(--ink70);border:1px solid var(--line);border-radius:11px;padding:9px 12px 10px;font-size:11.5px;line-height:1.45;box-shadow:0 10px 26px rgba(30,26,20,.13);opacity:0;visibility:hidden;transform:translateY(4px);transition:opacity .14s ease,transform .14s ease,visibility 0s .14s}
.efpHC.on{opacity:1;visibility:visible;transform:none;transition:opacity .14s ease,transform .14s ease}
.efpHC b{display:block;font:650 12.5px var(--sans);color:var(--ink);margin-bottom:2px}.efpHC p{margin:0;color:var(--ink70)}.efpHC .tag{display:inline-block;margin-top:6px;font:700 9px var(--sans);letter-spacing:.07em;text-transform:uppercase;padding:2px 6px;border-radius:4px;background:var(--goldSoft);color:#7a5a1d}.efpHC .tag.ok{background:var(--sageSoft);color:#46623f}.efpHC .prev{display:block;margin-top:5px;color:var(--ink45);font-size:11px}
.efpGrid{display:grid;gap:12px;min-width:0}.efpGrid.g2{grid-template-columns:repeat(2,minmax(0,1fr))}.efpGrid.g2>.wide{grid-column:1/-1}.efpGrid.g75{grid-template-columns:minmax(0,7fr) minmax(0,5fr)}.efpGrid.g57{grid-template-columns:minmax(0,5fr) minmax(0,7fr)}
.efpChart{padding:12px 18px 14px;display:grid;gap:6px;min-width:0;align-content:start}
.efpCH{display:flex;align-items:center;gap:6px 12px;flex-wrap:wrap;min-height:26px}
.efpCT{font-weight:700;letter-spacing:.07em;text-transform:uppercase;font-size:10.5px;color:var(--ink45)}.efpCP{margin-left:auto;font-size:11.5px;color:var(--ink45);display:inline-flex;gap:10px;align-items:center;flex-wrap:wrap}
.efpTog{display:inline-flex}.efpTog button{padding:2px 9px;font-size:10.5px}
.efpXY{position:relative;min-width:0}
.efpTip{position:absolute;top:2px;z-index:4;pointer-events:none;background:var(--card);color:var(--ink70);border:1px solid var(--line);border-radius:11px;padding:7px 10px;font-size:11px;box-shadow:0 10px 26px rgba(30,26,20,.13);white-space:nowrap;display:grid;gap:1px;opacity:0;visibility:hidden;transition:opacity .1s}
.efpTip.on{opacity:1;visibility:visible}.efpTipT{color:var(--ink45);font-size:10.5px}.efpTipV{font:650 14px var(--sans);color:var(--ink)}.efpTipR{display:flex;justify-content:space-between;gap:16px;color:var(--ink70)}.efpTipR b{color:var(--ink);font-weight:650}.efpTipH{margin-top:3px;color:#7a5a1d;font-size:10.5px}
.efpIs{display:grid}.efpIg+.efpIg{border-top:1px solid var(--line2)}
.efpIgh{display:grid;grid-template-columns:minmax(0,1fr) auto 18px;gap:10px;align-items:center;width:100%;border:0;background:transparent;padding:9px 18px;text-align:left;cursor:pointer;border-radius:0;color:var(--ink)}
.efpIgh:hover{background:var(--card2)}.efpIgh b{font-weight:700;font-size:12.5px}.efpIgh small{display:block;font-weight:400;color:var(--ink45);font-size:11px;margin-top:1px;white-space:normal}
.efpIgh em{font-style:normal;font:700 13px var(--sans);font-variant-numeric:tabular-nums}.efpIgh i{font-style:normal;font-size:9px;color:var(--ink45);transition:transform .25s ease}.efpIg.open .efpIgh i{transform:rotate(180deg)}
.efpIgw{display:grid;grid-template-rows:0fr;visibility:hidden;transition:grid-template-rows .3s ease,visibility 0s .3s}.efpIg.open .efpIgw{grid-template-rows:1fr;visibility:visible;transition:grid-template-rows .3s ease}
.efpIgi{min-height:0;overflow:hidden}.efpIgl{padding:0 18px 10px;display:grid}
.efpIr{display:grid;grid-template-columns:104px 112px minmax(0,1fr);gap:10px;align-items:baseline;padding:5px 0;border-top:1px solid var(--line2);font-size:12px}.efpIr time{color:var(--ink45);font-size:11.5px;font-variant-numeric:tabular-nums}.efpIr span{color:var(--ink70)}
.efpIm{border:0;background:transparent;color:var(--gold);font:700 11px var(--sans);padding:6px 0 2px;cursor:pointer;justify-self:start}.efpIm:hover{text-decoration:underline}
.efpRates{display:grid;gap:12px;padding:6px 18px 16px}
.efpRt{display:grid;gap:5px;padding:4px 0;border-radius:8px;cursor:default}
.efpRtH{display:flex;flex-wrap:wrap;align-items:baseline;gap:2px 8px;font-size:12px}.efpRtH b{font-weight:700}.efpRtH span{margin-left:auto;color:var(--ink70);font-variant-numeric:tabular-nums}.efpRtH em{font-style:normal;color:var(--ink45);font-size:11px;white-space:nowrap}.efpRtH em.efpD{font-weight:700;font-variant-numeric:tabular-nums;padding:0 5px;border-radius:5px;background:var(--paper2);color:var(--ink70)}.efpRtH em.efpD.up{background:var(--sageSoft);color:#46623f}.efpRtH em.efpD.down{background:var(--claySoft);color:#8a3f2b}
.efpOrdersMod{display:block;padding:2px 2px 4px}
.efpRtB{display:flex;height:7px;border-radius:4px;overflow:hidden;background:var(--line2);gap:2px}.efpRtB i{display:block;height:100%;width:0;transition:width .55s cubic-bezier(.2,.8,.2,1);min-width:0}.efpRtB .ok{background:var(--sage)}.efpRtB .bad{background:var(--clay)}.efpRtB .un{background:var(--ink25)}
.efpFind{display:flex;align-items:center;gap:8px;padding:12px 16px 8px;flex-wrap:wrap}
.efpSearch{position:relative;flex:1 1 280px;min-width:0}
.efpSearch input{width:100%;border:1px solid var(--line);background:var(--card2);border-radius:999px;padding:7px 34px 7px 34px;font:500 12.5px var(--sans);color:var(--ink);transition:border-color .2s,background .2s,box-shadow .2s}
.efpSearch input::placeholder{color:var(--ink45)}.efpSearch input:focus{outline:none;border-color:var(--gold);background:#fff;box-shadow:0 0 0 3px var(--goldSoft)}
.efpSearch svg{position:absolute;left:12px;top:50%;width:14px;height:14px;margin-top:-7px;color:var(--ink45);pointer-events:none}
.efpSearch .efpIcon{position:absolute;right:5px;top:50%;margin-top:-13px;width:26px;height:26px;font-size:13px}
.efpSt{display:inline-flex;align-items:center;gap:7px;color:var(--ink45);font-size:11.5px;min-height:20px}.efpSt b{color:var(--ink70);font-weight:650}
.efpOl{display:grid;padding:0 6px 4px}
.efpO{display:grid;grid-template-columns:96px minmax(150px,1.1fr) 120px minmax(0,2fr) 48px;gap:12px;align-items:center;padding:8px 10px;border-top:1px solid var(--line2);border-radius:10px;cursor:pointer;content-visibility:auto;contain-intrinsic-size:auto 64px;transition:background .2s;outline:none}
.efpO:first-child{border-top-color:transparent}.efpO:hover,.efpO:focus-visible{background:var(--card2)}.efpO:focus-visible{box-shadow:inset 0 0 0 2px var(--gold)}
.efpO.new{animation:efpNew 1.1s ease}@keyframes efpNew{from{background:var(--goldSoft);opacity:.2;transform:translateY(-4px)}to{background:transparent;opacity:1;transform:none}}
.efpO.ent{animation:efpIn .36s cubic-bezier(.2,.8,.2,1) both}
.efpOt{color:var(--ink45);font-size:11.5px;font-variant-numeric:tabular-nums;line-height:1.35}.efpOt b{display:block;color:var(--ink70);font-weight:650;font-size:12px}
.efpOn{display:grid;gap:1px;min-width:0}.efpOn .efpOid{justify-self:start}.efpOn span{color:var(--ink70);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.efpOs{display:grid;gap:1px;font-size:11.5px;color:var(--ink45)}.efpOs b{color:var(--ink);font-weight:650;font-size:12px}
.efpOp{display:flex;flex-wrap:wrap;gap:6px;align-items:center;min-width:0}
.efpO mark{background:var(--goldSoft);color:inherit;border-radius:3px;padding:0 1px}
.efpMore{display:flex;align-items:center;justify-content:center;gap:9px;padding:10px 0 14px;color:var(--ink45);font-size:11.5px;min-height:40px}
.efpMore button{border:1px solid var(--line);background:var(--card2);border-radius:999px;padding:5px 14px;font:650 11.5px var(--sans);color:var(--ink70);cursor:pointer}.efpMore button:hover{background:var(--paper2);color:var(--ink)}
.efpEmptyBox{padding:26px 18px;text-align:center;color:var(--ink45);font-size:12.5px}
.efpFoot{margin:2px 2px 0;color:var(--ink45);font-size:11px;line-height:1.5}
.efpSkel{border-radius:6px;background:linear-gradient(90deg,var(--line2),var(--paper2),var(--line2));background-size:200% 100%;animation:efpSk 1.4s ease-in-out infinite}@keyframes efpSk{to{background-position:-200% 0}}
.efpLink{border:0;background:transparent;color:var(--gold);font:700 11px var(--sans);letter-spacing:0;text-transform:none;padding:2px 6px;border-radius:6px;cursor:pointer;order:2}.efpLink:hover{background:var(--goldSoft)}
.efpK.more{animation:efpIn .32s cubic-bezier(.2,.8,.2,1) both}
.efpKT{flex:none;margin-left:auto;font:700 8.5px var(--sans);letter-spacing:.05em;text-transform:uppercase;color:var(--ink25);white-space:nowrap;line-height:1.6}
.efpShift{display:grid;gap:10px;padding:2px 0}.efpShiftT{display:flex;flex-wrap:wrap;gap:4px 18px;font-size:12.5px;color:var(--ink70)}.efpShiftT b{color:var(--ink);font-weight:650;font-variant-numeric:tabular-nums}
.efpSB{display:flex;height:12px;border-radius:6px;overflow:hidden;background:var(--line2);gap:2px;position:relative}.efpSB i{display:block;height:100%;width:0;transition:width .6s cubic-bezier(.2,.8,.2,1);cursor:default}.efpSB .a{background:#6f6a62}.efpSB .i{background:var(--gold2)}.efpSB .u{background:var(--line)}
.efpSLeg{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:11.5px;color:var(--ink45)}.efpSLeg span{display:inline-flex;gap:6px;align-items:center}.efpSLeg i{width:9px;height:9px;border-radius:3px;display:inline-block}
.efpSum{display:flex;flex-wrap:wrap;gap:6px;padding:10px 18px 4px}.efpSum span{display:inline-flex;gap:5px;align-items:baseline;border:1px solid var(--line);border-radius:999px;padding:2px 10px;font-size:11px;color:var(--ink70);background:var(--card2)}.efpSum b{color:var(--ink);font-weight:700;font-variant-numeric:tabular-nums}
.efpAt{display:inline-block;font:700 9px var(--sans);letter-spacing:.06em;text-transform:uppercase;padding:1px 6px;border-radius:4px;margin-left:8px;vertical-align:1px;background:var(--paper2);color:var(--ink70)}.efpAt.own{background:var(--goldSoft);color:#7a5a1d}.efpAt.system{background:var(--paper2);color:var(--ink45)}.efpNone{padding:8px 18px 4px;border-top:1px solid var(--line2)}.efpNone b{color:var(--ink70);font-weight:650}
.efpHow{padding:2px 0 6px;color:var(--ink45);font-size:11.5px;line-height:1.5}
.efpPad{padding:0 18px}.efpIs>.efpHow{padding:9px 18px 12px;border-top:1px solid var(--line2)}.efpIs>.efpHow+.efpHow{border-top:0;padding-top:0;margin-top:-6px}
.efpSub{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);font-weight:750;margin:6px 0 -4px}
.efpStat{display:flex;flex-wrap:wrap;gap:4px 18px;font-size:12px;color:var(--ink70)}.efpStat b{color:var(--ink);font-weight:650;font-variant-numeric:tabular-nums;margin-left:4px}
.efpCannot{display:grid;gap:3px;margin:2px 2px 0;color:var(--ink45);font-size:11.5px;line-height:1.5}.efpCannot b{color:var(--ink70);font-weight:650}
.efpFound{display:flex;align-items:center;gap:9px;padding:9px 14px;border:1px solid var(--line);background:var(--card2);border-radius:10px;color:var(--ink70);font-size:12px}
@container efp (max-width:1180px){.efpKpis{grid-template-columns:repeat(4,minmax(0,1fr))}.efpO{grid-template-columns:88px minmax(130px,1fr) 104px minmax(0,1.6fr) 48px}.efpDay{min-width:132px}}
@container efp (max-width:900px){.efpGrid.g2,.efpGrid.g75,.efpGrid.g57{grid-template-columns:minmax(0,1fr)}.efpKpis{grid-template-columns:repeat(2,minmax(0,1fr))}.efpO{grid-template-columns:78px minmax(0,1fr) 92px 44px;grid-template-areas:"t n s q" "p p p p";gap:6px 10px}.efpO>.efpOt{grid-area:t}.efpO>.efpOn{grid-area:n}.efpO>.efpOs{grid-area:s}.efpO>.efpOp{grid-area:p}.efpO>.efpQr{grid-area:q;width:44px;height:44px}}
@container efp (max-width:640px){.efpWhere{white-space:normal}.efpAv{width:42px;height:42px;flex-basis:42px;font-size:14px}.efpName{font-size:18px;white-space:normal;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;line-clamp:2;overflow-wrap:anywhere}.efpChips{flex:1 1 100%}.efpBar{gap:6px 8px;padding:6px 8px}.efpSeg{order:1;max-width:100%;overflow-x:auto;scrollbar-width:none}.efpSeg button{padding:4px 9px;flex:0 0 auto}.efpNav{order:2;flex:1 1 100%}.efpDay{flex:1;min-width:0}.efpBusy{order:3}
.efpRtH b{flex:1 1 100%;order:-2}.efpRtH span{order:-1;margin-left:0;color:var(--ink);font-weight:700}
.efpKpis{gap:8px}.efpKL{font-size:9.5px;letter-spacing:.04em;min-height:2.6em;align-items:flex-end}.efpK{padding:11px 12px 9px}.efpKV{font-size:25px}.efpKsp{flex-basis:48px}.efpChart{padding:11px 12px 12px}.efpNowCard{grid-template-columns:auto minmax(0,1fr);padding:11px 12px}.efpNowCard>.efpQr{display:none}
.efpO{grid-template-columns:70px minmax(0,1fr) 44px;grid-template-areas:"t n q" "s s s" "p p p"}.efpO>.efpOs{grid-area:s;display:flex;gap:10px;align-items:baseline}.efpIr{grid-template-columns:100px minmax(0,1fr);grid-template-areas:"t n" "r r"}.efpIr>time{grid-area:t}.efpIr>.efpOid{grid-area:n}.efpIr>span{grid-area:r}.efpIgh,.efpIgl,.efpRates{padding-left:12px;padding-right:12px}.efpPad,.efpIs>.efpHow,.efpNone,.efpSum{padding-left:12px;padding-right:12px}.efpFind{padding:10px 12px 6px}.efpOl{padding:0 2px 4px}
}
@container efp (max-width:330px){.efpKsp{display:none}.efpKV{font-size:22px}.efpKD span{white-space:normal}.efpTog{max-width:100%}.efpTog button{padding:2px 5px}.efp .efpTRm{white-space:normal}}
@media (prefers-reduced-motion:reduce){.efp *,.efp *:before,.efp *:after{transition:none!important;animation:none!important}}`;
    doc.head.appendChild(s);
  }

  /* ── the answer, normalised (a figure the data does not know is null = a dash, never a made-up zero) ── */
  const A = v => (Array.isArray(v) ? v : []);
  const first = (o, ...keys) => { if (o && typeof o === "object") for (const k of keys) if (o[k] != null) return o[k]; return undefined; };
  const pct1 = v => { v = num(v); return v == null ? null : Math.round(v * 10) / 10; };
  /** One number worth showing: how to say it (unit), which way is good, its plain definition, whether it is an estimate. (Plain words here are the fallback; the server's own `label` and `def` win.) */
  const CARD = {
    parts: { label: "Pieces", unit: "pieces", better: "up", def: "Pieces this person finished at a station (net of undos) in the period.", sp: "parts" },
    orders: { label: "Orders", unit: "orders", better: "up", def: "Distinct orders this person worked on in the period.", sp: "orders" },
    ordersCompleted: { label: "Orders completed", unit: "orders", better: "up", def: "Orders this person finished at a station.", sp: "completes" },
    scans: { label: "Scans", unit: "scans", better: "up", def: "Scan actions logged under this person's name.", sp: "scans" },
    partsPerDay: { label: "Pieces per day worked", unit: "pieces/day", better: "up", def: "Pieces divided by days worked.", sp: "parts" },
    ordersPerDay: { label: "Orders per day worked", unit: "orders/day", better: "up", def: "Orders divided by days worked.", sp: "orders" },
    partsPerActiveHour: { label: "Pieces per active hour", unit: "pieces/hour", better: "up", def: "Pieces divided by active hours: time between actions that were less than 5 minutes apart.", sp: "perActiveHour" },
    partsPerSignedHour: { label: "Pieces per signed-in hour", unit: "pieces/hour", better: "up", def: "Pieces divided by every hour signed in, active or not.", sp: "perSignedHour" },
    bestDay: { label: "Best day", unit: "pieces", better: "up", def: "The day with the most pieces in the period." },
    peakHour: { label: "Busiest hour", unit: "hour", better: null, def: "The hour of the day with the most pieces." },
    secPerOrderMedian: { label: "Median per order", unit: "seconds", better: "down", def: "The middle time this person took for one order. Half were faster, half slower.", sp: "medianSecPerOrder" },
    secPerOrderP90: { label: "Slowest 10%", unit: "seconds", better: "down", def: "Nine in ten orders were done faster than this: the slow tail, not thrown by one odd order." },
    secPerOrderMean: { label: "Average per order", unit: "seconds", better: "down", def: "Active seconds divided by orders.", sp: "secPerOrder" },
    secPerScanMean: { label: "Average per scan", unit: "seconds", better: "down", def: "Active seconds divided by scans." },
    secBetweenScansMedian: { label: "Between scans", unit: "seconds", better: "down", def: "The middle gap between one scan and the next, breaks left out." },
    signedHours: { label: "Signed in", unit: "hours", better: null, def: "Total time signed in at a station, from the sign-in records.", sp: "signedMs" },
    activeHours: { label: "Active", unit: "hours", better: "up", def: "Time between actions that were less than 5 minutes apart.", sp: "activeMs" },
    idleHours: { label: "Idle", unit: "hours", better: null, def: "Gaps of more than 5 minutes between actions while signed in.", sp: "idleMs" },
    unloggedHours: { label: "Unlogged", unit: "hours", better: null, def: "Signed-in time with no action logged at all." },
    activeShare: { label: "Active vs signed in", unit: "percent", better: "up", def: "Active time as a share of the time signed in. Breaks, waiting and desk work are not activity.", sp: "share" },
    avgStart: { label: "Usual start", unit: "clock", better: null, def: "The average time of the first sign-in on days worked." },
    avgEnd: { label: "Usual finish", unit: "clock", better: null, def: "The average time of the last sign-out on days worked." },
    avgShiftHours: { label: "Average shift", unit: "hours", better: null, def: "Signed-in time on an average day worked." },
    daysWorked: { label: "Days worked", unit: "days", better: "up", def: "Working days on which this person was signed in or had recorded work.", sp: "worked" },
    daysOff: { label: "Days off", unit: "days", better: "down", def: "Team working days on which this person never signed in and recorded nothing. Weekends and days nobody worked are not counted.", sp: "off" },
    workingDays: { label: "Working days", unit: "days", better: null, def: "Days the team worked and this person could have worked." },
    extraDays: { label: "Extra days", unit: "days", better: null, def: "Days this person came in that were not working days for the team." },
    lateDays: { label: "Late starts", unit: "days", better: "down", def: "Days this person first signed in more than 30 minutes after their own usual start." },
    shortDays: { label: "Short days", unit: "days", better: "down", def: "Days signed in for less than half of this person's own usual shift." },
    currentStreak: { label: "Current streak", unit: "days", better: "up", def: "Days worked in a row, up to the end of the period." },
    longestStreak: { label: "Longest streak", unit: "days", better: null, def: "The most days worked in a row." },
    attendanceRate: { label: "Attendance", unit: "percent", better: "up", def: "Days worked as a share of the team's working days." },
    avgShiftHours: { label: "Average shift", unit: "hours", better: null, def: "Signed-in time on an average finished day worked." },
    medianStart: { label: "Usual start", unit: "clock", better: null, def: "The middle time of the first sign-in on days worked." },
    medianEnd: { label: "Usual finish", unit: "clock", better: null, def: "The middle time of the last sign-out on days worked." },
    issues: { label: "Issues", unit: "count", better: "down", sp: "issuesDay", def: "Signals logged against this person's work: presses of their own, things about the order, and failures the screen showed them. Signals to look at, not verdicts." },
    issuesPer100Orders: { label: "Issues per 100 orders", unit: "count", better: "down", def: "Issues divided by orders, times 100, so busy and quiet periods compare fairly." },
    firstPass: { label: "First-pass rate", unit: "percent", better: "up", def: "Of the orders this person finished at a station, the share with no undo, reopen or reprint by them afterwards." },
    reworkRate: { label: "Rework rate", unit: "percent", better: "down", def: "Undo presses divided by complete presses at the production stations." },
    successRate: { label: "Error-free actions", unit: "percent", better: "up", def: "The share of logged actions that did not end in an error on screen." },
    "rates.failureRate": { label: "Error rate", unit: "percent", better: "down", def: "The share of logged actions that ended in an error on screen. Most errors come from the system, not the person." },
    holdRate: { label: "Hold and cancel rate", unit: "percent", better: null, def: "Refusals, holds, skips, flags and cancelled-order alerts per 100 orders handled." },
    reprintRate: { label: "Reprint rate", unit: "percent", better: "down", def: "Labels printed again divided by all label prints. A jammed printer raises it." },
    rescanRate: { label: "Repeat scan rate", unit: "percent", better: "down", def: "Scans marked 'again' divided by all scans." },
    drafted: { label: "Replies drafted", unit: "count", better: null, def: "Times this person asked the AI to draft or revise a reply." },
    sent: { label: "Replies sent", unit: "count", better: "up", def: "Replies the send queue accepted from this person." },
    delivered: { label: "Replies delivered", unit: "count", better: "up", def: "Replies Etsy confirmed as sent." },
    unconfirmed: { label: "Replies unconfirmed", unit: "count", better: null, def: "Probably sent, but not confirmed. Neither delivered nor failed." },
    failed: { label: "Replies failed", unit: "count", better: "down", def: "Replies that could not be sent." },
    refused: { label: "Replies refused", unit: "count", better: "down", def: "Replies the server turned away before they reached Etsy." },
    edited: { label: "AI drafts edited", unit: "count", better: null, def: "AI drafts this person changed before sending." },
    deliveryRate: { label: "Delivery rate", unit: "percent", better: "up", def: "Delivered divided by replies with a known result." },
    "contact.failureRate": { label: "Failure rate", unit: "percent", better: "down", def: "Failed or refused replies divided by replies with a known result." },
    editedShare: { label: "Drafts edited", unit: "percent", better: null, def: "AI drafts edited divided by all AI drafts sent." },
    medianFirstReplyMs: { label: "First reply (middle)", unit: "ms", better: "down", def: "The middle time from a customer's first message to this person's first reply." },
    meanFirstReplyMs: { label: "First reply (average)", unit: "ms", better: "down", def: "The average time from a customer's first message to this person's first reply." },
    conversationsDone: { label: "Conversations done", unit: "count", better: "up", def: "Conversations this person archived as done." },
    reopened: { label: "Reopened", unit: "count", better: "down", def: "Conversations this person reopened after marking them done." },
    reopenRate: { label: "Reopen rate", unit: "percent", better: "down", def: "Conversations reopened divided by conversations done." },
    weldingHours: { label: "Welding hours", unit: "hours", better: null, def: "Time signed in at the Welding station under the Welding task (welding the studs to the charm). It is signed-in time: welding has no scans, so it is never counted in pieces or orders.", sp: "weldingMs" },
    matchingHours: { label: "Matching hours", unit: "hours", better: null, def: "Time signed in at the Welding station under the Matching task (matching the welded earrings to their orders with the scanner app and adding the backings).", sp: "matchingMs" },
    matchedOrders: { label: "Orders matched", unit: "orders", better: null, def: "Order codes scanned as Matching at the Welding station. Every scan counts, so an order scanned twice counts twice. Not a completion: the Welding station is not counted in pieces or orders.", sp: "matched" },
    unknownHours: { label: "Welding (task not recorded)", unit: "hours", better: null, def: "Time signed in at the Welding station in older sign-ins that did not say which task. It is read as Welding time and nothing stored is changed.", sp: "unknownMs" }
  };
  /** Which cards the page shows: [group, [primary keys], [more keys]], and where each one is looked up (kpis | att | rates | contact | issues). */
  const GROUPS = [
    ["Production", ["kpis.parts", "kpis.orders", "kpis.partsPerActiveHour", "kpis.partsPerDay"], ["kpis.partsPerSignedHour", "kpis.ordersPerDay", "kpis.ordersCompleted", "kpis.scans", "kpis.bestDay", "kpis.peakHour"]],
    ["Welding station", ["welding.weldingHours", "welding.matchingHours", "welding.matchedOrders", "welding.unknownHours"], []],      // (shown only for a person with time or a matched scan at the Welding station)
    ["Speed", ["kpis.secPerOrderMedian", "kpis.secPerOrderP90", "kpis.secPerOrderMean", "kpis.secBetweenScansMedian"], ["kpis.secPerScanMean"]],
    ["Time", ["kpis.activeShare", "kpis.signedHours", "kpis.activeHours", "att.avgShiftHours"], ["kpis.idleHours", "kpis.unloggedHours", "att.medianStart", "att.medianEnd"]],
    ["Attendance", ["att.daysWorked", "att.daysOff", "att.lateDays", "att.shortDays"], ["att.attendanceRate", "att.extraDays", "att.currentStreak", "att.longestStreak"]],
    ["Quality", ["issues.issues", "issues.issuesPer100Orders", "rates.firstPass", "rates.reworkRate"], ["rates.successRate", "rates.failureRate", "rates.holdRate", "rates.reprintRate", "rates.rescanRate"]],
    ["Contact", ["contact.sent", "contact.deliveryRate", "contact.failed", "contact.medianFirstReplyMs"], ["contact.drafted", "contact.delivered", "contact.unconfirmed", "contact.refused", "contact.edited", "contact.editedShare", "contact.failureRate", "contact.conversationsDone", "contact.reopened", "contact.reopenRate", "contact.meanFirstReplyMs"]]
  ];
  const ALIAS = { "att.medianStart": ["att.medianStart", "kpis.avgStart"], "att.medianEnd": ["att.medianEnd", "kpis.avgEnd"], "att.avgShiftHours": ["att.avgShiftHours", "kpis.avgShiftHours"], "kpis.activeShare": ["kpis.activeShare", "rates.activeShare"], "att.attendanceRate": ["att.attendanceRate", "rates.attendanceRate"] };
  /** The card of a figure: its own entry when two groups share a name (rates.failureRate, contact.failureRate), else the short name's. */
  const cardOf = full => CARD[full] || CARD[full.split(".")[1]] || { label: kindWords(full.split(".")[1]), unit: "count", better: null, def: "" };
  /** A server METRIC / RATE (or a bare number) as the page reads it. */
  function metric(key, raw, over, full) {
    const d = CARD[full || key] || CARD[key] || {}, o = raw && typeof raw === "object" ? raw : null, v0 = o ? first(o, "value", "v") : raw;
    const m = { key, label: String((o && o.label) || d.label || kindWords(key)), unit: String((o && o.unit) || d.unit || "count"), v: num(v0), prev: o ? num(first(o, "prev", "previous")) : null, delta: o ? num(o.delta) : null, deltaPct: o ? num(o.deltaPct) : null, better: o && "better" in o ? o.better : d.better != null ? d.better : null,
      def: String((o && first(o, "def", "definition", "text")) || d.def || ""), est: !!(o && first(o, "estimated", "est")), why: o && Array.isArray(o.why) ? o.why.filter(Boolean).map(String).join(" ") : String((o && o.why) || ""), n: o ? num(o.n) : null, window: !!(o && o.window), day: o && o.day ? String(o.day) : "", num: o ? num(first(o, "num", "numerator")) : null, den: o ? num(first(o, "den", "denominator")) : null,
      daysCounted: o ? num(o.daysCounted) : null, daysActive: o ? num(o.daysActive) : null, coverage: o && o.coverage ? String(o.coverage) : "" };
    if (m.delta == null && m.v != null && m.prev != null) m.delta = m.v - m.prev;
    if (m.deltaPct == null && m.v != null && m.prev != null && m.prev !== 0) m.deltaPct = (m.v - m.prev) / Math.abs(m.prev) * 100;
    return Object.assign(m, over || {});
  }
  const kindWords = k => String(k || "").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").toLowerCase().replace(/^./, c => c.toUpperCase());
  const sumK = (list, f) => { let s = null; for (const x of list) { const v = f(x); if (v != null) s = (s || 0) + v; } return s; };
  function normPoint(p) {
    p = p || {};
    return { day: String(p.day || ""), to: isDay(p.to) ? p.to : String(p.day || ""), days: num(p.days) || 1, workedDays: num(p.workedDays), hasData: p.hasData !== false && p.parts !== null, parts: num(p.parts), orders: num(p.orders), scans: num(p.scans), completes: num(p.completes), prints: num(p.prints), rejects: num(p.rejects), errors: num(p.errors), undos: num(p.undos),
      activeMs: num(p.activeMs), idleMs: num(p.idleMs), signedMs: num(p.signedMs), weldingMs: num(p.weldingMs), matchingMs: num(p.matchingMs), unknownMs: num(p.unknownMs), matched: num(p.matched), perActiveHour: num(p.perActiveHour), perSignedHour: num(p.perSignedHour), secPerOrder: num(p.secPerOrder), medianSecPerOrder: num(p.medianSecPerOrder), firstIn: num(p.firstIn), lastOut: num(p.lastOut), shiftMs: num(p.shiftMs), state: p.state ? String(p.state) : "" };
  }
  const CAL = { worked: 1, partial: 1, off: 1, closed: 1, before: 1, future: 1, pending: 1, unknown: 1 };
  /** The `person` answer, in the shapes of plans/employee-hr/api.md; every field is optional. */
  function norm(r, req) {
    r = r || {}; req = req || {};
    const series = A(r.series).filter(p => p && p.day).map(normPoint), hoursRaw = A(r.hours);
    const hours = hoursRaw.length ? Array.from({ length: 24 }, (_, h) => { const x = hoursRaw.find(e => e && +e.hour === h) || hoursRaw[h] || {}; return { hour: h, parts: num(x.parts), scans: num(x.scans), perDay: num(x.perDay) }; }) : [];
    const stations = [];                                                      // (the server already answers with Sorting only; a stored "sorter" or "qr" row in an answer is added to Sorting's, never a row of its own)
    for (const s of A(r.stations).filter(s => s && s.station)) {
      const key = dispSt(String(s.station).toLowerCase()), x = { station: key, label: String(key !== String(s.station).toLowerCase() ? stName(key) : (s.label || stName(key))), parts: num(s.parts), orders: num(s.orders), scans: num(s.scans), completes: num(s.completes), prints: num(s.prints), minutes: num(s.minutes), shareParts: num(s.shareParts), shareMinutes: num(s.shareMinutes), perActiveHour: num(s.perActiveHour), matched: num(s.matched),
        taskMin: s.taskMin && typeof s.taskMin === "object" ? { welding: nz(s.taskMin.welding), matching: nz(s.taskMin.matching), unknown: nz(s.taskMin.unknown) } : null }, had = stations.find(y => y.station === key);
      if (had) { for (const k of ["parts", "orders", "scans", "completes", "prints", "minutes", "shareParts", "shareMinutes"]) had[k] += x[k]; if (x.matched != null) had.matched = (had.matched || 0) + x.matched; } else stations.push(x);
    }
    const cal = A(r.calendar).filter(c => c && c.day).map(c => ({ day: String(c.day), state: CAL[c.state] ? String(c.state) : "before", signedMs: num(c.signedMs), activeMs: num(c.activeMs), firstIn: num(c.firstIn), lastOut: num(c.lastOut), parts: num(c.parts), orders: num(c.orders), late: !!c.late, short: !!c.short, extra: !!c.extra, est: !!c.estimated, lengthKnown: c.lengthKnown !== false, note: c.note ? String(c.note) : "", weekend: !!c.weekend, others: num(c.others), ended: c.endedBy === "idle" || c.endedBy === "closing" ? c.endedBy : "", endedText: (c.endedBy === "idle" || c.endedBy === "closing") && typeof c.endedText === "string" ? c.endedText.slice(0, 120) : "" })).sort((a, b) => (a.day < b.day ? -1 : 1));
    const src = { kpis: {}, att: {}, rates: {}, contact: {}, issues: {}, welding: {} };
    // the Welding station (stations round 2): time signed in per task and the orders matched; null when the person has nothing there in this window. Its points ride on the same buckets as `series`.
    const wr = r.welding && typeof r.welding === "object" ? r.welding : null;
    const welding = wr ? { label: String(wr.label || "Welding station"), def: String(wr.def || ""), matched: num(wr.matched), hours: { welding: num(wr.hours && wr.hours.welding), matching: num(wr.hours && wr.hours.matching), unknown: num(wr.hours && wr.hours.unknown), station: num(wr.hours && wr.hours.station) } } : null;
    if (wr) {
      const wm = wr.metrics && typeof wr.metrics === "object" ? wr.metrics : {};
      for (const k of ["weldingHours", "matchingHours", "matchedOrders"]) if (wm[k] != null) src.welding[k] = metric(k, wm[k], null, "welding." + k);
      if (welding.hours.unknown > 0) src.welding.unknownHours = metric("unknownHours", { value: welding.hours.unknown, unit: "hours" }, { derived: false }, "welding.unknownHours");
      const wp = new Map(A(wr.series).filter(p => p && p.day).map(p => [String(p.day), p]));
      for (const p of series) { const w = wp.get(p.day); if (w) { p.weldingMs = num(w.weldingMs); p.matchingMs = num(w.matchingMs); p.unknownMs = num(w.unknownMs); p.matched = num(w.matched); } }
    }
    for (const k of Object.keys(r.kpis || {})) src.kpis[k] = metric(k, r.kpis[k]);
    // attendance (E9): attendance.metrics = METRICs (workingDays daysWorked daysOff extraDays shortDays lateDays attendanceRate avgShiftHours medianStart medianEnd currentStreak longestStreak)
    const at = r.attendance && typeof r.attendance === "object" ? r.attendance : {}, am = at.metrics && typeof at.metrics === "object" ? at.metrics : {};
    for (const k of Object.keys(am)) src.att[k] = metric(k, am[k], null, "att." + k);
    if (at.streaks && typeof at.streaks === "object") { if (src.att.currentStreak == null && at.streaks.current != null) src.att.currentStreak = metric("currentStreak", at.streaks.current); if (src.att.longestStreak == null && at.streaks.best != null) src.att.longestStreak = metric("longestStreak", at.streaks.best); }
    if (at.avgShiftMs != null && !src.att.avgShiftHours && !src.kpis.avgShiftHours) src.kpis.avgShiftHours = metric("avgShiftHours", num(at.avgShiftMs) == null ? null : at.avgShiftMs / HOUR_MS);
    const defs = at.definitions && typeof at.definitions === "object" ? at.definitions : {}, estF = at.estimated && at.estimated.fields && typeof at.estimated.fields === "object" ? at.estimated.fields : {};
    const FIELD = { avgShiftHours: "avgShiftMs", medianStart: "medianStart", medianEnd: "medianEnd", daysWorked: "daysWorked", daysOff: "daysOff", shortDays: "shortDays", lateDays: "lateDays", workingDays: "workingDays" };
    for (const k of Object.keys(src.att)) { const m = src.att[k], e = estF[FIELD[k] || k]; if (!m.def && typeof defs[k] === "string") m.def = defs[k]; if (e && e.estimated && !m.est) { m.est = true; if (!m.why) m.why = String(e.why || ""); } if (e && e.rule && !m.why) m.rule = String(e.rule); }
    const attNote = typeof defs.notAttendance === "string" ? defs.notAttendance : "";
    // issues (E10): { total, own, system, order, per100Orders, daysCounted, byKind[13], byDay, items[], itemsTotal, next }; a count that is null is a dash (not counted on these days), never 0
    const iraw = r.issues && typeof r.issues === "object" ? r.issues : null, items = A(iraw && iraw.items).filter(i => i && i.kind != null).map(i => ({ at: num(i.at), day: String(i.day || ""), rid: i.rid != null ? String(i.rid) : "", number: i.number != null ? String(i.number) : "", kind: String(i.kind), label: String(i.label || kindWords(i.kind)), station: String(i.station || ""), attribution: String(i.attribution || ""), note: String(i.note || i.reason || "") })).sort((a, b) => nz(b.at) - nz(a.at));
    const byKind = A(iraw && iraw.byKind).filter(k => k && k.kind != null).map(k => ({ kind: String(k.kind), label: String(k.label || kindWords(k.kind)), count: num(k.count), per100: num(k.per100Orders), coverage: String(k.coverage || "range"), attribution: String(k.attribution || ""), def: String(k.def || k.definition || ""), how: String(k.how || ""), est: !!k.estimated, why: Array.isArray(k.why) ? k.why.filter(Boolean).map(String).join(" ") : String(k.why || ""), prev: num(k.prev), delta: num(k.delta), deltaPct: num(k.deltaPct), daysCounted: num(k.daysCounted), daysActive: num(k.daysActive), complete: k.complete !== false, checked: k.checked && k.checked.of != null ? { orders: num(k.checked.orders), of: num(k.checked.of) } : null })).sort((a, b) => nz(b.count) - nz(a.count) || (a.count == null) - (b.count == null));
    const byDay = new Map(A(iraw && iraw.byDay).filter(d => d && d.day).map(d => [String(d.day), { total: num(d.total), own: num(d.own), system: num(d.system), order: num(d.order) }]));
    const issues = iraw ? { total: num(iraw.total), own: num(iraw.own), system: num(iraw.system), order: num(iraw.order), per100: num(first(iraw, "per100Orders", "per100")), daysCounted: num(iraw.daysCounted), daysActive: num(iraw.daysActive), prevTotal: num(iraw.prevTotal), byKind, byDay, items, itemsTotal: num(iraw.itemsTotal), capped: !!iraw.itemsCapped, next: iraw.next || "" } : null;
    if (iraw) { src.issues.issues = metric("issues", iraw.metric || { value: iraw.total, prev: iraw.prevTotal, better: "down", daysCounted: iraw.daysCounted, daysActive: iraw.daysActive, coverage: iraw.complete === false ? "range-partial" : "range" }, null, "issues.issues"); src.issues.issuesPer100Orders = metric("issuesPer100Orders", { value: first(iraw, "per100Orders", "per100"), prev: iraw.prevPer100Orders, better: "down", daysCounted: iraw.daysCounted }, null, "issues.issuesPer100Orders"); }
    // rates (E10): firstPass reworkRate successRate failureRate holdRate reprintRate rescanRate, each a RATE { label, value (percent), numerator, denominator, definition, better, coverage, estimated, why[], daysCounted, prev, delta }
    const rr = r.rates && typeof r.rates === "object" && !Array.isArray(r.rates) ? r.rates : {};
    for (const k of Object.keys(rr)) if (rr[k] != null) src.rates[k] = metric(k, rr[k] && typeof rr[k] === "object" ? Object.assign({ unit: "percent" }, rr[k]) : { value: rr[k], unit: "percent" }, { unit: "percent" }, "rates." + k);
    // contact (E10): contact.metrics = METRICs (drafted sent delivered unconfirmed failed refused edited aiSentUnchanged deliveryRate failureRate editedShare medianFirstReplyMs meanFirstReplyMs conversationsDone reopened reopenRate)
    const cr = r.contact && typeof r.contact === "object" ? r.contact : null, cm = cr && cr.metrics && typeof cr.metrics === "object" ? cr.metrics : {};
    if (cr) {
      for (const k of Object.keys(cm)) if (cm[k] != null) src.contact[k] = metric(k, cm[k], null, "contact." + k);
      for (const k of ["drafted", "sent", "delivered", "unconfirmed", "failed", "refused", "edited", "deliveryRate", "failureRate", "editedShare", "medianFirstReplyMs", "conversationsDone", "reopened", "reopenRate"]) if (!src.contact[k] && cr[k] !== undefined) src.contact[k] = metric(k, cr[k], null, "contact." + k);
    }
    const contact = cr ? { available: cr.available !== false, source: String(cr.source || ""), daysCounted: num(cr.daysCounted), daysActive: num(cr.daysActive), metrics: src.contact } : null;
    // what the days add up to, where the server sent none
    const fill = (ns, k, v) => { if (!src[ns][k]) src[ns][k] = metric(k, v, { derived: true }); else if (src[ns][k].v == null && v != null) { src[ns][k].v = v; src[ns][k].derived = true; } };
    if (series.length || cal.length) {
      if (series.length) { fill("kpis", "parts", sumK(series, p => p.parts)); fill("kpis", "orders", sumK(series, p => p.orders)); fill("kpis", "scans", sumK(series, p => p.scans)); const sg = sumK(series, p => p.signedMs), ac = sumK(series, p => p.activeMs); if (sg != null) fill("kpis", "signedHours", sg / HOUR_MS); if (ac != null) fill("kpis", "activeHours", ac / HOUR_MS); if (sg > 0 && ac != null) fill("kpis", "activeShare", Math.min(100, ac / sg * 100)); }
      if (cal.length) { fill("att", "daysWorked", cal.filter(c => c.state === "worked" || c.state === "partial").length); fill("att", "daysOff", cal.filter(c => c.state === "off").length); }
    }
    const last = cal.filter(c => c.signedMs > 0 || c.lastOut || c.firstIn).pop(), lastSeen = last ? (last.lastOut || last.firstIn) : null;
    return { name: String(r.name || req.name || ""), found: r.found !== false, spellings: A(r.spellings).map(String), mode: String(r.mode || ""), range: r.range, from: isDay(r.from) ? r.from : req.from, to: isDay(r.to) ? r.to : req.to, days: num(r.days), today: isDay(r.today) ? r.today : "", live: !!r.live, prev: r.prev && r.prev.from ? { from: String(r.prev.from), to: String(r.prev.to), days: num(r.prev.days) } : null,
      granularity: r.granularity === "week" ? "week" : "day", trackingStart: isDay(r.trackingStart) ? r.trackingStart : "", firstDay: isDay(r.firstDay) ? r.firstDay : "", eventWindow: r.eventWindow && r.eventWindow.from ? r.eventWindow : null, rules: r.rules || {},
      src, welding, series, hours, stations, cal, att: r.attendance || null, attNote, issues, contact, cannotTell: A(r.cannotTell).filter(c => c && c.text).map(c => ({ topic: String(c.topic || ""), text: String(c.text) })), notes: A(r.notes).map(String).filter(Boolean), partial: !!r.partial, errors: A(r.errors).map(String), now: num(r.now), lastSeen, lastStation: "" };
  }
  /** `personOrders` (E4's shape; E8's list reads it itself, this is the page's own small list until that module is in the page). */
  function normOrders(r) {
    r = r || {};
    const orders = A(r.orders).filter(o => o && (o.rid != null || o.number != null)).map(o => {
      const rid = String(o.rid != null ? o.rid : o.number).replace(/\D/g, "") || String(o.rid != null ? o.rid : o.number), pieces = A(o.pieces).map((p, i) => ({ id: String(p && p.id != null ? p.id : i), label: String((p && (p.label || p.sku)) || `Piece ${i + 1}`), thumb: safeUrl(p && p.thumbUrl) }));
      return { rid, number: String(o.number != null ? o.number : rid), customer: String(o.customer || ""), at: num(o.at), station: String(o.station || ""), durationMs: num(o.durationMs), pieces, count: num(o.piecesCount) || pieces.length, thumb: safeUrl(o.thumbUrl), qr: String((o.qr && o.qr.text) || ""), parts: num(o.parts), flags: A(o.issues).map(i => String(i && (i.label || i.kind) || "")).filter(Boolean).slice(0, 3) };
    });
    return { orders, next: r.next == null || r.next === "" ? "" : String(r.next), total: num(r.total) };
  }
  /** The console's `live` snapshot, reduced to this person: where they are, since when, and the order or orders in their hands. */
  function pickLive(r, name, also) {
    const mine = new Set([slug(name)].concat(A(also).filter(Boolean).map(slug))), isMe = n => mine.has(slug(n)), out = { at: num(r && r.at), where: null, current: [], stationKey: "" };
    if (!r) return out;
    const mineIn = A(r.signedIn).filter(s => s && isMe(s.name)), sig = mineIn[0];
    if (sig) {   // (a person in two tasks at the Welding station has two rows: one place, both tasks)
      const sk = dispSt(String(sig.stationKey || "")), here = mineIn.filter(s => dispSt(String(s.stationKey || "")) === sk), tasks = ["welding", "matching"].filter(t => here.some(s => s.task === t));
      out.where = { name: String(sig.name), stationKey: sk, since: here.reduce((m, s) => (num(s.since) && (m == null || s.since < m) ? s.since : m), null) || num(sig.since), lastSeenAt: here.reduce((m, s) => Math.max(m, num(s.lastSeenAt) || 0), 0) || num(sig.lastSeenAt), tasks, lastInputAt: here.reduce((m, s) => Math.max(m, num(s.lastInputAt) || 0), 0) || null }; out.stationKey = out.where.stationKey;
    }
    const seen = new Set(), add = (c, st) => { if (!c || !isMe(c.person)) return; const id = String(c.rid || c.orderNumber || ""); if (seen.has(id)) return; seen.add(id); out.current.push(Object.assign({}, c, { stationKey: dispSt(String(c.station || (st && st.key) || "")), stationLabel: c.stationLabel || (st && st.label) || stName(c.station || (st && st.key)) })); };
    for (const c of A(r.current)) add(c, null); for (const st of A(r.stations)) for (const c of A(st && st.current)) add(c, st);
    if (!out.stationKey && out.current[0]) out.stationKey = out.current[0].stationKey;
    return out;
  }

  /* ── the buckets the throughput, speed and time charts share: hours for a Day, the server's days or weeks otherwise ── */
  function buckets(M, today, nowMs) {
    const from = M.from, to = M.to, n = diffDays(from, to) + 1;
    if (n === 1) {
      const hrs = M.hours, act = []; hrs.forEach(h => { if (nz(h.parts) > 0 || nz(h.scans) > 0) act.push(h.hour); });
      const nowH = from === today ? nyParts(nowMs || Date.now()).hour : -1; let lo = 7, hi = 18; if (act.length) { lo = Math.min(lo, ...act); hi = Math.max(hi, ...act); } if (nowH >= 0) hi = Math.max(hi, Math.min(23, nowH));
      const items = []; for (let h = lo; h <= hi; h++) { const x = hrs[h] || { hour: h, parts: null, scans: null, perDay: null }; items.push({ key: String(h), label: hourShort(h), title: hourLabel(h) + (h === nowH ? " · now" : ""), parts: x.parts, scans: x.scans, orders: null, future: nowH >= 0 && h > nowH, day: from, pickable: false }); }
      return { kind: "hour", noun: "hour", items, hi: nowH >= 0 ? items.findIndex(i => +i.key === nowH) : -1 };
    }
    const cal = new Map(M.cal.map(c => [c.day, c]));
    if (M.granularity === "week") {
      const items = M.series.map(p => Object.assign({}, p, { key: p.day, label: mdLbl(p.day), title: p.days > 1 ? `${mdLbl(p.day)} – ${mdLbl(p.to)}` : mdLbl(p.day), future: p.day > today, pickable: p.day <= today, span: [p.day, p.to] }));
      return { kind: "week", noun: "week", items, hi: today >= from && today <= to ? items.findIndex(i => i.span[0] <= today && today <= i.span[1]) : -1 };
    }
    const by = new Map(M.series.map(p => [p.day, p])), items = [];
    for (let d = from, i = 0; d <= to && i < 800; d = addDays(d, 1), i++) { const p = by.get(d) || normPoint({ day: d, parts: null }); items.push(Object.assign({}, p, { day: d, key: d, label: n <= 7 ? wdNum.format(dayDate(d)) : (items.length % 5 === 0 || d === to ? mdLbl(d) : ""), title: dayLbl(d), future: d > today, state: p.state || (cal.get(d) || {}).state || "", pickable: d <= today })); }
    return { kind: "day", noun: "day", items, hi: today >= from && today <= to ? items.findIndex(d => d.day === today) : -1 };
  }

  /* ── pictures: a thumbnail per piece, a small QR (the app's own generator, lib/qrcode.min.js), both zoom in place ── */
  const ZOOM = { th: "1.9", big: "1.55", qr: "2" };
  const QRC = new Map();
  function qrData(text) {
    text = String(text || ""); if (!text) return "";
    if (QRC.has(text)) return QRC.get(text);
    let url = "";
    try {
      if (typeof root.QRCode === "function" && doc.body) {
        const holder = doc.createElement("div"); holder.style.cssText = "position:fixed;left:-9999px;top:0;width:0;height:0;overflow:hidden"; doc.body.appendChild(holder);
        try { new root.QRCode(holder, { text, width: 96, height: 96, correctLevel: root.QRCode.CorrectLevel.M }); const cv = holder.querySelector("canvas"), im = holder.querySelector("img"); url = (cv && cv.toDataURL && cv.toDataURL("image/png")) || (im && im.src) || ""; } finally { holder.remove(); }
      }
    } catch (_) { url = ""; }
    QRC.set(text, url); if (QRC.size > 600) QRC.delete(QRC.keys().next().value);
    return url;
  }
  let qrQueue = [], qrBusy = false;
  function qrFill(box, text) {
    if (!text) { box.classList.add("hidden"); return; }
    box.dataset.qr = text; qrQueue.push(box);
    if (!qrBusy) { qrBusy = true; raf(function pump() { let n = 0; while (qrQueue.length && n < 4) { const b = qrQueue.shift(); if (!b.isConnected) continue; const u = qrData(b.dataset.qr); if (u) { const im = el("img"); im.alt = ""; im.src = u; b.replaceChildren(im); } else b.classList.add("hidden"); n++; } if (qrQueue.length) raf(pump); else qrBusy = false; }); }
  }
  const qrBox = (text, label) => { const b = el("span", "efpQr"); b.setAttribute("data-zoom-dot", ZOOM.qr); b.title = label || "Order QR"; b.setAttribute("role", "img"); b.setAttribute("aria-label", label || "Order QR"); qrFill(b, text); return b; };
  function thumb(src, label, cls, n) {
    const b = el("button", "efpTh" + (cls ? " " + cls : "") + (src ? "" : " none")); b.type = "button"; b.tabIndex = -1; b.title = label || ""; b.setAttribute("aria-label", label || "Thumbnail"); b.setAttribute("data-zoom-dot", cls === "big" ? ZOOM.big : ZOOM.th);
    if (src) { const im = el("img"); im.alt = ""; im.loading = "lazy"; im.decoding = "async"; im.draggable = false; im.src = src; im.onerror = () => { b.classList.add("none"); im.remove(); b.textContent = n || ""; }; b.appendChild(im); } else b.textContent = n || "";
    return b;
  }
  /** One order's pictures: the order's own image first, then one thumbnail for each piece of a multi-piece order. */
  function pictures(host, o, max) {
    const pcs = o.pieces || [], multi = pcs.length > 1, cap = max || 7;
    if (o.thumb || !pcs.length || !multi) host.appendChild(thumb(o.thumb || (pcs[0] && pcs[0].thumb) || "", multi || !pcs.length ? "Order image" : (pcs[0] && pcs[0].label) || "Order image", "", ""));
    if (multi) { pcs.slice(0, cap).forEach((p, i) => host.appendChild(thumb(p.thumb, `${p.label || "Piece " + (i + 1)}`, "", String(i + 1)))); if (pcs.length > cap) host.appendChild(el("span", "efpPlus")).textContent = `+${pcs.length - cap} pieces`; }
  }
  /** What the person is doing at this moment, drawn here until the stations board's shared card (EfficiencyStations.orderCard) is in the page. */
  function nowCard(c, now) {
    const card = el("div", "efpCard efpNowCard"), info = el("div", "efpNowInfo"), top = el("div", "efpNowTop"), pic = el("div", "efpPieces");
    const rid = String(c.rid || c.orderId || "").replace(/\D/g, ""), pcs = A(c.pieces).map((p, i) => ({ id: String(p.id != null ? p.id : i), label: String(p.label || `Piece ${i + 1}`), thumb: safeUrl(p.thumbUrl || p.thumb) }));
    top.appendChild(el("span", "efpNowSt")).textContent = c.stationLabel || stName(c.stationKey);
    if (rid) { const b = el("button", "efpOid"); b.type = "button"; b.dataset.order = rid; b.title = "Open this order"; b.textContent = String(c.orderNumber || rid); top.appendChild(b); }
    if (c.customer) top.appendChild(el("span", "efpNowCust")).textContent = c.customer;
    const since = el("div", "efpNowSince"); since.innerHTML = `Scanned <b data-since="${num(c.scannedAt) || ""}"></b> ago`;
    info.append(top, since); if (c.note) info.appendChild(el("div", "efpNowSince")).textContent = String(c.note);
    const big = thumb(safeUrl(c.thumbUrl), "Order image", "big", ""); pictures(pic, { thumb: "", pieces: pcs }, 8); if (pcs.length > 1) info.appendChild(pic);
    card.append(big, info); if (c.qr && (c.qr.text || typeof c.qr === "string")) card.appendChild(qrBox(String(c.qr.text || c.qr), "Order QR"));
    tickSince(card, now); return card;
  }
  function tickSince(scope, now) {
    const a = root.Efficiency && root.Efficiency.api, f = a && a.fmt && typeof a.fmt.since === "function" ? a.fmt.since : t => durMs(t).replace("<1 s", "0 s").replace(/^0 min$/, "0 s");
    scope.querySelectorAll("[data-since]").forEach(b => { const t = num(b.dataset.since); setText(b, t ? f(Math.max(0, now - t)) : "—"); });
  }

  /* the page opens an order the way every other list does: the sorter's own function, never a pop-up over a pop-up */
  function openOrder(btn, id) {
    id = String(id || "").replace(/\D/g, ""); if (!id) return;
    try { if (root.Seal && root.Seal.zoom && root.Seal.zoom.away) root.Seal.zoom.away(); } catch (_) {}
    try { const a = root.Efficiency && root.Efficiency.api; if (a && typeof a.openOrder === "function") a.openOrder(btn, id); else if (typeof root.openOrderFrom === "function") root.openOrderFrom(btn, id); else if (root.OrderWin && root.OrderWin.openOrder) root.OrderWin.openOrder(id, { from: btn }); } catch (e) { console.warn("[efficiency person] order not opened:", e && e.message); }
  }
  const endpoint = () => root.location.origin + "/.netlify/functions/employeeEfficiency";
  function failure(status, j, net) {
    const srv = j && j.error ? String(j.error).replace(/\s+/g, " ").slice(0, 100) : "";
    let msg, short = "Reconnecting…";
    if (net) msg = "The service cannot be reached from here. Check the connection.";
    else if (status === 401) msg = "That passcode was not accepted.";
    else if (status === 403) msg = "No manager passcode is set up yet.";
    else if (status === 429) { msg = "Too many requests from this address. Waiting a minute."; short = "Paused · too many requests"; }
    else if (status === 404 || status === 405) { msg = `The efficiency service is not available (${status}).`; short = `Service not available (${status})`; }
    else if (status === 400 || status === 413) { msg = `The request was refused${srv ? `: ${srv}` : ""}.`; short = "Request refused"; }
    else if (status >= 500) msg = `The data could not be read just now${srv ? ` (${srv})` : ""}.`;
    else msg = srv ? `${srv}.` : `The service answered ${status || "with an error"}.`;
    return Object.assign(new Error(msg), { status: status || 0, auth: status === 401 || status === 403, limited: status === 429, short });
  }

  /** A small floating read-out for cells (heat, calendar, donut): one per card, placed over the cell, never off the card. */
  function miniTip(host) {
    host.classList.add("efpXY"); const tip = el("div", "efpTip"); host.appendChild(tip);
    return {
      show(target, t) {
        tip.textContent = ""; tip.appendChild(el("div", "efpTipT")).textContent = t.t; if (t.v) tip.appendChild(el("div", "efpTipV")).textContent = t.v;
        for (const [k, v] of t.rows || []) { const r = tip.appendChild(el("div", "efpTipR")); r.appendChild(el("span")).textContent = k; r.appendChild(el("b")).textContent = v; }
        if (t.hint) tip.appendChild(el("div", "efpTipH")).textContent = t.hint;
        tip.classList.add("on");
        const hr = host.getBoundingClientRect(), tr = target.getBoundingClientRect(), w = tip.offsetWidth, h = tip.offsetHeight;
        const x = tr.left - hr.left + tr.width / 2 - w / 2; let y = tr.top - hr.top - h - 8; if (y < -6) y = tr.bottom - hr.top + 8;
        tip.style.left = Math.max(0, Math.min(hr.width - w, x)) + "px"; tip.style.top = y + "px";
      },
      hide() { tip.classList.remove("on"); }
    };
  }


  /* ── how a metric reads ── */
  const clockMin = m => { m = Math.round(num(m) || 0) % 1440; const h = Math.floor(m / 60); return `${h % 12 || 12}:${pad(m % 60)} ${h < 12 ? "AM" : "PM"}`; };
  const pctVal = v => (v >= 10 ? `${Math.round(v)}%` : `${Math.round(v * 10) / 10}%`);
  /** A group of cards is ONE Tab stop; the arrow keys, Home and End move inside it (a keyboard user does not pass sixteen stops before the first chart). */
  function roveFix(box, sel) { const all = [].slice.call(box.querySelectorAll(sel)), vis = all.filter(n => !n.classList.contains("hidden")), cur = vis.find(n => n.tabIndex === 0) || vis[0]; all.forEach(n => { n.tabIndex = n === cur ? 0 : -1; }); }
  function roveInit(box, sel) {
    roveFix(box, sel);
    box.addEventListener("focusin", e => { const n = e.target && e.target.closest ? e.target.closest(sel) : null; if (n && box.contains(n)) [].slice.call(box.querySelectorAll(sel)).forEach(x => { x.tabIndex = x === n ? 0 : -1; }); });
    box.addEventListener("keydown", e => {
      const n = e.target; if (!n || !n.matches || !n.matches(sel)) return;
      const l = [].slice.call(box.querySelectorAll(sel)).filter(x => !x.classList.contains("hidden")), i = l.indexOf(n); let j = -1;
      if (e.key === "ArrowRight" || e.key === "ArrowDown") j = Math.min(l.length - 1, i + 1); else if (e.key === "ArrowLeft" || e.key === "ArrowUp") j = Math.max(0, i - 1); else if (e.key === "Home") j = 0; else if (e.key === "End") j = l.length - 1; else return;
      e.preventDefault(); if (l[j]) l[j].focus();
    });
  }
  function fmtFor(key, unit) {
    if (key === "messagesPerCustomer" || key === "repliesPerDay") return nf1;   // (the two inbox averages keep their decimal)
    if (key === "timeToFirstReplyMin" || unit === "minutes") return v => durMs(v * 60000);
    if (key === "peakHour") return v => (unit === "clock" ? clockMin(v) : hourLabel(Math.round(v) % 24));
    if (key === "issuesPer100Orders") return nf1;
    switch (unit) {
      case "ms": return v => durMs(v); case "hours": return v => hoursTxt(v * HOUR_MS); case "seconds": return secTxt; case "percent": return pctVal; case "clock": return clockMin;
      case "pieces/hour": case "orders/hour": return rateTxt; case "pieces/day": case "orders/day": return nf1; case "hour": return v => hourLabel(Math.round(v) % 24);
      default: return nf;
    }
  }
  const nameOf = (m, d) => String((m && m.label) || d.label || "").replace(/\bseconds\b/i, "time");
  const metricAt = (M, full) => { for (const name of ALIAS[full] || [full]) { const [ns, k] = name.split("."); const m = M.src[ns] && M.src[ns][k]; if (m) return m; } return null; };
  const shortKey = full => full.split(".")[1];

  /* ════════════ INBOX (IN2, Paul 6 Oct 2026, R8): everything this person SENT from the inbox, in the same date chips ════════════
   *  A clearly separate "Inbox" section of this page: replies sent, the orders they cover, distinct customers, total messages, messages per customer
   *  (average and a top list), a per-day chart (per hour for a Day) and the list of orders covered. Read from op personInbox (plans/stations-round2/api.md,
   *  section IN2; the same window and Real | Sandbox as op person). The figures are the page's own cards (efpK), the charts EfficiencyCharts, the order list
   *  EfficiencyOrders (station "inbox"): the very same components as every other section. A figure the server does not know is a dash, never a zero. */
  options.inboxOp = "personInbox"; options.inboxMs = 30000; options.inboxTop = 10; options.inboxShow = 5;
  const IN_GROUP = ["Inbox", ["inbox.replies", "inbox.orders", "inbox.customers", "inbox.messages"], ["inbox.messagesPerCustomer", "inbox.maxPerCustomer", "inbox.repliesPerDay", "inbox.daysActive"]];
  const IN_KEYS = IN_GROUP[1].concat(IN_GROUP[2]).map(k => k.slice(6));
  Object.assign(CARD, {
    "inbox.replies": { label: "Replies sent", unit: "count", better: null, sp: "replies", def: "Replies this person sent from the inbox in the period. Only what a person actually sent: an AI draft nobody sent is not counted." },
    "inbox.orders": { label: "Orders covered", unit: "orders", better: null, sp: "orders", def: "Different orders those replies were about. Several replies on one order count once." },
    "inbox.customers": { label: "Customers", unit: "count", better: null, sp: "customers", def: "Different customers who got a reply from this person in the period." },
    "inbox.messages": { label: "Messages sent", unit: "count", better: null, sp: "messages", def: "Messages sent to customers. A reply that goes out as several messages counts each one." },
    "inbox.messagesPerCustomer": { label: "Messages per customer", unit: "count", better: null, def: "Messages sent divided by customers: how many messages the average customer got in the period." },
    "inbox.maxPerCustomer": { label: "Most to one customer", unit: "count", better: null, def: "The most messages any one customer got from this person in the period." },
    "inbox.repliesPerDay": { label: "Replies per day with replies", unit: "count", better: null, def: "Replies sent divided by the days on which at least one reply was sent." },
    "inbox.daysActive": { label: "Days with replies", unit: "days", better: null, def: "Days on which this person sent at least one reply from the inbox." }
  });
  const IN_MEASURE = { replies: ["Replies", "reply", "replies"], messages: ["Messages", "message", "messages"], orders: ["Orders", "order", "orders"], customers: ["Customers", "customer", "customers"] };
  /** The `personInbox` answer, in the shape of plans/stations-round2/api.md (IN2): every field is optional, a missing one is null (a dash). */
  function normInbox(r, req) {
    r = r && typeof r === "object" ? r : {}; req = req || {};
    const tot = r.totals && typeof r.totals === "object" ? r.totals : r.kpis && typeof r.kpis === "object" ? r.kpis : {}, src = {};
    for (const k of IN_KEYS) if (tot[k] != null) src[k] = metric(k, tot[k], null, "inbox." + k);
    const series = A(r.series).filter(p => p && p.day).map(p => ({ day: String(p.day), to: isDay(p.to) ? p.to : String(p.day), days: num(p.days) || 1, replies: num(p.replies), orders: num(p.orders), customers: num(p.customers), messages: num(p.messages) }));
    const hr = A(r.hours), hours = hr.length ? Array.from({ length: 24 }, (_, h) => { const x = hr.find(e => e && +e.hour === h) || {}; return { hour: h, replies: num(x.replies), messages: num(x.messages) }; }) : [];
    const pc = r.perCustomer && typeof r.perCustomer === "object" ? r.perCustomer : null;
    const per = pc ? { average: num(pc.average), median: num(pc.median), max: num(pc.max), total: num(pc.total),
      dist: A(pc.distribution).filter(d => d && d.customers != null).map(d => ({ messages: num(d.messages), customers: num(d.customers), plus: !!d.plus })),
      top: A(pc.top).filter(t => t && (t.customer != null || t.rid != null)).map(t => ({ customer: String(t.customer || ""), messages: num(t.messages), replies: num(t.replies), orders: num(t.orders), lastAt: num(t.lastAt), rid: t.rid != null ? String(t.rid).replace(/\D/g, "") : "" })) } : null;
    const derive = (k, v) => { if (!src[k] && v != null) src[k] = metric(k, v, { derived: true }, "inbox." + k); };
    if (series.some(p => p.replies != null)) derive("replies", sumK(series, p => p.replies));
    if (series.some(p => p.messages != null)) derive("messages", sumK(series, p => p.messages));
    if (per) { derive("messagesPerCustomer", per.average); derive("maxPerCustomer", per.max); }
    const un = r.unknown && typeof r.unknown === "object" ? { replies: num(r.unknown.replies), messages: num(r.unknown.messages) } : null;
    return { found: r.found !== false, name: String(r.name || req.name || ""), from: isDay(r.from) ? r.from : req.from || "", to: isDay(r.to) ? r.to : req.to || "", days: num(r.days), today: isDay(r.today) ? r.today : "", live: !!r.live, prev: r.prev && r.prev.from ? { from: String(r.prev.from), to: String(r.prev.to), days: num(r.prev.days) } : null,
      granularity: r.granularity === "week" ? "week" : "day", knownFrom: isDay(r.knownFrom) ? r.knownFrom : "", src, series, hours, per, unknown: un && (un.replies || un.messages) ? un : null, notes: A(r.notes).map(String).filter(Boolean), partial: !!r.partial, errors: A(r.errors).map(String), now: num(r.now) };
  }

  /* ════════════ the page ════════════ */
  function mount(host, o) {
    o = o || {}; if (!host || !doc) return null; style();
    const EA = () => (root.Efficiency && root.Efficiency.api) || null;
    const now = () => (typeof o.now === "function" ? o.now() : EA() && typeof EA().now === "function" ? EA().now() : Date.now() + S.off), today = () => nyDay(now());
    const S = { name: String(o.name || ""), range: "week", anchor: null, custom: null, following: true, metric: "parts", M: null, cache: new Map(), gen: 0, busy: false, err: "", errShort: "", fails: 0, at: 0, off: 0, ctl: null, locked: false, dead: false,
      live: null, liveAt: 0, liveFails: 0, liveBusy: false, calx: null, calBusy: false, ordersAll: false, ordersSig: "", now: new Map(), openKinds: new Set(), kinds: false, moreOpen: new Set(), calW: 0,
      orders: { q: "", list: [], seen: new Set(), next: "", busy: false, gen: 0, err: "", total: null, done: false, loaded: false, typing: false, reset: false, at: 0 } };
    const want = o.range || store.get(RANGE_STORE) || "week"; S.range = RANGES.some(r => r[0] === want && r[0] !== "custom") || want === "custom" && o.custom ? want : "week";
    S.anchor = isDay(o.day) ? o.day : today(); S.custom = o.custom || null; S.following = S.anchor >= today();
    const T = { range: 0, live: 0, cal: 0, orders: 0, tick: 0, deb: 0 };
    const root0 = el("div", "efp in"); root0.setAttribute("aria-label", `${S.name}, employee page`);
    host.textContent = ""; host.appendChild(root0);
    const E = {}; let charts = {}, mini = {}, io = null, unsubLive = null, ordersH = null, matchedH = null;
    let laserH = null;
    /** One chart of E7's EfficiencyCharts in `host` (null, with a quiet line, when the library is not in the page). */
    function chart(kind, host, opts) {
      const lib = root.EfficiencyCharts; host.textContent = "";
      if (lib && typeof lib[kind] === "function") { try { return lib[kind](host, opts || {}); } catch (e) { console.warn("[efficiency person] chart " + kind + ":", e && e.message); } }
      host.appendChild(el("div", "efpEmptyBox")).textContent = "Charts are not available on this page."; return null;
    }

    /* ── the wire: the console's api (passcode and Real | Sandbox added by the shell), or this tab's own passcode ── */
    async function call(body, signal) {
      if (typeof o.call === "function") return o.call(body, signal);
      const a = EA(); if (a && typeof a.call === "function") return a.call(body, { signal });
      const key = store.get(KEY_STORE); if (!key) throw Object.assign(failure(401, null), { message: "The manager passcode is not set in this tab." });
      const payload = Object.assign({}, body, { key }), m = typeof o.mode === "function" ? o.mode() : o.mode;
      if (m === "sandbox") payload.sandbox = true;
      let res; try { res = await fetch(endpoint(), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload), cache: "no-store", signal }); } catch (e) { if (e && e.name === "AbortError") throw e; throw failure(0, null, true); }
      const txt = await res.text(); let j = null; try { j = JSON.parse(txt); } catch (_) {}
      if (!res.ok || !j || j.ok === false) throw failure(res.status, j);
      return j;
    }
    const isAuth = e => !!(e && (e.auth || e.locked));
    const period = () => periodOf(S.range, S.anchor, S.custom);
    const reqKey = () => { const p = period(); return `${S.range}|${p.from}|${p.to}`; };
    const request = () => { const p = period(); return { op: "person", name: S.name, range: S.range === "custom" ? { from: p.from, to: p.to } : S.range, day: S.range === "custom" ? p.to : S.anchor, compare: true }; };
    const visible = () => !S.dead && !S.locked && host.isConnected && doc.visibilityState !== "hidden" && host.getClientRects().length > 0;
    const POLL = { range: () => fetchRange(S.gen, true), live: () => pollLive(), cal: () => pollCal(), orders: () => pollOrders() };
    function schedule(kind, ms) { clearTimeout(T[kind]); T[kind] = 0; if (visible()) T[kind] = setTimeout(POLL[kind], ms); }
    const backoff = n => Math.min(options.maxBackoffMs, 3000 * Math.pow(2, Math.max(0, n - 1)));
    function lockOut(e) {
      S.locked = true; for (const k of ["range", "live", "cal", "orders"]) { clearTimeout(T[k]); T[k] = 0; }
      E.msg.className = "efpMsg bad"; E.msg.textContent = ""; const sp = el("span"); sp.textContent = "The manager passcode is needed. Go back to Employee efficiency and enter it, then open this page again."; E.msg.appendChild(sp);
      E.wait.classList.add("hidden"); paintLive(); try { o.onAuth && o.onAuth(e); } catch (_) {}
    }

    /* ── INBOX (IN2): the Inbox section. Its figures are the page's own cards (E.k["inbox.*"], drawn by renderKpis from M.src.inbox), the chart is
          EfficiencyCharts.bars, the list is EfficiencyOrders (station "inbox"). Its own read (op personInbox) follows the date chips; a service
          that does not know the op says so quietly and nothing else on the page changes. ── */
    S.inb = { topAll: false, r: null, key: "", asked: "", busy: false, err: "", unsupported: false, fails: 0, gen: 0, at: 0, cache: new Map(), metric: "replies", ordersSig: "", drawn: false };
    T.inbox = 0; T.inTick = 0; POLL.inbox = () => fetchInbox(true);
    let inOrdersH = null;
    const inRange = () => { const p = period(); return { from: p.from, to: p.to }; };
    const inboxMetrics = () => (S.inb.r ? S.inb.r.src : {});
    function inboxStyle() {
      if (doc.getElementById("efpInStyle")) return;
      const s = doc.createElement("style"); s.id = "efpInStyle";
      s.textContent = `
.efpIn{display:grid;gap:12px;min-width:0;align-content:start}
.efpIn .efpLabel{margin-bottom:0}
.efpInN:empty{display:none}.efpInOL{margin-top:6px}.efpInOn:empty{display:none}
.efpInBusy{order:3;display:inline-flex;align-items:center;gap:7px;color:var(--ink45);font-size:11.5px;letter-spacing:0;text-transform:none;font-weight:500;opacity:0;transition:opacity .2s;min-width:0}.efpInBusy.on{opacity:1}
.efpInBody{display:grid;gap:12px;min-width:0;transition:opacity .25s ease}.efpInBody.dim{opacity:.5}
.efpInMsg{justify-content:space-between;flex-wrap:wrap}.efpInMsg .btn,.efpInMsg button{margin-left:auto}
.efpInMsg button{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:3px 12px;font:650 11.5px var(--sans);color:var(--ink70);cursor:pointer}.efpInMsg button:hover{background:var(--paper2);color:var(--ink)}
.efpInDist{min-width:0}.efpInDist:empty{display:none}
.efpInTop{display:grid;gap:1px;min-width:0;position:relative}
.efpTR{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:3px 12px;align-items:baseline;width:100%;border:0;background:transparent;border-radius:10px;padding:7px 10px;margin:0;text-align:left;color:var(--ink);font-family:inherit;cursor:default;transition:background .2s}
.efpTR[data-order]{cursor:pointer}.efpTR:hover,.efpTR:focus-visible{background:var(--card2)}.efpTR:focus-visible{outline:2px solid var(--gold);outline-offset:-2px}
.efpTRn{font-weight:650;font-size:12.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.efpTRv{font:700 13px var(--sans);font-variant-numeric:tabular-nums;white-space:nowrap}.efpTRv small{font-weight:500;color:var(--ink45);font-size:11px;margin-left:3px}
.efpTRm{grid-column:1/-1;color:var(--ink45);font-size:11px;font-variant-numeric:tabular-nums;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.efpTRb{grid-column:1/-1;display:block;height:5px;border-radius:3px;background:var(--line2);overflow:hidden}.efpTRb i{display:block;height:100%;width:0;background:var(--gold2);border-radius:3px;transition:width .55s cubic-bezier(.2,.8,.2,1)}
.efpInNone{padding:14px 10px;color:var(--ink45);font-size:12.5px}
.efpInMoreB{justify-self:start;margin:4px 0 0 8px;border:0;background:transparent;color:var(--gold);font:700 11.5px var(--sans);padding:4px 6px;border-radius:6px;cursor:pointer}.efpInMoreB:hover{background:var(--goldSoft)}.efpInMoreB:focus-visible{outline:2px solid var(--gold);outline-offset:1px}
.efpInOrders{padding:2px 2px 4px;min-width:0}.efpInOrdersMod{display:block}.efpInOrders.hidden{display:none}
.efpInChartNote{color:var(--ink45);font-size:11px}
.efpLabel .efpInLr{order:2;letter-spacing:0;text-transform:none;font-weight:500;font-size:11.5px}
.efpIn .efpInFoot{display:grid;gap:3px;margin:0 2px;color:var(--ink45);font-size:11.5px;line-height:1.5}`;
      doc.head.appendChild(s);
    }
    /** The inbox section: its figure cards (the very same card as every other group), charts, list, and the section's own listeners. */
    function inboxBuild(q) {
      inboxStyle();
      const sec = q(".efpIn"); if (!sec) return;
      E.in = { sec, n: q(".efpInN"), lr: q(".efpInLr"), busy: q(".efpInBusy"), busyT: q(".efpInBusyT"), msg: q(".efpInMsg"), body: q(".efpInBody"), kp: q(".efpInK"), cp: q('[data-c="inb"] .efpCP'), chH: q('[data-c="inb"] .efpCh'), card: q('[data-c="inb"]'), topCP: q('[data-c="inbTop"] .efpCP'),
        distH: q(".efpInDist"), top: q(".efpInTop"), topL: q(".efpInTopL"), orders: q(".efpInOrders"), ordersN: q(".efpInOn"), foot: q(".efpInFoot") };
      const [gname, prim, more] = IN_GROUP;
      for (const full of prim.concat(more)) {
        const key = shortKey(full), d = cardOf(full), c = el("div", "efpK", `<span class="efpKL"><span class="t"></span></span><span class="efpKR"><b class="efpKV">—</b><span class="efpKsp"></span></span><span class="efpKD"></span><span class="efpKF"><span class="efpKS"></span><span class="efpKT"></span></span>`);
        c.tabIndex = -1; c.dataset.k = full; c.setAttribute("role", "group"); setText(c.querySelector(".t"), d.label); if (more.includes(full)) c.classList.add("hidden", "extra");
        E.k[full] = { card: c, val: c.querySelector(".efpKV"), d: c.querySelector(".efpKD"), s: c.querySelector(".efpKS"), tag: c.querySelector(".efpKT"), sp: null, spH: c.querySelector(".efpKsp"), t: c.querySelector(".t"), group: gname };
        c.addEventListener("pointerenter", () => showHC(full)); c.addEventListener("pointerleave", hideHC); c.addEventListener("focus", () => showHC(full)); c.addEventListener("blur", hideHC);
        E.in.kp.appendChild(c);
      }
      roveInit(E.in.kp, ".efpK");
      charts.inb = chart("bars", E.in.chH, { height: 262, name: "Replies sent", emptyText: "No replies in this range" });
      charts.inDist = chart("bars", E.in.distH, { height: 96, name: "Customers by messages received", emptyText: "" });
      mini.top = miniTip(E.in.top);
      sec.addEventListener("click", e => {
        const b = e.target.closest && e.target.closest("button"); if (!b || !sec.contains(b)) return;
        if (b.dataset.inmetric) { S.inb.metric = b.dataset.inmetric; renderInbox(S.M, S.M && buckets(S.M, today(), now())); return; }
        if (b.hasAttribute("data-inretry")) { S.inb.fails = 0; fetchInbox(false); }
        if (b.hasAttribute("data-intopmore")) { S.inb.topAll = !S.inb.topAll; E.in.top._sig = ""; paintTop(S.inb.r); }
      });
      const EO = root.EfficiencyOrders;
      if (EO && typeof EO.mount === "function") {
        try {
          const host1 = el("div", "efpInOrdersMod"), r0 = inRange(); E.in.orders.appendChild(host1);
          inOrdersH = EO.mount(host1, { name: S.name, station: "inbox", range: r0, filters: false, dateFields: false, moreButton: true, pollMs: 15000, onOpen: (rid, btn) => openOrder(btn || host1, rid) }); S.inb.ordersSig = JSON.stringify(r0);
          if (!inOrdersH) host1.remove();
        } catch (e) { console.warn("[efficiency person] inbox order list:", e && e.message); inOrdersH = null; }
      }
      E.in.orders.classList.toggle("hidden", !inOrdersH); E.in.sec.querySelector(".efpInOL").classList.toggle("hidden", !inOrdersH);
      T.inTick = setInterval(() => { if (S.dead || S.locked) return; if (visible() && !T.inbox && !S.inb.busy) schedule("inbox", 200); }, 1000);
    }
    function syncInOrders() {
      if (!inOrdersH) return; const r = inRange(), sig = JSON.stringify(r); if (sig === S.inb.ordersSig) return; S.inb.ordersSig = sig;
      if (typeof inOrdersH.setRange === "function") { try { inOrdersH.setRange(r); } catch (e) { console.warn("[efficiency person] inbox order list range:", e && e.message); } }
    }
    function inboxEnd() { if (inOrdersH) { try { (inOrdersH.unmount || inOrdersH.destroy).call(inOrdersH); } catch (_) {} inOrdersH = null; } }
    const inReq = () => { const p = period(); return { op: options.inboxOp, name: S.name, range: S.range === "custom" ? { from: p.from, to: p.to } : S.range, day: S.range === "custom" ? p.to : S.anchor, compare: true, top: options.inboxTop }; };
    /** The window changed (or the page opened): a stored answer is shown at once and made fresh; otherwise the old figures stay, dimmed, until the new ones arrive. */
    function inboxGo() {
      if (!E.in) return; const I = S.inb, key = reqKey(), hit = I.cache.get(key);
      syncInOrders();
      if (hit) { I.r = hit.r; I.key = key; I.err = ""; applyInbox(); } else { E.in.body.classList.add("dim"); paintInbox(); }
      if (!hit || Date.now() - hit.at > 4000) fetchInbox(false); else schedule("inbox", options.inboxMs);
    }
    async function fetchInbox(poll) {
      clearTimeout(T.inbox); T.inbox = 0; const I = S.inb; if (S.dead || S.locked || !E.in) return; if (poll && (!visible() || I.busy)) return;
      if (I.busy && !poll) { I.gen++; }   // (a newer window: the older answer is dropped when it lands)
      const key = reqKey(), gen = ++I.gen; I.busy = true; I.asked = key; paintInbox();
      try {
        const r = await call(inReq()); if (gen !== I.gen || S.dead) return;
        const p = period(), R = normInbox(r, { name: S.name, from: p.from, to: p.to });
        I.cache.set(key, { r: R, at: Date.now() }); while (I.cache.size > options.cacheMax) I.cache.delete(I.cache.keys().next().value);
        I.r = R; I.key = key; I.err = ""; I.unsupported = false; I.fails = 0; I.at = Date.now(); applyInbox();
      } catch (e) {
        if (gen !== I.gen || S.dead || (e && e.name === "AbortError")) return;
        if (isAuth(e)) { lockOut(e); return; }
        if ((e && (e.status === 404 || e.status === 405)) || (e && e.status === 400 && /unknown op/i.test(String(e.serverError || e.message)))) { I.unsupported = true; I.err = ""; } else { I.err = String((e && e.message) || e).slice(0, 160); I.fails++; }
      } finally { if (gen === I.gen) { I.busy = false; paintInbox(); if (!S.locked && !S.dead) schedule("inbox", I.unsupported ? options.inboxMs * 4 : I.fails ? backoff(I.fails) : period().to >= today() ? options.inboxMs : options.inboxMs * 4); } }
    }
    /** New inbox figures: they ride in M.src like every other figure, so the cards, the hover card and the count-up are the page's own. */
    function applyInbox() {
      const M = S.M, I = S.inb; paintInbox(); if (!M) return;
      M.src.inbox = inboxMetrics(); const B = buckets(M, today(), now()), first = !I.drawn; I.drawn = true;
      renderKpis(M, B, first); renderInbox(M, B);
    }
    /** What a card under the figures says in one plain line. */
    function inboxSub(full, m) {
      const R = S.inb.r; if (!R || m.v == null) return ""; const v = k => (R.src[k] ? R.src[k].v : null);
      switch (shortKey(full)) {
        case "replies": return v("daysActive") > 0 ? `${nf1(m.v / v("daysActive"))} per day with replies` : "";
        case "orders": { if (v("replies") == null || !(m.v > 0)) return ""; const x = Math.round(v("replies") / m.v * 10) / 10; return `${nf1(x)} ${x === 1 ? "reply" : "replies"} per order`; }
        case "customers": { if (v("messages") == null || !(m.v > 0)) return ""; const x = Math.round(v("messages") / m.v * 10) / 10; return `${nf1(x)} ${x === 1 ? "message" : "messages"} each`; }
        case "messages": return v("replies") != null && v("replies") > 0 && v("replies") !== m.v ? `${nf1(m.v / v("replies"))} per reply` : "";
        case "messagesPerCustomer": return R.per && R.per.median != null ? `middle customer ${nf1(R.per.median)}` : "";
        case "maxPerCustomer": { const t = R.per && R.per.top[0]; return t && t.customer ? t.customer : ""; }
        case "daysActive": { const n = R.from && R.to ? diffDays(R.from, R.to) + 1 : null; return n ? `of ${nf(n)} ${n === 1 ? "day" : "days"}` : ""; }
        default: return "";
      }
    }
    /** The little trend line of a card: the inbox's own series on the page's buckets (hours for a Day). */
    function inboxSpark(full, B) {
      const R = S.inb.r, sp = cardOf(full).sp; if (!R || !sp) return [];
      if (B.kind === "hour") return (sp === "replies" || sp === "messages") && R.hours.length ? B.items.map(x => (x.future ? null : num((R.hours[+x.key] || {})[sp]))) : [];
      const by = new Map(R.series.map(p => [p.day, p])); return B.items.map(x => { if (x.future) return null; const p = by.get(x.key); return p ? p[sp] : null; });
    }
    /** The chart's buckets: hours for a Day, else the server's days (every day of the window, a gap where nothing is known) or weeks. */
    function inBuckets(R) {
      const td = today(), from = R.from, to = R.to, n = diffDays(from, to) + 1;
      if (n === 1) {
        const hs = R.hours, act = []; hs.forEach(h => { if (nz(h.replies) > 0 || nz(h.messages) > 0) act.push(h.hour); });
        const nowH = from === td ? nyParts(now()).hour : -1; let lo = 7, hi = 18; if (act.length) { lo = Math.min(lo, ...act); hi = Math.max(hi, ...act); } if (nowH >= 0) hi = Math.max(hi, Math.min(23, nowH));
        const items = []; for (let h = lo; h <= hi; h++) { const x = hs[h] || { replies: null, messages: null }; items.push({ key: String(h), label: hourShort(h), title: hourLabel(h) + (h === nowH ? " · now" : ""), replies: x.replies, messages: x.messages, orders: null, customers: null, future: nowH >= 0 && h > nowH, pickable: false }); }
        return { kind: "hour", noun: "hour", items, hi: nowH >= 0 ? items.findIndex(i => +i.key === nowH) : -1 };
      }
      if (R.granularity === "week") {
        const items = R.series.map(p => Object.assign({}, p, { key: p.day, label: mdLbl(p.day), title: p.days > 1 ? `${mdLbl(p.day)} – ${mdLbl(p.to)}` : mdLbl(p.day), future: p.day > td, pickable: p.day <= td, span: [p.day, p.to] }));
        return { kind: "week", noun: "week", items, hi: td >= from && td <= to ? items.findIndex(i => i.span[0] <= td && td <= i.span[1]) : -1 };
      }
      const by = new Map(R.series.map(p => [p.day, p])), items = [];
      for (let d = from, i = 0; d <= to && i < 800; d = addDays(d, 1), i++) { const p = by.get(d) || { replies: null, orders: null, customers: null, messages: null }; items.push(Object.assign({}, p, { day: d, key: d, label: n <= 7 ? wdNum.format(dayDate(d)) : (items.length % 5 === 0 || d === to ? mdLbl(d) : ""), title: dayLbl(d), future: d > td, pickable: d <= td })); }
      return { kind: "day", noun: "day", items, hi: td >= from && td <= to ? items.findIndex(x => x.day === td) : -1 };
    }
    const inPlural = (k, v) => (v === 1 ? IN_MEASURE[k][1] : IN_MEASURE[k][2]);
    /** The status line of the section: reading, not available yet, could not be read; the quiet notes (what is known and what is not). */
    function paintInbox() {
      const e = E.in; if (!e) return; const I = S.inb, R = I.r, stale = I.busy && (!R || I.key !== reqKey());
      e.busy.classList.toggle("on", stale); setText(e.busyT, stale ? "Reading the inbox…" : "");
      e.body.classList.toggle("dim", !!(stale && R));
      const p = period(); setText(e.lr, periodLabel(S.range, p, today()));
      let msg = "";
      if (I.unsupported) msg = `<span>Inbox figures are not available from the service yet.</span>`;
      else if (I.err && !R) msg = `<span>The inbox figures could not be read just now. Trying again.</span><button type="button" data-inretry>Try now</button>`;
      else if (I.err && R) msg = `<span>The inbox figures could not be updated just now.</span><button type="button" data-inretry>Try now</button>`;
      if (e.msg._h !== msg) { e.msg._h = msg; e.msg.innerHTML = msg; } e.msg.classList.toggle("hidden", !msg);
      e.body.classList.toggle("hidden", I.unsupported && !R);
    }
    /** The chart, the customers card and the notes (the figures are drawn by renderKpis). */
    function renderInbox(M, B) {
      const e = E.in, I = S.inb, R = I.r; if (!e || !M) return; paintInbox();
      const rep = R && R.src.replies, dm = R && R.src[I.metric];
      setText(e.n, rep && rep.v != null ? nf(rep.v) : "");
      // the chart: replies (or messages, orders, customers) per day, per week beyond 92 days, per hour for a Day
      let ib = null; if (R) { ib = inBuckets(R); }
      if (ib) setText(e.card.querySelector(".efpCT"), `Sent per ${ib.noun}`);
      const avail = !ib || ib.kind === "hour" ? ["replies", "messages"] : ["replies", "messages", "orders", "customers"]; if (!avail.includes(I.metric)) I.metric = "replies";
      e.sec.querySelectorAll("[data-inmetric]").forEach(b => { const ok = avail.includes(b.dataset.inmetric), on = b.dataset.inmetric === I.metric; b.classList.toggle("hidden", !ok); b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
      if (charts.inb) {
        if (!R) { if (I.err) charts.inb.update(null, { emptyText: "Not read just now" }); } else {
          const mt = I.metric, rows = ib.items.filter(x => !x.future), vals = rows.map(x => x[mt]), known = vals.filter(v => v != null), hiItem = ib.items[ib.hi], kind = ib.kind, xKind = kind === "hour" ? "hour" : kind === "week" ? "week" : "day", hiX = kind === "hour" ? rows.indexOf(hiItem) : hiItem ? hiItem.key : null;
          const xs = rows.map(x => (kind === "hour" ? +x.key : x.key)), noun = IN_MEASURE[mt], onPoint = p => { const m = p.meta; if (!m || !m.pickable) return; if (kind === "day") go({ range: "day", anchor: m.key }); else if (kind === "week") go({ range: "week", anchor: m.span[1] > today() ? today() : m.span[1] }); };
          const other = (k, x) => [IN_MEASURE[k][0], x[k] == null ? "—" : nf(x[k])], click = x => (x && x.pickable ? [{ k: kind === "day" ? "Click to open this day" : "Click to open this week", v: "", muted: true }] : []);
          charts.inb.update({ x: xs, series: [{ key: "v", label: noun[0], values: vals }], highlight: hiX, meta: rows }, { unit: "count", xKind, fmt: v => (v == null ? "—" : `${nf(v)} ${v === 1 ? noun[1] : noun[2]}`), tickFmt: v => nf(v), def: dm && dm.def || "", name: `${noun[0]} sent per ${ib.noun}`, onPoint, nullText: "Nothing recorded", emptyText: known.length ? "No replies in this range" : "Not recorded for this range",
            tipRows: p => { const x = p.meta; if (!x || p.values.v == null) return []; return (kind === "hour" ? [other(mt === "replies" ? "messages" : "replies", x)] : ["replies", "messages", "orders", "customers"].filter(k => k !== mt).map(k => other(k, x))).concat(click(x)); } });
          const worked = rows.filter(x => x[mt] != null && x[mt] > 0), avg = kind !== "hour" && worked.length > 1 ? worked.reduce((n, x) => n + x[mt], 0) / worked.length : null;
          setText(e.cp, avg != null ? `average ${nf1(avg)} per ${ib.noun} with ${noun[2]}` : kind === "hour" && known.length ? (() => { let pk = -1; vals.forEach((v, i) => { if (v > 0 && (pk < 0 || v > vals[pk])) pk = i; }); return pk >= 0 ? `busiest hour ${hourLabel(+rows[pk].key)}` : ""; })() : "");
        }
      }
      // customers: messages per customer (the average, how they are spread, and who got the most)
      const per = R && R.per, avg = R && R.src.messagesPerCustomer;
      setText(e.topCP, avg && avg.v != null ? `average ${nf1(avg.v)} per customer` : "");
      if (charts.inDist) {
        const dist = per ? per.dist.filter(d => d.messages != null) : [];
        e.distH.classList.toggle("hidden", !dist.length);
        if (dist.length) charts.inDist.update({ x: dist.map(d => (d.plus ? `${d.messages}+` : String(d.messages))), series: [{ key: "c", label: "Customers", values: dist.map(d => d.customers) }], meta: dist }, { unit: "count", fmt: v => (v == null ? "—" : `${nf(v)} ${v === 1 ? "customer" : "customers"}`), tickFmt: v => nf(v), name: "Customers by messages received", emptyText: "", label: p => { const d = p.meta; return !d ? String(p.x) : d.plus ? `${d.messages} or more messages` : `${d.messages} ${d.messages === 1 ? "message" : "messages"} each`; }, def: "How many customers got one, two, three, four, or five or more messages from this person in the period." });
      }
      paintTop(R);
      // the orders covered: the list follows the window; its label carries the figure
      const orc = R && R.src.orders; setText(e.ordersN, orc && orc.v != null ? nf(orc.v) : "");
      // what is known and what is not
      const lines = [];
      if (R) {
        if (R.knownFrom && R.from && R.from < R.knownFrom) lines.push(`The inbox keeps its own record of sent replies from ${fullFmt.format(dayDate(R.knownFrom))}: earlier days show a dash, not zero.`);
        if (R.unknown) lines.push(`${nf(R.unknown.replies)} ${R.unknown.replies === 1 ? "reply" : "replies"} in this period ${R.unknown.replies === 1 ? "was" : "were"} sent before a name was recorded (unknown): they are not counted for anyone.`);
        for (const n of R.notes) lines.push(n);
        if (R.partial && !R.notes.length) lines.push("Part of the inbox records could not be read just now, so some figures may be low.");
      }
      lines.push("Only replies a person actually sent count. AI drafts nobody sent, and the AI's own replies, are left out.");
      const sig = lines.join("|"); if (e.foot._sig !== sig) { e.foot._sig = sig; e.foot.textContent = ""; for (const l of lines) e.foot.appendChild(el("div")).textContent = l; }
      roveFix(e.kp, ".efpK");   // (one Tab stop for the group, whichever cards "More figures" shows)
    }
    /** The customers who got the most messages: each row opens that customer's newest order; the same bar and hover card as the rates. */
    function paintTop(R) {
      const e = E.in, per = R && R.per, all = per ? per.top : [], few = options.inboxShow, more = all.length > few, top = more && !S.inb.topAll ? all.slice(0, few) : all, sig = JSON.stringify([top, all.length, S.inb.topAll, R && R.from, R && R.to, !!R]); if (e.top._sig === sig) return; e.top._sig = sig;
      const keep = e.top.querySelector(".efpTip"); e.top.textContent = ""; if (keep) e.top.appendChild(keep);
      e.topL.classList.toggle("hidden", !top.length);
      if (!top.length) { const msg = !R ? (S.inb.err ? "Not read just now" : "Reading the inbox…") : per && per.total === 0 || R.src.replies && R.src.replies.v === 0 ? "No replies were sent in this range" : "The customers are not listed for this range"; e.top.insertBefore(el("div", "efpInNone", esc(msg)), e.top.firstChild); return; }
      const max = Math.max(1, ...top.map(t => nz(t.messages))), td = today();
      top.forEach((t, i) => {
        const b = el(t.rid ? "button" : "div", "efpTR"); if (t.rid) { b.type = "button"; b.dataset.order = t.rid; }
        const meta = [t.orders != null ? `${nf(t.orders)} ${t.orders === 1 ? "order" : "orders"}` : "", t.replies != null ? `${nf(t.replies)} ${t.replies === 1 ? "reply" : "replies"}` : "", t.lastAt ? `last ${stamp(t.lastAt, td)}` : ""].filter(Boolean).join(" · ");
        b.innerHTML = `<span class="efpTRn">${esc(t.customer || "Customer")}</span><span class="efpTRv">${t.messages != null ? nf(t.messages) : "—"}<small>${t.messages === 1 ? "message" : "messages"}</small></span><span class="efpTRm">${esc(meta)}</span><span class="efpTRb"><i data-w="${(nz(t.messages) / max * 100).toFixed(1)}"></i></span>`;
        const tip = { t: t.customer || "Customer", v: t.messages != null ? `${nf(t.messages)} ${t.messages === 1 ? "message" : "messages"}` : "—", rows: [].concat(t.replies != null ? [["Replies", nf(t.replies)]] : [], t.orders != null ? [["Orders", nf(t.orders)]] : [], t.lastAt ? [["Last message", stamp(t.lastAt, td)]] : [], per && per.total ? [["Rank", `${i + 1} of ${nf(per.total)} customers`]] : []), hint: t.rid ? "Click to open their newest order." : "" };
        b.setAttribute("aria-label", `${t.customer || "Customer"}: ${t.messages != null ? nf(t.messages) : "no"} messages${meta ? ". " + meta : ""}${t.rid ? ". Open their newest order" : ""}`);
        b.addEventListener("pointerenter", () => mini.top.show(b, tip)); b.addEventListener("pointerleave", () => mini.top.hide()); b.addEventListener("focus", () => mini.top.show(b, tip)); b.addEventListener("blur", () => mini.top.hide());
        e.top.insertBefore(b, e.top.querySelector(".efpTip"));
      });
      if (more) { const t = el("button", "efpInMoreB"); t.type = "button"; t.setAttribute("data-intopmore", ""); t.setAttribute("aria-expanded", !!S.inb.topAll); t.textContent = S.inb.topAll ? `Show the top ${few}` : `Show all ${all.length}`; e.top.insertBefore(t, e.top.querySelector(".efpTip")); }
      requestAnimationFrame(() => e.top.querySelectorAll(".efpTRb i").forEach(x => { x.style.width = x.dataset.w + "%"; }));
    }

    /* ── build once; everything after updates in place ── */
    const CHEV = `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M10 3 5 8l5 5"/></svg>`;
    const SEARCH = `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" aria-hidden="true"><circle cx="7" cy="7" r="4.5"/><path d="m10.5 10.5 3 3"/></svg>`;
    function build() {
      root0.innerHTML = `
<div class="efpTop"><button type="button" class="efpBack" title="Back to Employee efficiency">${CHEV}<span>Employee efficiency</span></button></div>
<header class="efpHead"><span class="efpAv" aria-hidden="true"></span><div class="efpWho"><h2 class="efpName"></h2><div class="efpWhere" role="status"></div></div><div class="efpChips"></div>
  <span class="efpLive" data-s="load" role="status"><i class="efpDot"></i><span class="spin" aria-hidden="true"></span><span class="efpLiveT">Connecting…</span></span></header>
<div class="efpBar" role="group" aria-label="Period">
  <span class="seg efpSeg" role="group" aria-label="Range">${RANGES.map(([k, l]) => `<button type="button" data-range="${k}">${l}</button>`).join("")}</span>
  <span class="efpNav"><button type="button" class="efpIcon" data-nav="-1" aria-label="Earlier">‹</button><span class="efpDay" aria-live="polite"></span><button type="button" class="efpIcon" data-nav="1" aria-label="Later">›</button><button type="button" class="efpToday hidden" data-today>Today</button></span>
  <form class="efpCustom hidden" autocomplete="off"><label>From <input type="date" name="from" aria-label="From"></label><label>To <input type="date" name="to" aria-label="To"></label><button class="btn gold sm" type="submit">Apply</button></form>
  <span class="efpGrow"></span><span class="efpBusy" role="status"><span class="spin" aria-hidden="true"></span><span class="efpBusyT"></span></span>
</div>
<div class="efpMsg hidden" role="alert"></div>
<div class="efpWait"><span class="spin" aria-hidden="true"></span><span class="efpWaitT"></span></div>
<section class="efpNowS hidden" aria-label="Now working on"><div class="efpLabel">Now working on</div><div class="efpNow"></div></section>
<div class="efpBody hidden">
  <div class="efpNote hidden" role="status"></div>
  <div class="efpKGroups"></div>
  <section aria-label="Over time"><div class="efpLabel">Over time</div>
    <div class="efpCard efpChart" data-c="tp"><div class="efpCH"><span class="efpCT">Throughput</span><span class="seg efpTog" role="group" aria-label="Measure"><button type="button" data-metric="parts">Pieces</button><button type="button" data-metric="scans">Scans</button><button type="button" data-metric="orders">Orders</button><button type="button" data-metric="perActiveHour">Per hour</button></span><span class="efpCP"></span></div><div class="efpCh"></div></div></section>
  <div class="efpGrid g2">
    <div class="efpCard efpChart" data-c="sp"><div class="efpCH"><span class="efpCT">Speed per order</span><span class="efpCP"></span></div><div class="efpCh"></div></div>
    <div class="efpCard efpChart" data-c="tm"><div class="efpCH"><span class="efpCT">Active vs signed in</span><span class="efpCP"></span></div><div class="efpCh"></div></div>
    <div class="efpCard efpChart wide hidden" data-c="shift"><div class="efpCH"><span class="efpCT">The shift</span><span class="efpCP"></span></div><div class="efpShiftH efpXY"></div></div>
  </div>
  <div class="efpGrid g2 efpWC hidden">
    <div class="efpCard efpChart" data-c="wt"><div class="efpCH"><span class="efpCT">Welding station: time by task</span><span class="efpCP"></span></div><div class="efpCh"></div></div>
    <div class="efpCard efpChart" data-c="wm"><div class="efpCH"><span class="efpCT">Orders matched</span><span class="efpCP"></span></div><div class="efpCh"></div></div>
  </div>
  <div class="efpGrid g75">
    <div class="efpCard efpChart" data-c="cal"><div class="efpCH"><span class="efpCT">Days worked</span><span class="efpCP"></span></div><div class="efpCalH"></div><div class="efpHow efpCalNote hidden"></div></div>
    <div class="efpCard efpChart" data-c="mix"><div class="efpCH"><span class="efpCT">Station mix</span><span class="efpCP"></span></div><div class="efpMixH"></div></div>
  </div>
  <div class="efpCard efpChart" data-c="heat"><div class="efpCH"><span class="efpCT">Busiest hours</span><span class="efpCP"></span></div><div class="efpHeatH"></div></div>
  <div class="efpGrid g57">
    <section aria-label="Issues"><div class="efpLabel">Issues <b class="efpIN"></b></div><div class="efpCard efpIs"></div></section>
    <section aria-label="Success and contact rates"><div class="efpLabel">Success and contact</div><div class="efpCard efpRates"></div></section>
  </div>
  <section class="efpIn" aria-label="Inbox"><div class="efpLabel"><span>Inbox</span><b class="efpInN"></b><span class="efpInLr"></span><button type="button" class="efpLink" data-more-group="Inbox" aria-expanded="false">More figures</button><span class="efpInBusy" role="status"><span class="spin" aria-hidden="true"></span><span class="efpInBusyT"></span></span></div>
    <div class="efpInMsg efpFound hidden" role="status"></div>
    <div class="efpInBody">
      <div class="efpKpis efpInK" role="group" aria-label="Inbox figures"></div>
      <div class="efpGrid g75">
        <div class="efpCard efpChart" data-c="inb"><div class="efpCH"><span class="efpCT">Sent per day</span><span class="seg efpTog" role="group" aria-label="Measure">${Object.keys(IN_MEASURE).map(k => `<button type="button" data-inmetric="${k}">${IN_MEASURE[k][0]}</button>`).join("")}</span><span class="efpCP"></span></div><div class="efpCh"></div></div>
        <div class="efpCard efpChart" data-c="inbTop"><div class="efpCH"><span class="efpCT">Messages per customer</span><span class="efpCP"></span></div><div class="efpInDist"></div><div class="efpSub efpInTopL">Most messages</div><div class="efpInTop"></div></div>
      </div>
      <div class="efpLabel efpInOL"><span>Orders covered</span><b class="efpInOn"></b></div>
      <div class="efpCard efpInOrders"></div>
      <div class="efpInFoot"></div>
    </div>
  </section>
  <section class="efpLaserS hidden" aria-label="Laser sheets"></section>
  <section aria-label="Orders"><div class="efpLabel">Orders <b class="efpON"></b><span class="efpLr"></span><button type="button" class="efpLink" data-orders-all>Show all time</button></div>
    <div class="efpCard efpOrdersHost"><div class="efpOrdersOwn"><div class="efpFind"><label class="efpSearch">${SEARCH}<input type="text" name="q" inputmode="search" autocomplete="off" spellcheck="false" placeholder="Search orders: number, customer or station" aria-label="Search this person's orders"><button type="button" class="efpIcon hidden" data-clear aria-label="Clear the search">✕</button></label><span class="efpSt" role="status"></span></div>
      <div class="efpOl"></div><div class="efpMore"></div></div></div></section>
  <section class="efpWO hidden" aria-label="Orders matched"><div class="efpLabel">Orders matched <b class="efpWON"></b><span class="efpWR"></span></div>
    <div class="efpCard efpOrdersHost efpWOrders"></div></section>
  <div class="efpCannot hidden"></div>
  <p class="efpFoot">These figures are logged activity, not effort: a phone scan is credited to the signed-in desktop's person, the sorter's name is typed, and label reprints and QA notes are signals, not verdicts. Hover any number for how it is worked out.</p>
</div>
<div class="efpHC" role="tooltip"></div>`;
      const q = s => root0.querySelector(s);
      Object.assign(E, { back: q(".efpBack"), av: q(".efpAv"), name: q(".efpName"), where: q(".efpWhere"), chips: q(".efpChips"), live: q(".efpLive"), liveT: q(".efpLiveT"), bar: q(".efpBar"), day: q(".efpDay"), prev: q('[data-nav="-1"]'), next: q('[data-nav="1"]'), today: q(".efpToday"),
        custom: q(".efpCustom"), busy: q(".efpBusy"), busyT: q(".efpBusyT"), msg: q(".efpMsg"), wait: q(".efpWait"), waitT: q(".efpWaitT"), nowS: q(".efpNowS"), now: q(".efpNow"), body: q(".efpBody"), note: q(".efpNote"), kg: q(".efpKGroups"),
        hc: q(".efpHC"), tpP: q('[data-c="tp"] .efpCP'), calP: q('[data-c="cal"] .efpCP'), mixP: q('[data-c="mix"] .efpCP'), heatP: q('[data-c="heat"] .efpCP'), shiftP: q('[data-c="shift"] .efpCP'), calH: q(".efpCalH"), calNote: q(".efpCalNote"), mixH: q(".efpMixH"), heatH: q(".efpHeatH"), shiftH: q(".efpShiftH"), iN: q(".efpIN"), is: q(".efpIs"), rates: q(".efpRates"),
        oN: q(".efpON"), find: q(".efpSearch input"), clear: q("[data-clear]"), st: q(".efpFind .efpSt"), ol: q(".efpOl"), more: q(".efpMore"), heat: q('[data-c="heat"]'), cSp: q('[data-c="sp"]'), cTm: q('[data-c="tm"]'), cShift: q('[data-c="shift"]'), wc: q(".efpWC"), wtP: q('[data-c="wt"] .efpCP'), wmP: q('[data-c="wm"] .efpCP'), wo: q(".efpWO"), woN: q(".efpWON"), woLr: q(".efpWR"), woHost: q(".efpWOrders"), ordersHost: q(".efpOrdersHost"), ordersOwn: q(".efpOrdersOwn"), cannot: q(".efpCannot"), lr: q(".efpLr"), oAll: q("[data-orders-all]") });
      E.k = {};
      for (const [gname, prim, more] of GROUPS) {
        const g = el("div", "efpGroup"), grid = el("div", "efpKpis"), lab = g.appendChild(el("div", "efpLabel")); lab.appendChild(el("span")).textContent = gname;
        if (more.length) { const b = el("button", "efpLink"); b.type = "button"; b.dataset.moreGroup = gname; b.setAttribute("aria-expanded", "false"); b.textContent = "More figures"; lab.appendChild(b); }
        g.appendChild(grid);
        for (const full of prim.concat(more)) {
          const key = shortKey(full), d = cardOf(full), c = el("div", "efpK", `<span class="efpKL"><span class="t"></span></span><span class="efpKR"><b class="efpKV">—</b><span class="efpKsp"></span></span><span class="efpKD"></span><span class="efpKF"><span class="efpKS"></span><span class="efpKT"></span></span>`);
          c.tabIndex = -1; c.dataset.k = full; c.setAttribute("role", "group"); setText(c.querySelector(".t"), d.label); if (more.includes(full)) c.classList.add("hidden", "extra");
          E.k[full] = { card: c, val: c.querySelector(".efpKV"), d: c.querySelector(".efpKD"), s: c.querySelector(".efpKS"), tag: c.querySelector(".efpKT"), sp: null, spH: c.querySelector(".efpKsp"), t: c.querySelector(".t"), group: gname };
          grid.appendChild(c);
        }
        g.dataset.g = gname; (E.groups || (E.groups = {}))[gname] = g; E.kg.appendChild(g);
      }
      charts = { tp: chart("bars", q('[data-c="tp"] .efpCh'), { height: 188, name: "Throughput", emptyText: "No activity in this range" }), sp: chart("line", q('[data-c="sp"] .efpCh'), { height: 170, name: "Speed per order" }), tm: chart("bars", q('[data-c="tm"] .efpCh'), { height: 170, name: "Active vs signed in" }),
        cal: chart("calendarHeat", E.calH, { name: "Days worked" }), mix: chart("donut", E.mixH, { name: "Station mix" }), heat: chart("hourHeatmap", E.heatH, { name: "Busiest hours of the day" }),
        wt: chart("bars", q('[data-c="wt"] .efpCh'), { height: 170, name: "Welding station: time by task", stack: true }), wm: chart("bars", q('[data-c="wm"] .efpCh'), { height: 170, name: "Orders matched" }) };
      mini = { rate: miniTip(E.rates), shift: miniTip(E.shiftH) };
      inboxBuild(q);
      setText(E.name, S.name); setText(E.av, initials(S.name)); E.av.dataset.t = String(tint(S.name));
      root0.addEventListener("click", onClick); root0.addEventListener("submit", onSubmit);
      E.find.addEventListener("input", onSearchInput); E.find.addEventListener("keydown", e => { if (e.key === "Escape" && E.find.value) { e.preventDefault(); E.find.value = ""; onSearchInput(); } });
      E.ol.addEventListener("keydown", e => { if ((e.key === "Enter" || e.key === " ") && e.target.classList && e.target.classList.contains("efpO")) { e.preventDefault(); openOrder(e.target, e.target.dataset.rid); } });
      for (const k of Object.keys(E.k)) { const c = E.k[k].card; c.addEventListener("pointerenter", () => showHC(k)); c.addEventListener("pointerleave", hideHC); c.addEventListener("focus", () => showHC(k)); c.addEventListener("blur", hideHC); }
      E.kg.querySelectorAll(".efpKpis").forEach(g => roveInit(g, ".efpK")); roveInit(E.rates, ".efpRt");
      setTimeout(() => root0.classList.remove("in"), 900);
    }

    /* ── hover card of a figure: its plain definition, and whether it is counted or estimated ── */
    function showHC(full) {
      const k = E.k[full], M = S.M, m = M && metricAt(M, full), key = shortKey(full), d = cardOf(full); if (!k) return;
      const fmt = fmtFor(key, (m && m.unit) || d.unit), def = (m && m.def) || d.def;
      const rd = M ? diffDays(M.from, M.to) + 1 : 0, part = m && m.daysCounted != null && m.daysCounted < rd, tag = m && m.est ? `<span class="tag">Estimated</span>` : part ? `<span class="tag">Counted on ${esc(nf(m.daysCounted))} of ${esc(nf(rd))} days</span>` : m && m.window && M.eventWindow ? `<span class="tag">From the newest ${nf(M.eventWindow.days)} days</span>` : m && m.derived ? `<span class="tag">Worked out from the days shown</span>` : `<span class="tag ok">Counted from logged activity</span>`;
      const why = m && m.est && m.why ? `<p>${esc(m.why)}</p>` : "" + (full === "att.daysOff" && M && M.attNote ? `<p>${esc(M.attNote)}</p>` : "") + (m && m.rule && !m.est ? `<p>${esc(m.rule)}</p>` : ""), n = m && m.n != null ? `<span class="prev">Based on ${esc(nf(m.n))}</span>` : "";
      const pv = M && M.prev ? `<span class="prev">${esc(PREV_WORD[S.range] ? PREV_WORD[S.range].replace(/^./, c => c.toUpperCase()) : "Before")}: ${m && m.prev != null ? esc(fmt(m.prev)) : "no data"}</span>` : "";
      E.hc.innerHTML = `<b>${esc(nameOf(m, d))}</b><p>${esc(def)}</p>${why}${tag}${pv}${n}`;
      const rr = root0.getBoundingClientRect(), cr = k.card.getBoundingClientRect(); E.hc.classList.add("on");
      const w = E.hc.offsetWidth, h = E.hc.offsetHeight; let x = cr.left - rr.left + cr.width / 2 - w / 2, y = cr.top - rr.top - h - 8; if (cr.top - h - 8 < 56) y = cr.bottom - rr.top + 8;
      E.hc.style.left = Math.max(8, Math.min(rr.width - w - 8, x)) + "px"; E.hc.style.top = y + "px"; k.card.classList.add("hov");
    }
    function hideHC() { E.hc.classList.remove("on"); for (const k of Object.keys(E.k)) E.k[k].card.classList.remove("hov"); }

    /* ── the bar and the head ── */
    function paintBar() {
      const p = period(), td = today();
      root0.querySelectorAll(".efpSeg button").forEach(b => { const on = b.dataset.range === S.range; b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
      setText(E.day, periodLabel(S.range, p, td)); E.next.disabled = p.to >= td; E.today.classList.toggle("hidden", td >= p.from && td <= p.to && S.anchor >= td);
      const w = S.range === "day" ? "day" : S.range === "week" ? "week" : S.range === "month" ? "month" : S.range === "quarter" ? "3 months" : S.range === "year" ? "year" : "period";
      E.prev.setAttribute("aria-label", `Previous ${w}`); E.next.setAttribute("aria-label", `Next ${w}`);
      E.custom.classList.toggle("hidden", S.range !== "custom");
      if (S.range === "custom") { const f = E.custom.elements.from, t = E.custom.elements.to, sig = p.from + "|" + p.to; f.max = t.max = td; if (E.custom._p !== sig) { E.custom._p = sig; f.value = p.from; t.value = p.to; } } else E.custom._p = "";   // (only when the period itself changed: a refresh must not undo a date being typed)
    }
    function paintBusy(label) { E.busy.classList.toggle("on", !!label); setText(E.busyT, label || ""); }
    function paintLive() {
      let s, t; const age = S.at ? (Date.now() - S.at) / 1000 : 0, la = S.liveAt ? (Date.now() - S.liveAt) / 1000 : 0, a = EA(), st = a && typeof a.state === "function" ? a.state() : null;
      if (S.locked) { s = "off"; t = "Locked"; }
      else if (!S.M && S.busy) { s = "load"; t = "Reading…"; }
      else if (S.fails && S.M) { s = "slow"; t = `${esc(S.errShort || "Reconnecting…")} · last update ${ago(age)}`; }
      else if (!S.M) { s = S.err ? "off" : "load"; t = S.err ? "Not connected · trying again" : "Connecting…"; }
      else if (S.liveFails > 1 || (st && st.connected === false)) { s = "slow"; t = `Live view reconnecting…${S.liveAt ? ` · ${ago(la)}` : ""}`; }
      else if (period().to < today()) { s = "off"; t = `Updated ${ago(age)}`; }
      else { s = "live"; t = `Live · updated ${ago(Math.min(age, S.liveAt ? la : age))}`; }
      if (E.live.dataset.s !== s) E.live.dataset.s = s; if (E.liveT.innerHTML !== t) E.liveT.innerHTML = t;
    }
    function renderHead() {
      const M = S.M, L = S.live, w = L && L.where, td = today(), seen = (M && M.to >= td && M.lastSeen) || S.seenAt || null;
      E.av.toggleAttribute("data-on", !!w);
      let txt;
      if (w) txt = `Signed in${w.stationKey ? ` at <b>${esc(stName(w.stationKey))}</b>` : ""}${w.tasks && w.tasks.length ? ` · ${w.tasks.length > 1 ? "Tasks" : "Task"}: ${esc(w.tasks.map(t => t.charAt(0).toUpperCase() + t.slice(1)).join(" and "))}` : ""}${w.since ? ` · since ${esc(stamp(w.since, td))}` : ""}`;
      else if (seen) txt = `${L ? "Not signed in · last" : "Last"} seen <b>${esc(stamp(seen, td))}</b>`;
      else txt = L || M ? "Not signed in now" : "";
      if (E.where._h !== txt) { E.where._h = txt; E.where.innerHTML = txt; }
      const here = L ? L.stationKey : "", st = (M && M.stations.length ? M.stations : []).slice(0, 5), sig = st.map(x => x.station + x.minutes + x.parts + (x.taskMin ? ":" + x.matched + ":" + x.taskMin.welding + "/" + x.taskMin.matching : "")).join() + "|" + here + !!w;
      if (E.chips._sig !== sig) { E.chips._sig = sig; E.chips.innerHTML = st.map(x => `<span class="efpChip${w && here && x.station === here ? " now" : ""}" title="${esc(x.taskMin ? `Welding ${durMs((x.taskMin.welding + x.taskMin.unknown) * 60000)} · Matching ${durMs(x.taskMin.matching * 60000)} · ${nf(x.matched)} matched` : x.parts != null ? nf(x.parts) + " pieces" : "")}"><b>${esc(x.label)}</b>${esc(x.minutes != null ? durMs(x.minutes * 60000) : x.parts != null ? nf(x.parts) + " pieces" : "")}</span>`).join(""); }
      E.name.title = S.name + (M && M.spellings.length > 1 ? `. Also seen as: ${M.spellings.filter(x => x !== M.name).join(", ")}` : "");
    }

    /* ── the figures ── */
    function deltaOf(m, key, unit) {
      const d = CARD[key]; if (m == null || m.v == null) return { txt: "", cls: "" };
      if (m.prev == null && m.delta == null) return { txt: "", cls: "", none: true };
      let ch, txt;
      if (unit === "percent") { ch = m.delta; if (ch == null) return { txt: "", cls: "", none: true }; if (Math.abs(ch) < 0.5) return { txt: "no change", cls: "" }; txt = `${ch > 0 ? "▲" : "▼"} ${Math.abs(Math.round(ch))} pts`; }
      else if (m.deltaPct != null) { ch = m.deltaPct; if (Math.abs(ch) < 0.5) return { txt: "no change", cls: "" }; txt = `${ch > 0 ? "▲" : "▼"} ${Math.abs(Math.round(ch))}%`; }
      else if (m.prev === 0) { if (m.v === 0) return { txt: "no change", cls: "" }; return { txt: "new", cls: "" }; }
      else if (m.delta != null) { ch = m.delta; if (!ch) return { txt: "no change", cls: "" }; txt = `${ch > 0 ? "▲" : "▼"} ${nf1(Math.abs(ch))}`; }
      else return { txt: "", cls: "", none: true };
      const good = !m.better ? 0 : (ch > 0 ? "up" : "down") === m.better ? 1 : -1;
      return { txt, cls: good > 0 ? "up" : good < 0 ? "down" : "" };
    }
    const kv = (M, full) => { const m = metricAt(M, full); return m ? m.v : null; };
    function subFor(full, m, M) {
      if (full.startsWith("inbox.")) return inboxSub(full, m);
      const key = shortKey(full), work = kv(M, "att.daysWorked"), off = M.cal.filter(c => c.state === "off");
      switch (key) {
        case "parts": return m.v != null && work > 0 ? `${nf(m.v / work)} per day worked` : "";
        case "orders": return m.v > 0 && kv(M, "kpis.parts") != null ? `${nf1(kv(M, "kpis.parts") / m.v)} pieces per order` : "";
        case "bestDay": return m.day ? dayLbl(m.day) : "";
        case "activeShare": { const a = kv(M, "kpis.activeHours"), s = kv(M, "kpis.signedHours"); return a != null && s != null ? `${hoursTxt(a * HOUR_MS)} of ${hoursTxt(s * HOUR_MS)}` : ""; }
        case "daysWorked": { const wd = kv(M, "att.workingDays") != null ? kv(M, "att.workingDays") : kv(M, "att.teamDays") != null ? kv(M, "att.teamDays") : work != null && kv(M, "att.daysOff") != null ? work + kv(M, "att.daysOff") : null; return wd ? `of ${nf(wd)} working days` : ""; }
        case "daysOff": return off.length ? off.slice(-3).map(c => mdLbl(c.day)).join(", ") + (off.length > 3 ? ` +${off.length - 3}` : "") : m.v === 0 ? "none" : "";
        case "issues": { const I = M.issues, top = I && I.byKind.find(k => k.count > 0); return top ? `most: ${top.label} (${nf(top.count)})` : m.v === 0 ? "none logged" : ""; }
        case "issuesPer100Orders": return kv(M, "issues.issues") != null && kv(M, "kpis.orders") != null ? `${nf(kv(M, "issues.issues"))} in ${nf(kv(M, "kpis.orders"))} orders` : "";
        case "firstPass": case "deliveryRate": case "failureRate": case "reworkRate": case "successRate": case "holdRate": case "reprintRate": case "rescanRate": case "reopenRate": case "editedShare": return m.num != null && m.den ? `${nf(m.num)} of ${nf(m.den)}` : "";
        case "lateDays": case "shortDays": return m.v === 0 ? "none" : "";
        case "weldingHours": case "matchingHours": return M.welding && M.welding.hours.station > 0 ? `of ${hoursTxt(M.welding.hours.station * HOUR_MS)} at the Welding station` : "";
        case "matchedOrders": return m.v > 0 ? "scans, not completions" : "";
        default: return "";
      }
    }
    function sparkFor(full, B, M) {
      if (full.startsWith("inbox.")) return inboxSpark(full, B);
      const key = shortKey(full), sp = cardOf(full).sp; if (!sp) return [];
      const it = B.items;
      if (B.kind === "hour") return sp === "parts" || sp === "scans" ? it.map(x => (x.future ? null : x[sp])) : [];
      const f = g => it.map(x => (x.future ? null : g(x)));
      switch (sp) {
        case "issuesDay": return B.kind === "day" && M.issues && M.issues.byDay.size ? f(x => { const b = M.issues.byDay.get(x.day); return b && b.total != null ? b.total : null; }) : [];
        case "share": return f(x => (x.activeMs != null && x.signedMs > 0 ? Math.min(100, x.activeMs / x.signedMs * 100) : null));
        case "weldingMs": case "matchingMs": case "unknownMs": return f(x => (x[sp] == null ? null : x[sp] / HOUR_MS));
        case "worked": return B.kind === "day" ? f(x => (x.state === "worked" || x.state === "partial" ? 1 : x.state === "off" ? 0 : null)) : [];
        case "off": return B.kind === "day" ? f(x => (x.state === "off" ? 1 : x.state === "worked" || x.state === "partial" ? 0 : null)) : [];
        default: return f(x => x[sp]);
      }
    }
    function renderKpis(M, B, first) {
      const evs = M.eventWindow;
      if (E.groups && E.groups["Welding station"]) E.groups["Welding station"].classList.toggle("hidden", !M.welding);
      for (const full of Object.keys(E.k)) {
        const k = E.k[full], key = shortKey(full), d = cardOf(full), m = metricAt(M, full), unit = (m && m.unit) || d.unit, fmt = fmtFor(key, unit);
        if (full.startsWith("welding.")) k.card.classList.toggle("hidden", !M.welding || !m);   // (every card of the group hidden: the group is not a Tab stop either)
        const contactOff = full.startsWith("contact.") && M.contact && M.contact.available === false;
        setNum(k.val, m ? m.v : null, fmt, first); setText(k.t, nameOf(m, d));
        const dl = M.prev && m ? deltaOf(m, key, unit) : { txt: "", cls: "" }, sig = dl.txt + "|" + dl.cls + "|" + dl.none + "|" + S.range + "|" + (m ? m.v : "x");
        if (k.d._sig !== sig) { k.d._sig = sig; const pw = PREV_WORD[S.range] || "before"; k.d.innerHTML = !m || m.v == null ? "" : dl.txt ? `<b class="${dl.cls}">${esc(dl.txt)}</b><span>vs ${esc(pw)}</span>` : dl.none ? `<span>vs ${esc(pw)}: no data</span>` : ""; }
        setText(k.s, contactOff ? "Not recorded in this range" : m && m.v != null ? subFor(full, m, M) : "");
        const rd = diffDays(M.from, M.to) + 1, tag = m && m.est ? "est." : m && m.daysCounted != null && m.daysCounted < rd && m.v != null ? `${nf(m.daysCounted)} of ${nf(rd)} days` : m && m.window && evs ? "recent" : m && m.derived ? "sum" : ""; setText(k.tag, tag);
        // a small trend line joins the days that have a figure (a day off or a future day would cut it into stubs); the charts below keep every gap
        const sp = m ? sparkFor(full, B, M).filter(x => x != null) : [], known = sp.length, on = known >= 3 && !k.card.classList.contains("hidden") && root.EfficiencyCharts;
        if (on) { if (!k.sp) k.sp = chart("sparkline", k.spH, { height: 26, hover: false, fill: false, emptyText: "", name: `${nameOf(m, d)} across the range` }); if (k.sp) k.sp.update({ values: sp }); k.spH.style.visibility = ""; }
        else { if (k.sp) { k.sp.destroy(); k.sp = null; } k.spH.style.visibility = "hidden"; }
        k.card.setAttribute("aria-label", `${nameOf(m, d)}: ${m && m.v != null ? fmt(m.v) : "no data"}. ${(m && m.def) || d.def}`);
      }
      E.kg.querySelectorAll(".efpKpis").forEach(g => roveFix(g, ".efpK"));
    }
    const TP = { parts: ["pieces", "pieces", "kpis.parts"], scans: ["scans", "scans", "kpis.scans"], orders: ["orders", "orders", "kpis.orders"], perActiveHour: ["pieces/hour", "pieces per active hour", "kpis.partsPerActiveHour"] };
    function renderCharts(M, B) {
      const kind = B.kind, rows = B.items.filter(x => !x.future), hiItem = B.items[B.hi], hiX = kind === "hour" ? rows.indexOf(hiItem) : hiItem ? hiItem.key : null;
      const xs = rows.map(x => (kind === "hour" ? +x.key : x.key)), xKind = kind === "hour" ? "hour" : kind === "week" ? "week" : "day";
      const onPoint = p => { const m = p.meta; if (!m || !m.pickable) return; if (kind === "day") go({ range: "day", anchor: m.key }); else if (kind === "week") go({ range: "week", anchor: m.span[1] > today() ? today() : m.span[1] }); };
      const click = x => (x && x.pickable ? [{ k: kind === "day" ? "Click to open this day" : "Click to open this week", v: "", muted: true }] : []);
      const avail = kind === "hour" ? ["parts", "scans"] : ["parts", "orders", "perActiveHour"]; if (!avail.includes(S.metric)) S.metric = "parts";
      root0.querySelectorAll("[data-metric]").forEach(b => { const ok = avail.includes(b.dataset.metric), on = b.dataset.metric === S.metric; b.classList.toggle("hidden", !ok); b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
      const day = kind === "hour"; E.cSp.classList.toggle("hidden", day); E.cTm.classList.toggle("hidden", day); E.cShift.classList.toggle("hidden", !day);
      // throughput
      const mt = S.metric, [unit, noun, defKey] = TP[mt], dm = metricAt(M, defKey), vals = rows.map(x => x[mt]), known = vals.filter(v => v != null), fmt = mt === "perActiveHour" ? rateTxt : nf;
      const worked = rows.filter(x => x[mt] != null && (x[mt] > 0 || nz(x.signedMs) > 0)), avg = kind !== "hour" && worked.length > 1 ? worked.reduce((n, x) => n + x[mt], 0) / worked.length : null;
      const row = (k, v) => [k, v == null ? "—" : nf(v)];
      if (charts.tp) charts.tp.update({ x: xs, series: [{ key: "v", label: noun, values: vals }], highlight: hiX, meta: rows }, { unit, xKind, def: dm && dm.def || "", emptyText: known.length ? "No activity in this range" : "Not logged for this range", onPoint, name: `Throughput, ${noun} per ${B.noun}`,
        tipRows: p => { const x = p.meta; if (!x || p.values.v == null) return []; return (kind === "hour" ? [row(mt === "parts" ? "Scans" : "Pieces", mt === "parts" ? x.scans : x.parts)] : [mt !== "orders" ? row("Orders", x.orders) : row("Pieces", x.parts), mt !== "perActiveHour" ? ["Per active hour", rateTxt(x.perActiveHour)] : row("Pieces", x.parts), ["Signed in", durMs(x.signedMs)], ["Active", durMs(x.activeMs)]]).concat(click(x)); } });
      setText(E.tpP, avg != null ? `average ${fmt(avg)} per ${B.noun} worked` : kind === "hour" && known.length ? (() => { let pk = -1; vals.forEach((v, i) => { if (v > 0 && (pk < 0 || v > vals[pk])) pk = i; }); return pk >= 0 ? `busiest hour ${hourLabel(+rows[pk].key)}` : ""; })() : "");
      if (day) { renderShift(M); return; }
      // speed: the median (days with single events) and the average
      const med = rows.map(x => x.medianSecPerOrder), avgS = rows.map(x => x.secPerOrder), hasMed = med.some(v => v != null), hasAvg = avgS.some(v => v != null), sdef = metricAt(M, hasMed ? "kpis.secPerOrderMedian" : "kpis.secPerOrderMean");
      if (charts.sp) charts.sp.update({ x: xs, series: [hasMed ? { key: "median", label: "Median", values: med, color: "ink70" } : null, hasAvg ? { key: "avg", label: "Average", values: avgS, color: "gold2" } : null].filter(Boolean), meta: rows }, { unit: "seconds", better: "down", xKind, def: sdef && sdef.def || "", emptyText: "No speed data in this range", onPoint, tipRows: p => (p.meta && (p.values.median != null || p.values.avg != null) ? [row("Orders", p.meta.orders)].concat(click(p.meta)) : []) });
      // active vs signed in, in hours
      const sg = rows.map(x => (x.signedMs == null ? null : x.signedMs / HOUR_MS)), ac = rows.map(x => (x.activeMs == null ? null : x.activeMs / HOUR_MS)), adef = metricAt(M, "kpis.activeShare");
      if (charts.tm) charts.tm.update({ x: xs, series: [{ key: "signed", label: "Signed in", values: sg, color: "ink25" }, { key: "active", label: "Active", values: ac, color: "ink70" }], meta: rows }, { unit: "hours", better: null, xKind, fmt: v => durMs(v * HOUR_MS), def: adef && adef.def || "", emptyText: "No sign-in time in this range", onPoint,
        tipRows: p => { const x = p.meta; if (!x || x.signedMs == null && x.activeMs == null) return []; return [["Active share", x.signedMs > 0 && x.activeMs != null ? pctTxt(Math.min(1, x.activeMs / x.signedMs)) : "—"], row("Pieces", x.parts)].concat(click(x)); } });
    }
    /* the Welding station: hours signed in per task (stacked), and the orders matched, on the same buckets as the other charts (a Day is one bar) */
    function renderWelding(M, B) {
      const W = M.welding, on = !!W; E.wc.classList.toggle("hidden", !on); if (!on) return;
      const hour = B.kind === "hour", items = hour ? M.series.map(p => Object.assign({}, p, { key: p.day, label: mdLbl(p.day), title: dayLbl(p.day), future: false, pickable: false })) : B.items.filter(x => !x.future);
      const xs = items.map(x => x.key), kind = hour ? "day" : B.kind, H = v => (v == null ? null : v / HOUR_MS), hiX = !hour && B.items[B.hi] ? B.items[B.hi].key : hour && items.length ? items[items.length - 1].key : null;
      const onPoint = p => { const m = p.meta; if (!m || !m.pickable) return; if (kind === "day") go({ range: "day", anchor: m.key }); else if (kind === "week") go({ range: "week", anchor: m.span[1] > today() ? today() : m.span[1] }); };
      const click = x => (x && x.pickable ? [{ k: kind === "day" ? "Click to open this day" : "Click to open this week", v: "", muted: true }] : []);
      const hasUnknown = items.some(x => nz(x.unknownMs) > 0), wdef = (metricAt(M, "welding.weldingHours") || {}).def || CARD.weldingHours.def, mdef = (metricAt(M, "welding.matchedOrders") || {}).def || CARD.matchedOrders.def;
      if (charts.wt) charts.wt.update({ x: xs, series: [{ key: "welding", label: "Welding", values: items.map(x => H(x.weldingMs)) }, { key: "matching", label: "Matching", values: items.map(x => H(x.matchingMs)), color: "gold2" }].concat(hasUnknown ? [{ key: "unknown", label: "Welding (task not recorded)", values: items.map(x => H(x.unknownMs)), color: "ink25" }] : []), highlight: hiX, meta: items },
        { unit: "hours", stack: true, xKind: kind, fmt: v => durMs(v * HOUR_MS), def: wdef, emptyText: "No time at the Welding station in this range", onPoint, name: "Welding station: time signed in by task", tipRows: p => { const x = p.meta; if (!x) return []; return [["Signed in at Welding", durMs(nz(x.weldingMs) + nz(x.matchingMs) + nz(x.unknownMs))]].concat(click(x)); } });
      if (charts.wm) charts.wm.update({ x: xs, series: [{ key: "matched", label: "Orders matched", values: items.map(x => x.matched) }], highlight: hiX, meta: items },
        { unit: "orders", xKind: kind, fmt: nf, def: mdef, emptyText: "No orders matched in this range", onPoint, name: "Orders matched, per " + (hour ? "day" : B.noun), tipRows: p => click(p.meta) });
      const wh = W.hours, tot = nz(wh.welding) + nz(wh.matching) + nz(wh.unknown);
      setText(E.wtP, tot > 0 ? `${hoursTxt(nz(wh.welding) * HOUR_MS + nz(wh.unknown) * HOUR_MS)} welding · ${hoursTxt(nz(wh.matching) * HOUR_MS)} matching` : "");
      setText(E.wmP, W.matched != null && W.matched > 0 ? `${nf(W.matched)} ${W.matched === 1 ? "scan" : "scans"}` : "");
    }
    /** The orders this person matched: the shared order list (EfficiencyOrders) asked for `matched` orders, over the same days as the date chips. Mounted once the person has Welding time. */
    function syncMatched(M) {
      const on = !!(M && M.welding), EO = root.EfficiencyOrders; E.wo.classList.toggle("hidden", !on || !(EO && typeof EO.mount === "function"));
      if (!on || !EO || typeof EO.mount !== "function") return;
      const r = ordersRange(), label = S.ordersAll ? "All time" : periodLabel(S.range, period(), today()); setText(E.woLr, label);
      if (!matchedH) {
        try { const host2 = el("div", "efpOrdersMod"); E.woHost.appendChild(host2); const h = EO.mount(host2, { name: S.name, range: r, dateFields: false, filters: false, call: (body, signal) => call(Object.assign({}, body, { matched: true }), signal), onOpen: (rid, btn) => openOrder(btn || host2, rid), onState: i => { try { setText(E.woN, i && i.total != null ? nf(i.total) : ""); } catch (_) {} } }); if (h) { matchedH = h; S.matchedSig = JSON.stringify(r); return; } host2.remove(); } catch (e) { console.warn("[efficiency person] matched orders:", e && e.message); }
        return;
      }
      const sig = JSON.stringify(r); if (sig !== S.matchedSig) { S.matchedSig = sig; if (typeof matchedH.setRange === "function") { try { matchedH.setRange(r); } catch (e) { console.warn("[efficiency person] matched orders range:", e && e.message); } } }
    }
    /* how a day ended when the station signed the person out by itself (the auto sign-out: both are normal sign-outs, hours end at the last input) */
    const ENDED_WORDS = { idle: "Signed out after 10 minutes without input", closing: "Signed out at 5:00 pm" };
    /* (the station's own words come with the day: Laser "Signed out after 1 hour without input" / "Signed out at 5:00 pm after 30 minutes without input", Welding "Signed out at 5:00 pm"; the two above are for an older service) */
    const endedWord = c => (c && c.ended ? c.endedText || ENDED_WORDS[c.ended] || "" : "");
    /* the Day view: one shift, in, out, and what the signed-in time was made of */
    function renderShift(M) {
      const c = M.cal[0] || {}, p = M.series[0] || {}, td = today(), host0 = E.shiftH, tipEl = host0.querySelector(".efpTip");
      const sg = c.signedMs != null ? c.signedMs : p.signedMs, ac = c.activeMs != null ? c.activeMs : p.activeMs, id = p.idleMs, ul = sg != null && ac != null ? Math.max(0, sg - ac - nz(id)) : null;
      const sig = JSON.stringify([c, p.idleMs, M.from]); if (host0._sig === sig) return; host0._sig = sig; host0.querySelectorAll(":scope>:not(.efpTip)").forEach(x => x.remove());
      const box = el("div", "efpShift");
      if (!(sg > 0)) { const why = { off: "A day off: a team working day on which this person never signed in.", closed: "The team did not work this day.", before: "Before this person's first record.", future: "Not yet.", pending: "Not signed in yet today." }[c.state] || "Not signed in this day."; box.innerHTML = `<div class="efpEmptyBox" style="padding:12px 0">${esc(why)}</div>`; setText(E.shiftP, ""); host0.insertBefore(box, tipEl); return; }
      setText(E.shiftP, c.late ? "late start" : c.short ? "short day" : "");
      const pc = v => (v > 0 && sg > 0 ? Math.max(0, Math.min(100, v / sg * 100)) : 0);
      box.innerHTML = `<div class="efpShiftT"><span>In <b>${c.firstIn ? esc(clock(c.firstIn)) : "—"}</b></span><span>Out <b>${c.lastOut ? esc(clock(c.lastOut)) : M.from === td ? "still in" : "—"}</b></span>${endedWord(c) ? `<span>${esc(endedWord(c))}</span>` : ""}<span>Signed in <b>${esc(durMs(sg))}</b></span>${ac != null ? `<span>Active <b>${esc(durMs(ac))}</b>${sg > 0 ? ` (${pctTxt(Math.min(1, ac / sg))})` : ""}</span>` : ""}</div>
<div class="efpSB"><i class="a" data-w="${pc(ac).toFixed(1)}"></i><i class="i" data-w="${pc(id).toFixed(1)}"></i><i class="u" data-w="${pc(ul).toFixed(1)}"></i></div>
<div class="efpSLeg"><span><i style="background:#6f6a62"></i>Active</span><span><i style="background:var(--gold2)"></i>Idle (gaps over 5 minutes)</span><span><i style="background:var(--line)"></i>No action logged</span></div>`;
      host0.insertBefore(box, tipEl);
      const segs = [["a", "Active", ac, "Time between actions that were less than 5 minutes apart."], ["i", "Idle", id, "Gaps of more than 5 minutes between actions."], ["u", "No action logged", ul, "Signed in, but nothing was logged."]];
      segs.forEach(([cls, t, v, h]) => { const b = box.querySelector(".efpSB ." + cls); if (!b) return; const tp = { t, v: durMs(v), rows: sg > 0 && v != null ? [["Of signed in", pctTxt(Math.min(1, v / sg))]] : [], hint: h }; b.addEventListener("pointerenter", () => mini.shift.show(b, tp)); b.addEventListener("pointerleave", () => mini.shift.hide()); });
      requestAnimationFrame(() => box.querySelectorAll(".efpSB i").forEach(b => { b.style.width = b.dataset.w + "%"; }));
    }

    /* the calendar: a month laid out as weeks, a long range as a column of seven per week */
    function calSource(M) {
      const n = diffDays(M.from, M.to) + 1;
      if (n >= 28) return { cal: M.cal, from: M.from, to: M.to, sel: [M.from, M.to] };
      const cx = S.calx && S.calx.to === M.to ? S.calx : null;
      return { cal: cx ? cx.cal : M.cal, from: cx ? cx.from : M.from, to: cx ? cx.to : M.to, sel: [M.from, M.to], wait: !cx };
    }
    function renderCal() {
      const M = S.M; if (!M) return; const src = calSource(M), td = today(), n = diffDays(src.from, src.to) + 1;
      const worked = src.cal.filter(c => c.state === "worked" || c.state === "partial").length, off = src.cal.filter(c => c.state === "off").length;
      setText(E.calP, src.wait && n < 28 ? "" : worked || off ? `${nf(worked)} worked · ${nf(off)} off` : ""); setText(E.calNote, M.attNote && off ? M.attNote : ""); E.calNote.classList.toggle("hidden", !(M.attNote && off));
      if (!charts.cal) return;
      if (src.wait && n < 28) { charts.cal.setLoading(true, "Reading the month"); return; }
      const days = src.cal.filter(c => c.day >= src.from && c.day <= src.to).map(c => (c.state === "unknown" ? Object.assign({}, c, { state: "before", stateLabel: "Before sign-in logging began" }) : c.day > td ? Object.assign({}, c, { state: "future" }) : c));
      charts.cal.update({ days, today: td }, { selected: S.range === "day" ? M.from : "", emptyText: "No days to show for this range", onDay: day => go({ range: "day", anchor: day }),
        tipRows: it => { const r = []; if (it.extra) r.push({ k: "Extra day (not a working day)", v: "", muted: true }); if (it.lengthKnown === false) r.push({ k: "Length of the day not known", v: "", muted: true }); if (it.est) r.push({ k: "Estimated", v: "", muted: true }); if (it.others != null && !["future", "closed", "before", "unknown"].includes(it.state)) r.push({ k: "Team", v: `${nf(it.others)} others in`, muted: true }); if (it.state !== "future" && it.state !== "before") r.push({ k: "Click to open this day", v: "", muted: true }); return r; } });
    }

    /* station mix */
    function renderMix(M) {
      const st = M.stations.filter(s => nz(s.parts) > 0 || nz(s.minutes) > 0), byParts = st.some(s => nz(s.parts) > 0), val = s => (byParts ? nz(s.parts) : nz(s.minutes)), tot = st.reduce((n, s) => n + val(s), 0);
      setText(E.mixP, tot ? (byParts ? `${nf(tot)} pieces` : durMs(tot * 60000)) : "");
      if (!charts.mix) return;
      charts.mix.update({ slices: st.map(s => ({ key: s.station, label: s.label, value: val(s), meta: s })), center: tot ? { value: tot, label: byParts ? "pieces" : "time" } : null },
        { unit: byParts ? "pieces" : "min", centerLabel: byParts ? "pieces" : "time", emptyText: M.stations.length ? "No station activity in this range" : "No station data in this range", def: byParts ? "Pieces finished at each station in this range." : "Time signed in at each station in this range.",
          tipRows: x => { const s = x.meta; if (s && !Array.isArray(s) && s.taskMin) return [["Matched", s.matched == null ? "—" : nf(s.matched)], ["Welding", durMs((s.taskMin.welding + s.taskMin.unknown) * 60000)], ["Matching", durMs(s.taskMin.matching * 60000)]]; return s && !Array.isArray(s) ? [["Orders", s.orders == null ? "—" : nf(s.orders)], ["Time", s.minutes == null ? "—" : durMs(s.minutes * 60000)], ["Per active hour", rateTxt(s.perActiveHour)]] : []; } });
    }

    /* busiest hours: the hours of the day over the range */
    function renderHeat(M, B) {
      const dayRange = B.kind === "hour", has = M.hours.length && M.hours.some(h => h.parts != null);
      E.heat.classList.toggle("hidden", dayRange || !has); if (dayRange || !has) return;
      const vals = M.hours.map(h => h.parts); let peak = -1, lo = 7, hi = 18; vals.forEach((v, h) => { if (v > 0) { if (peak < 0 || v > vals[peak]) peak = h; lo = Math.min(lo, h); hi = Math.max(hi, h); } });
      setText(E.heatP, peak >= 0 ? `busiest around ${hourLabel(peak)}` : "");
      if (!charts.heat) return;
      charts.heat.update({ values: vals }, { unit: "pieces", hourFrom: lo, hourTo: hi, emptyText: "No activity in this range", def: "Pieces finished in each hour of the day, over the whole range (New York time).",
        tipRows: c => { const x = M.hours[c.hour] || {}; return [["Scans", x.scans == null ? "—" : nf(x.scans)], ["Per day worked", x.perDay == null ? "—" : nf1(x.perDay)]]; } });
    }

    /* issues, by kind, each with its order and the plain reason */
    const ATTR = { own: "Their action", system: "System failure", order: "About the order" };
    function renderIssues(M) {
      const I = M.issues; setText(E.iN, I && I.total != null ? nf(I.total) : "");
      const sig = JSON.stringify([I && Object.assign({}, I, { byDay: null }), [...S.openKinds], S.kinds, M.eventWindow, M.from, M.to]); if (E.is._sig === sig) return; E.is._sig = sig;
      if (!I) { E.is.innerHTML = `<div class="efpEmptyBox">No issue data in this range</div>`; return; }
      if (!I.byKind.length) { E.is.innerHTML = `<div class="efpEmptyBox">${I.total === 0 ? "No issues logged in this range" : "No issue data in this range"}</div>`; return; }
      const td = today(), rd = diffDays(M.from, M.to) + 1, ew = M.eventWindow ? M.eventWindow.days : null, d = v => (v == null ? "—" : nf1(v));
      const sum = [["Total", I.total], ["Their action", I.own], ["About the order", I.order], ["System failure", I.system], ["Per 100 orders", I.per100]].filter(x => x[1] != null || x[0] === "Total");
      const partial = I.daysCounted != null && I.daysCounted < rd ? `<div class="efpHow efpPad">Counted on ${nf(I.daysCounted)} of ${nf(rd)} days: the other days were logged before these counters existed, so they are not guessed.</div>` : "";
      const some = I.byKind.filter(k => k.count > 0), none = I.byKind.filter(k => k.count === 0), unk = I.byKind.filter(k => k.count == null);
      const group = (k, gi) => {
        const items = I.items.filter(x => x.kind === k.kind), open = S.openKinds.has(k.kind) || (!S.kinds && gi === 0), shown = items.slice(0, 8);
        const cov = k.coverage === "window" ? `<div class="efpHow">${k.checked ? `Counted from the newest ${nf(k.checked.of)} finished orders (${nf(k.checked.orders)} found).` : ew ? `Counted from the newest ${nf(ew)} days of the range only.` : "Counted from the newest orders only."}</div>` : k.coverage === "range-partial" && k.daysCounted != null ? `<div class="efpHow">Counted on ${nf(k.daysCounted)} of ${nf(rd)} days.</div>` : "";
        return `<div class="efpIg${open ? " open" : ""}" data-kind="${esc(k.kind)}"><button type="button" class="efpIgh" aria-expanded="${open}" data-kindbtn="${esc(k.kind)}" title="${esc(k.def)}"><span><b>${esc(k.label)}</b>${k.attribution && ATTR[k.attribution] ? `<span class="efpAt ${esc(k.attribution)}">${esc(ATTR[k.attribution])}</span>` : ""}${k.est ? `<span class="efpAt">Estimated</span>` : ""}${k.def ? `<small>${esc(k.def)}</small>` : ""}</span><em>${nf(k.count)}</em><i aria-hidden="true">▼</i></button><div class="efpIgw"><div class="efpIgi"><div class="efpIgl">${k.how ? `<div class="efpHow">${esc(k.how)}</div>` : ""}${k.est && k.why ? `<div class="efpHow">${esc(k.why)}</div>` : ""}${cov}${shown.map(x => `<div class="efpIr"><time>${x.at ? esc(stamp(x.at, td)) : x.day ? esc(mdLbl(x.day)) : "—"}</time>${x.rid ? `<button type="button" class="efpOid" data-order="${esc(x.rid)}" title="Open this order">${esc(x.number || x.rid)}</button>` : "<span>—</span>"}<span>${x.station ? esc(stName(x.station)) + " · " : ""}${esc(x.note || "No reason recorded")}</span></div>`).join("")}${items.length > shown.length ? `<div class="efpHow">+${nf(items.length - shown.length)} more in this range</div>` : ""}${!items.length ? `<div class="efpHow">${nf(k.count)} counted. The single events are not listed for this range.</div>` : ""}</div></div></div></div>`;
      };
      E.is.innerHTML = `<div class="efpSum">${sum.map(([k, v]) => `<span>${esc(k)} <b>${d(v)}</b></span>`).join("")}</div>${partial}` + (some.length ? some.map(group).join("") : `<div class="efpEmptyBox" style="padding:12px 18px">${I.total === 0 ? "No issues logged in this range" : "No issues counted in this range"}</div>`)
        + (none.length ? `<div class="efpHow efpNone"><b>None logged:</b> ${none.map(k => esc(k.label)).join(", ")}.</div>` : "") + (unk.length ? `<div class="efpHow efpNone"><b>Not counted on these days:</b> ${unk.map(k => esc(k.label)).join(", ")}.</div>` : "");
    }
    const RATE_ORDER = ["firstPass", "reworkRate", "successRate", "failureRate", "holdRate", "reprintRate", "rescanRate"];
    function renderRates(M) {
      const R = Object.values(M.src.rates).sort((a, b) => { const i = RATE_ORDER.indexOf(a.key), j = RATE_ORDER.indexOf(b.key); return (i < 0 ? 99 : i) - (j < 0 ? 99 : j); }), C = M.contact, cm = C && C.available ? C.metrics : null;
      const sig = JSON.stringify([R, C, M.from, M.to]); if (E.rates._sig === sig) return; E.rates._sig = sig;
      const rd = diffDays(M.from, M.to) + 1;
      const rateRow = (r, i) => { const known = r.v != null, w = known ? Math.max(0, Math.min(100, r.v)) : 0, cls = r.better === "down" ? "bad" : r.better === "up" ? "ok" : "un", dl = M.prev ? deltaOf(r, "x", "percent") : { txt: "" }, part = r.daysCounted != null && r.daysCounted < rd && r.coverage !== "window";
        return `<div class="efpRt" data-r="${i}" tabindex="0" aria-label="${esc(r.label)}: ${known ? pctVal(r.v) : "no data"}. ${esc(r.def)}"><div class="efpRtH"><b>${esc(r.label)}</b>${r.est ? `<em>est.</em>` : ""}<span>${known ? pctVal(r.v) : "—"}</span>${r.num != null && r.den ? `<em>${nf(r.num)} of ${nf(r.den)}</em>` : ""}${part ? `<em>${nf(r.daysCounted)} of ${nf(rd)} days</em>` : ""}${dl.txt ? `<em class="efpD ${dl.cls}">${esc(dl.txt)}</em>` : ""}</div><div class="efpRtB"><i class="${cls}" data-w="${w.toFixed(1)}"></i></div></div>`; };
      let html = R.length ? R.map((r, i) => rateRow(r, i)).join("") : `<div class="efpEmptyBox" style="padding:12px 0">No rates in this range yet</div>`;
      html += `<div class="efpSub">Inbox replies</div>`;
      if (!C) html += `<div class="efpHow">Contact figures are not read yet.</div>`;
      else if (!C.available) html += `<div class="efpHow">Inbox replies are not recorded for this person in this range.</div>`;
      else { const f = (k, l, fmt) => (cm[k] ? `<span>${esc(l)}<b>${cm[k].v != null ? esc(fmt(cm[k].v)) : "—"}</b></span>` : ""); html += `<div class="efpStat">${f("sent", "Sent", nf)}${f("delivered", "Delivered", nf)}${f("failed", "Failed", nf)}${f("refused", "Refused", nf)}${f("drafted", "Drafts", nf)}${f("edited", "Edited", nf)}${f("medianFirstReplyMs", "First reply (middle)", durMs)}</div>${C.daysCounted != null && C.daysCounted < rd ? `<div class="efpHow">Counted on ${nf(C.daysCounted)} of ${nf(rd)} days.</div>` : ""}`; }
      E.rates.innerHTML = html;
      roveFix(E.rates, ".efpRt");
      E.rates.querySelectorAll(".efpRt").forEach(n => { const r = R[+n.dataset.r], t = { t: r.label, v: r.v != null ? pctVal(r.v) : "No data", rows: [].concat(r.num != null ? [["Good", nf(r.num)]] : [], r.den != null ? [["Of", nf(r.den)]] : [], r.prev != null ? [[(PREV_WORD[S.range] || "Before").replace(/^./, c => c.toUpperCase()), pctVal(r.prev)]] : []), hint: r.def + (r.est && r.why ? " Estimated: " + r.why : r.est ? " Estimated." : "") + (r.coverage === "window" ? " Counted from the newest finished orders only." : "") }; n.addEventListener("pointerenter", () => mini.rate.show(n, t)); n.addEventListener("pointerleave", () => mini.rate.hide()); n.addEventListener("focus", () => mini.rate.show(n, t)); n.addEventListener("blur", () => mini.rate.hide()); });
      requestAnimationFrame(() => E.rates.querySelectorAll(".efpRtB i").forEach(b => { b.style.width = b.dataset.w + "%"; }));
    }
    function renderNote(M) {
      const lines = M.notes.slice(); if (M.partial && !lines.length) lines.push("Part of the data could not be read just now, so some figures may be low.");
      if (!M.found) lines.push("No sign-ins or activity were found for this name.");
      else if (!Object.values(M.src.kpis).some(k => k.v != null) && !lines.length) lines.push("No activity in this range.");
      const sig = lines.join("|"); if (E.note._sig === sig) return; E.note._sig = sig; E.note.textContent = ""; for (const l of lines) E.note.appendChild(el("span")).textContent = l; E.note.classList.toggle("hidden", !lines.length);
      const ct = M.cannotTell, csig = JSON.stringify(ct); if (E.cannot._sig !== csig) { E.cannot._sig = csig; E.cannot.innerHTML = ct.length ? `<div><b>What this cannot tell you</b></div>` + ct.map(c => `<div>${c.topic ? `<b>${esc(c.topic)}.</b> ` : ""}${esc(c.text)}</div>`).join("") : ""; E.cannot.classList.toggle("hidden", !ct.length); }
    }
    function render(M, first) {
      const B = buckets(M, today(), now()); paintBar(); renderHead(); renderNote(M); renderKpis(M, B, first); renderCharts(M, B); renderWelding(M, B); renderCal(); renderMix(M); renderHeat(M, B); renderIssues(M); renderRates(M); syncMatched(M); paintLive();
      renderInbox(M, B);
    }

    /* ── the live card: the order in this person's hands right now ── */
    function renderLive() {
      const L = S.live; if (!L) return; E.nowS.classList.remove("hidden");
      const cur = L.current, ids = new Set(cur.map(c => String(c.rid || c.orderNumber)));
      for (const [id, n] of [...S.now]) if (!ids.has(id)) leaveLive(id, n, L);
      const idle = E.now.querySelector(".efpNowIdle");
      if (!cur.length) {
        if (S.leaving) return;                                   // a finished card is still on its way out: the quiet line waits for it
        const w = L.where, td = today(), last = (w && w.lastSeenAt) || S.seenAt || null;
        const html = w ? `<i></i><span><b>Not on an order right now</b>${last ? ` · last activity ${esc(ago((now() - last) / 1000))}` : ""}${w.stationKey ? ` at ${esc(stName(w.stationKey))}` : ""}</span>` : `<i></i><span><b>Not signed in</b>${last ? ` · last seen ${esc(stamp(last, td))}` : ""}</span>`;
        if (!idle) { const e = el("div", "efpCard efpNowIdle", html); e._h = html; E.now.appendChild(e); if (!still() && e.animate) e.animate([{ opacity: 0, transform: "translateY(4px)" }, { opacity: 1, transform: "none" }], { duration: 260, easing: "cubic-bezier(.2,.8,.2,1)" }); } else if (idle._h !== html) { idle._h = html; idle.innerHTML = html; }
        return;
      }
      if (idle) idle.remove();
      for (const c of cur) {
        const id = String(c.rid || c.orderNumber), sig = JSON.stringify(c); let n = S.now.get(id);
        if (n && n.sig === sig) continue;
        if (n && typeof n.el.update === "function") { try { n.el.update(c); n.sig = sig; continue; } catch (e) { console.warn("[efficiency person] card update:", e && e.message); } }   // the shared card changes in place: its timer and pictures are kept
        let node = null; const OC = root.EfficiencyStations && root.EfficiencyStations.orderCard;
        if (typeof OC === "function") { try { const r = OC(c, { hideStation: false, onOpen: (cc, card) => { openOrder(card, (cc && (cc.rid || cc.orderNumber)) || id); return true; } }); node = r && r.nodeType === 1 ? r : r && (r.el || r.node) || null; if (!node && typeof r === "string") node = el("div", "", r); } catch (e) { console.warn("[efficiency person] shared order card:", e && e.message); } }
        if (!node) node = nowCard(c, now());
        if (n) n.el.replaceWith(node); else { E.now.appendChild(node); if (!still() && node.animate) node.animate([{ opacity: 0, transform: "translateY(6px)" }, { opacity: 1, transform: "none" }], { duration: 300, easing: "cubic-bezier(.2,.8,.2,1)" }); }
        S.now.set(id, { el: node, sig });
      }
    }
    /** An order that left this person's hands: the shared card says how long it took, stays a moment, then folds away. */
    function leaveLive(id, n, L) {
      S.now.delete(id); const e = n.el, c = n.sig ? (() => { try { return JSON.parse(n.sig); } catch (_) { return null; } })() : null;
      const gone = () => { if (e.isConnected) e.remove(); S.leaving = Math.max(0, (S.leaving || 0) - 1); if (!S.dead && S.live) renderLive(); };
      const out = () => { if (S.dead || still() || !e.animate) return gone(); const a = e.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateY(-6px)" }], { duration: 260, easing: "ease-in", fill: "forwards" }); a.onfinish = gone; a.oncancel = gone; };
      S.leaving = (S.leaving || 0) + 1;
      if (typeof e.finish === "function" && !S.dead) { const ms = c && num(c.scannedAt) ? now() - c.scannedAt : 0, on = !!(L && L.where), text = ms >= 0 && ms < 864e5 && c && c.scannedAt ? `${on ? "Done in" : "Left after"} ${durMs(ms)}` : on ? "Done" : "Left"; try { e.finish(text); } catch (_) {} setTimeout(out, S.leaveMs == null ? 2400 : S.leaveMs); } else out();
    }

    /* ── reading: the period (a stored answer is shown at once, then made fresh), the live card, the month for the calendar ── */
    function accept(r, fromCache, at) {
      const p = period(), M = norm(r, { name: S.name, from: p.from, to: p.to }); if (!M.from || !M.to) { M.from = p.from; M.to = p.to; }
      const first = !S.M || !!fromCache; if (num(r.now) && !fromCache && typeof o.now !== "function" && !(EA() && EA().now)) S.off = num(r.now) - Date.now();
      if (M.lastSeen && M.to >= today()) S.seenAt = Math.max(S.seenAt || 0, M.lastSeen);       // "last seen" survives a switch to a day that has none (only windows that reach today say anything about now)
      M.src.inbox = inboxMetrics();
      S.M = M; S.at = at || Date.now(); E.wait.classList.add("hidden"); E.body.classList.remove("hidden"); E.body.classList.remove("dim"); if (!S.locked) E.msg.classList.add("hidden");
      render(M, first);
      if (laserH) laserH.repaint();
    }
    async function fetchRange(gen, poll) {
      clearTimeout(T.range); T.range = 0; if (S.dead || S.locked) return; if (poll && (!visible() || S.busy)) return;
      if (gen !== S.gen) return; const key = reqKey(); S.busy = true; S.ctl = root.AbortController ? new AbortController() : null;
      if (!S.M) setText(E.waitT, `Reading ${S.name}'s history…`); if (S.M && !poll) paintBusy(`Reading ${S.range === "day" ? "the day" : "the period"}…`); paintLive();
      try {
        const r = await call(request(), S.ctl && S.ctl.signal); if (gen !== S.gen) return;
        S.cache.set(key, { r, at: Date.now() }); while (S.cache.size > options.cacheMax) S.cache.delete(S.cache.keys().next().value);
        S.fails = 0; S.err = ""; S.errShort = ""; accept(r, false);
      } catch (e) {
        if (gen !== S.gen || (e && e.name === "AbortError")) return;
        if (isAuth(e)) { lockOut(e); return; }
        S.fails++; S.err = String(e.message || e).slice(0, 200); S.errShort = e.short || "Reconnecting…";
        if (!S.M) { setText(E.waitT, `${S.err} Trying again.`); E.wait.querySelector(".spin").style.visibility = "hidden"; } else E.body.classList.remove("dim");
      } finally { if (gen === S.gen) { S.busy = false; paintBusy(""); paintLive(); if (!S.locked) schedule("range", S.fails ? backoff(S.fails) : period().to >= today() ? options.rangeMs : options.rangeMs * 4); } }
    }
    function applyLive(snap) { S.live = pickLive(snap, S.name, S.M && S.M.spellings); S.liveAt = Date.now(); S.liveFails = 0; renderLive(); renderHead(); paintLive(); }
    async function pollLive() {
      clearTimeout(T.live); T.live = 0; if (unsubLive || !visible() || S.liveBusy) return; S.liveBusy = true;
      try { const r = await call({ op: "live" }); if (S.dead) return; applyLive(r); }
      catch (e) { if (e && e.name === "AbortError") return; if (isAuth(e)) { lockOut(e); return; } S.liveFails++; }
      finally { S.liveBusy = false; paintLive(); if (!S.locked) schedule("live", S.liveFails ? backoff(S.liveFails) : options.liveMs); }
    }
    async function pollCal() {
      clearTimeout(T.cal); T.cal = 0; if (!visible() || S.calBusy || !S.M) return; const M = S.M; if (diffDays(M.from, M.to) + 1 >= 28) return;
      S.calBusy = true; const to = M.to;
      try { const r = await call({ op: "person", name: S.name, range: "month", day: to, compare: false }); const X = norm(r, { name: S.name }); if (S.M && S.M.to === to) { S.calx = { to, from: X.from || addDays(to, -29), cal: X.cal }; if (X.lastSeen && to >= today()) { S.seenAt = Math.max(S.seenAt || 0, X.lastSeen); renderHead(); } renderCal(); } }
      catch (e) { if (isAuth(e)) lockOut(e); }
      finally { S.calBusy = false; if (!S.locked) schedule("cal", options.calMs); }
    }
    function go(ch) {
      Object.assign(S, ch);
      if (S.range === "custom" && !S.custom) { const p = period(); S.custom = { from: p.from, to: p.to }; }
      S.following = period().to >= today() && S.anchor >= today(); store.set(RANGE_STORE, S.range === "custom" ? "" : S.range);
      S.gen++; if (S.ctl) { try { S.ctl.abort(); } catch (_) {} } S.busy = false; S.fails = 0; S.err = ""; paintBar(); syncOrders(); hideHC(); S.calx = S.calx && S.M && S.calx.to === period().to ? S.calx : null;
      try { o.onState && o.onState({ range: S.range, day: S.anchor, from: period().from, to: period().to }); } catch (_) {}
      syncLaser();
      inboxGo();
      const key = reqKey(), hit = S.cache.get(key);
      if (hit) accept(hit.r, true, hit.at); else E.body.classList.add("dim");
      clearTimeout(T.cal); T.cal = 0; if (S.M) schedule("cal", hit ? 200 : 900);
      if (!hit || Date.now() - hit.at > 4000) fetchRange(S.gen, false); else schedule("range", options.rangeMs);
    }

    /* ── the orders: E8's list when its module is in the page, else this small one (search as you type, paged as you scroll) ── */
    const hl = (s, q) => { s = String(s || ""); if (!q) return esc(s); const i = s.toLowerCase().indexOf(q.toLowerCase()); return i < 0 ? esc(s) : `${esc(s.slice(0, i))}<mark>${esc(s.slice(i, i + q.length))}</mark>${esc(s.slice(i + q.length))}`; };
    function orderRow(x, q, i, fresh) {
      const r = el("article", "efpO" + (fresh ? " new" : i < 12 && !still() ? " ent" : "")); r.tabIndex = 0; r.setAttribute("role", "button"); r.dataset.rid = x.rid; r.setAttribute("aria-label", `Order ${x.number}${x.customer ? ", " + x.customer : ""}. Open`);
      if (!fresh && i < 12 && !still()) r.style.animationDelay = `${i * 28}ms`;
      const td = today(), when = x.at ? `<b>${esc(clock(x.at))}</b>${esc(nyDay(x.at) === td ? "Today" : mdLbl(nyDay(x.at)))}` : "—";
      r.innerHTML = `<div class="efpOt">${when}</div><div class="efpOn"><button type="button" class="efpOid" data-order="${esc(x.rid)}" tabindex="-1" title="Open this order">${hl(x.number, q)}</button><span>${hl(x.customer || "", q)}</span></div><div class="efpOs"><b>${hl(x.station ? stName(x.station) : "—", q)}</b><span>${x.durationMs != null ? esc(durMs(x.durationMs)) : "—"}${x.count > 1 ? ` · ${x.count} pieces` : ""}${x.flags.length ? ` · ${esc(x.flags.join(", "))}` : ""}</span></div><div class="efpOp"></div>`;
      pictures(r.querySelector(".efpOp"), x, 7); r.appendChild(qrBox(x.qr || x.rid, `QR for order ${x.number}`)); return r;
    }
    function paintOrders() {
      const O = S.orders, q = O.q;
      setText(E.oN, O.total != null ? nf(O.total) : O.list.length ? `${nf(O.list.length)}${O.done ? "" : "+"}` : "");
      E.clear.classList.toggle("hidden", !E.find.value);
      let st = "";
      if (O.err && !O.list.length) st = `Not read · trying again`; else if (O.busy && (O.typing || !O.list.length || O.reset)) st = `<span class="spin" aria-hidden="true"></span>${q ? "Searching orders…" : "Reading orders…"}`; else if (O.list.length) st = `<b>${nf(O.list.length)}</b>${O.total != null && O.total > O.list.length ? ` of ${nf(O.total)}` : ""} ${q ? (O.list.length === 1 && O.done ? "match" : "matches") : "orders"}`;
      if (E.st._h !== st) { E.st._h = st; E.st.innerHTML = st; }
      let mh = ""; if (O.busy && O.list.length && !O.reset) mh = `<span class="spin" aria-hidden="true"></span>Loading more orders…`; else if (O.err && O.list.length) mh = `<span>Not read · </span><button type="button" data-more>Try again</button>`; else if (!O.done && O.list.length && !O.busy) mh = `<button type="button" data-more>Load more orders</button>`; else if (O.done && O.list.length > 12) mh = `<span>That is every order${q ? " that matches" : ""}</span>`;
      if (E.more._h !== mh) { E.more._h = mh; E.more.innerHTML = mh; }
      E.ol.style.opacity = O.reset && O.busy && O.list.length ? ".45" : "";
      if (!O.list.length && !O.busy && O.loaded && !E.ol.querySelector(".efpEmptyBox")) E.ol.innerHTML = `<div class="efpEmptyBox">${q ? `No orders match “${esc(q)}”` : "No orders yet"}</div>`;
    }
    function drawOrders(keep) {
      const O = S.orders, q = O.q; if (!keep || E.ol.querySelector(".efpEmptyBox")) E.ol.textContent = "";
      const have = keep ? E.ol.children.length : 0, frag = doc.createDocumentFragment();
      O.list.slice(have).forEach((x, i) => frag.appendChild(orderRow(x, q, i, false))); E.ol.appendChild(frag);
    }
    async function loadOrders(reset) {
      const O = S.orders; if (ordersH || S.dead || S.locked || (O.busy && !reset)) return; if (!reset && (O.done || (O.loaded && !O.next))) return;
      const gen = reset ? ++O.gen : O.gen; if (reset) { O.reset = true; O.done = false; O.next = ""; }
      O.busy = true; O.err = ""; paintOrders();
      try {
        const r = await call({ op: "personOrders", name: S.name, q: O.q, cursor: reset ? "" : O.next, limit: options.pageSize }); if (gen !== O.gen || S.dead) return;
        const N = normOrders(r);
        if (reset) { O.list = []; O.seen = new Set(); }
        for (const x of N.orders) if (!O.seen.has(x.rid)) { O.seen.add(x.rid); O.list.push(x); }
        O.next = N.next; O.done = !N.next; O.total = N.total != null ? N.total : O.done ? O.list.length : null; O.loaded = true; O.err = ""; O.at = Date.now();
        drawOrders(!reset);
      } catch (e) {
        if (gen !== O.gen || (e && e.name === "AbortError")) return; if (isAuth(e)) { lockOut(e); return; } O.err = String(e.message || e).slice(0, 100);
      } finally { if (gen === O.gen) { O.busy = false; O.reset = false; O.typing = false; paintOrders(); } }
    }
    async function pollOrders() {
      clearTimeout(T.orders); T.orders = 0; const O = S.orders; if (ordersH) return; if (!visible() || O.busy || !O.loaded) { schedule("orders", options.ordersMs); return; }
      const gen = O.gen, q = O.q;
      try {
        const r = await call({ op: "personOrders", name: S.name, q, cursor: "", limit: options.pageSize }); if (gen !== O.gen || S.dead || O.q !== q) return;
        const N = normOrders(r), fresh = N.orders.filter(x => !O.seen.has(x.rid));
        if (fresh.length) { fresh.forEach(x => O.seen.add(x.rid)); O.list = fresh.concat(O.list); if (N.total != null) O.total = N.total; if (E.ol.querySelector(".efpEmptyBox")) E.ol.textContent = ""; const frag = doc.createDocumentFragment(); fresh.forEach((x, i) => frag.appendChild(orderRow(x, q, i, true))); E.ol.insertBefore(frag, E.ol.firstChild); paintOrders(); }
        else if (N.total != null && N.total !== O.total) { O.total = N.total; paintOrders(); }
      } catch (e) { if (isAuth(e)) lockOut(e); }
      finally { if (!S.locked) schedule("orders", options.ordersMs); }
    }
    function onSearchInput() {
      const v = E.find.value.trim(), O = S.orders; E.clear.classList.toggle("hidden", !E.find.value);
      clearTimeout(T.deb); if (v === O.q && !O.err) { O.typing = false; paintOrders(); return; }
      O.typing = true; E.st.innerHTML = `<span class="spin" aria-hidden="true"></span>Searching orders…`; E.st._h = E.st.innerHTML;
      T.deb = setTimeout(() => { O.q = v; loadOrders(true); }, options.debounceMs);
    }
    /** E8's list follows the date chips (its own From / To fields are off); "Show all time" lifts the range for a search over everything. */
    const ordersRange = () => { if (S.ordersAll) return null; const p = period(); return { from: p.from, to: p.to }; };
    function paintOrdersLabel() {
      if (!ordersH) { setText(E.lr, "Everything this person handled"); E.oAll.classList.add("hidden"); return; }
      setText(E.lr, S.ordersAll ? "All time" : periodLabel(S.range, period(), today())); E.oAll.classList.remove("hidden"); setText(E.oAll, S.ordersAll ? "Only this period" : "Show all time");
    }
    function syncOrders() {
      if (S.M) syncMatched(S.M);
      if (!ordersH) return; paintOrdersLabel();
      const r = ordersRange(), sig = JSON.stringify(r); if (sig === S.ordersSig) return; S.ordersSig = sig;
      if (typeof ordersH.setRange === "function") { try { ordersH.setRange(r); } catch (e) { console.warn("[efficiency person] order list range:", e && e.message); } }
    }
    function mountOrders() {
      const EO = root.EfficiencyOrders;
      if (EO && typeof EO.mount === "function") {
        try { const host1 = el("div", "efpOrdersMod"); E.ordersHost.appendChild(host1); const r0 = ordersRange(), h = EO.mount(host1, { name: S.name, range: r0, dateFields: false, onOpen: (rid, btn) => openOrder(btn || host1, rid) }); if (h) { ordersH = h; S.ordersSig = JSON.stringify(r0); E.ordersOwn.classList.add("hidden"); if (io) io.disconnect(); paintOrdersLabel(); return; } host1.remove(); } catch (e) { console.warn("[efficiency person] order list module:", e && e.message); }
      }
      if (root.IntersectionObserver) { io = new IntersectionObserver(es => { if (es.some(x => x.isIntersecting)) loadOrders(false); }, { rootMargin: "400px 0px" }); io.observe(E.more); }
      loadOrders(true); schedule("orders", options.ordersMs);
    }
    /* ── the laser sheets (LS1: charm-nest-efficiency-laser.js draws its own section, which stays hidden for somebody with no sheet in the range; it follows the date chips) ── */
    const laserRange = () => { const p = period(); return { from: p.from, to: p.to }; };
    function syncLaser() { if (!laserH) return; try { laserH.setRange(laserRange()); } catch (e) { console.warn("[efficiency person] laser sheets range:", e && e.message); } }
    function mountLaser() {
      const EL = root.EfficiencyLaser, host1 = root0.querySelector(".efpLaserS"); if (!EL || typeof EL.mount !== "function" || !host1) return;
      try { laserH = EL.mount(host1, { name: S.name, range: laserRange(), call: (b, sig) => call(b, sig), now, expected: () => !!(S.M && S.M.stations.some(s => s.station === "laser")), onError: e => { if (isAuth(e)) lockOut(e); } }) || null; }
      catch (e) { console.warn("[efficiency person] laser sheets:", e && e.message); laserH = null; }
    }

    /* ── clicks ── */
    function onClick(e) {
      const t = e.target; if (!t.closest) return;
      const b = t.closest("button"), row = t.closest(".efpO");
      if (b && b.classList.contains("efpBack")) { try { o.onBack && o.onBack(); } catch (x) { console.warn("[efficiency person] back:", x && x.message); } return; }
      if (b && b.dataset.range) { const r = b.dataset.range; if (r === S.range) return; return go({ range: r, custom: r === "custom" ? (S.custom || (() => { const p = period(); return { from: p.from, to: p.to }; })()) : S.custom }); }
      if (b && b.dataset.nav) {
        const p = period(), dir = +b.dataset.nav; if (dir > 0 && p.to >= today()) return;
        if (S.range === "custom") { const n = diffDays(p.from, p.to) + 1, c = dir < 0 ? { from: addDays(p.from, -n), to: addDays(p.from, -1) } : { from: addDays(p.to, 1), to: addDays(p.to, n) }; if (c.to > today()) { c.to = today(); } return go({ custom: c, anchor: c.to }); }
        const a = shiftAnchor(S.range, S.anchor, dir, p); return go({ anchor: a > today() ? today() : a });
      }
      if (b && b.hasAttribute("data-today")) { const p = period(), n = diffDays(p.from, p.to); return go({ anchor: today(), custom: S.range === "custom" ? { from: addDays(today(), -n), to: today() } : S.custom }); }
      if (b && b.dataset.metric) { S.metric = b.dataset.metric; if (S.M) renderCharts(S.M, buckets(S.M, today(), now())); return; }
      if (b && b.dataset.moreGroup) { const g = b.dataset.moreGroup, on = !S.moreOpen.has(g); if (on) S.moreOpen.add(g); else S.moreOpen.delete(g); b.setAttribute("aria-expanded", on); b.textContent = on ? "Fewer figures" : "More figures"; for (const full of Object.keys(E.k)) { const k = E.k[full]; if (k.group === g && k.card.classList.contains("extra")) { k.card.classList.toggle("hidden", !on); if (on && !still()) { k.card.classList.remove("more"); void k.card.offsetWidth; k.card.classList.add("more"); } } } if (S.M) renderKpis(S.M, buckets(S.M, today(), now()), true); return; }
      if (b && b.dataset.kindbtn) { const k = b.dataset.kindbtn, g = b.closest(".efpIg"); if (!S.kinds) { S.kinds = true; const f = S.M && S.M.issues && S.M.issues.byKind[0]; if (f && f.kind !== k) S.openKinds.add(f.kind); } const open = !g.classList.contains("open"); if (open) S.openKinds.add(k); else S.openKinds.delete(k); g.classList.toggle("open", open); b.setAttribute("aria-expanded", open); E.is._sig = JSON.stringify([S.M && S.M.issues, [...S.openKinds], S.kinds, S.M && S.M.eventWindow]); return; }
      if (b && b.hasAttribute("data-clear")) { E.find.value = ""; onSearchInput(); E.find.focus(); return; }
      if (b && b.hasAttribute("data-orders-all")) { S.ordersAll = !S.ordersAll; syncOrders(); return; }
      if (b && b.hasAttribute("data-more")) { loadOrders(false); return; }
      const ob = t.closest("[data-order]"); if (ob && ob.dataset.order) { e.stopPropagation(); return openOrder(ob, ob.dataset.order); }
      if (row && row.dataset.rid) return openOrder(row, row.dataset.rid);
    }
    function onSubmit(e) {
      if (!e.target.classList.contains("efpCustom")) return; e.preventDefault();
      const f = e.target.elements.from.value, t = e.target.elements.to.value, td = today();
      if (!isDay(f) || !isDay(t)) return; let a = f, b = t > td ? td : t; if (a > b) { const x = a; a = b; b = x; } if (diffDays(a, b) > 730) a = addDays(b, -730);
      go({ range: "custom", custom: { from: a, to: b }, anchor: b });
    }

    /* ── the clock: since-scanned ticks, ages tick, timers come back when the page is shown again ── */
    let lastDay = today();
    function tick() {
      if (S.dead) return; if (!host.isConnected) return unmount();
      const vis = visible(); paintLive();
      if (vis) {
        tickSince(E.now, now());
        for (const k of ["range", "live", "orders"]) if (!T[k] && !(k === "range" && S.busy) && !(k === "live" && (S.liveBusy || unsubLive)) && !(k === "orders" && ordersH)) schedule(k, 200);
        if (!T.cal && S.M && diffDays(S.M.from, S.M.to) < 27) schedule("cal", 300);
        const td = today(); if (td !== lastDay) { lastDay = td; if (S.following) { S.anchor = td; go({}); } else paintBar(); }
      }
    }
    function onVisible() { if (S.dead) return; if (doc.visibilityState === "hidden") { for (const k of ["range", "live", "cal", "orders"]) { clearTimeout(T[k]); T[k] = 0; } hideHC(); return; } if (!visible()) return; if (S.M && Date.now() - S.at > 2000 && !S.busy) fetchRange(S.gen, true); pollLive(); if (S.orders.loaded) schedule("orders", 400); schedule("cal", 600); const a = EA(); if (unsubLive && a && typeof a.live === "function") { const s = a.live(); if (s) applyLive(s); } }
    function unmount() {
      if (S.dead) return; S.dead = true; for (const k of Object.keys(T)) { clearTimeout(T[k]); clearInterval(T[k]); T[k] = 0; }
      if (S.ctl) { try { S.ctl.abort(); } catch (_) {} } if (io) io.disconnect(); Object.values(charts).forEach(c => c && c.destroy && c.destroy()); for (const k of Object.keys(E.k || {})) if (E.k[k].sp) E.k[k].sp.destroy();
      if (typeof unsubLive === "function") { try { unsubLive(); } catch (_) {} } if (ordersH) { try { (ordersH.unmount || ordersH.destroy || ordersH).call(ordersH); } catch (_) {} } if (matchedH) { try { (matchedH.unmount || matchedH.destroy || matchedH).call(matchedH); } catch (_) {} }
      if (laserH) { try { laserH.unmount(); } catch (_) {} laserH = null; }
      doc.removeEventListener("visibilitychange", onVisible); try { if (root.Seal && root.Seal.zoom && root.Seal.zoom.away) root.Seal.zoom.away(); } catch (_) {}
      inboxEnd();
      if (root0.parentNode) root0.remove(); instances.delete(api);
    }
    function refresh() { if (S.dead) return; if (laserH) laserH.refresh(); S.cache.clear(); S.gen++; S.fails = 0; fetchRange(S.gen, false); pollLive(); pollCal(); if (ordersH && typeof ordersH.refresh === "function") ordersH.refresh(); else loadOrders(true); if (matchedH && typeof matchedH.refresh === "function") matchedH.refresh(); }

    build(); paintBar(); setText(E.waitT, `Reading ${S.name}'s history…`);
    if (!o.onBack) E.back.classList.add("hidden");
    doc.addEventListener("visibilitychange", onVisible); T.tick = setInterval(tick, Math.max(250, options.tickMs));
    const api = { unmount, destroy: unmount, refresh, get state() { return { name: S.name, range: S.range, anchor: S.anchor, from: period().from, to: period().to, metric: S.metric, locked: S.locked, fails: S.fails, liveFails: S.liveFails, orders: S.orders.list.length, query: S.orders.q, loaded: !!S.M, at: S.at, ordersModule: !!ordersH, welding: !!(S.M && S.M.welding), matchedModule: !!matchedH }; }, go, el: root0 };
    instances.add(api);
    // the live card: the console's own live read when it offers one, else this page asks for `live` itself
    const subscribe = typeof o.onLive === "function" ? o.onLive : EA() && typeof EA().onLive === "function" ? EA().onLive : null;
    if (subscribe) { try { unsubLive = subscribe(applyLive) || true; } catch (_) { unsubLive = null; } const cur = typeof o.live === "function" ? o.live() : EA() && typeof EA().live === "function" ? EA().live() : null; if (cur) applyLive(cur); }
    go({}); if (!unsubLive) pollLive(); mountOrders();
    mountLaser();
    return api;
  }
  const instances = new Set();
  root.EfficiencyEmployee = { mount, options, norm, normOrders, normInbox, pickLive, buckets, periodOf, shiftAnchor, periodLabel, CARD, GROUPS, get instances() { return [...instances]; } };
})(window);
