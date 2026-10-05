/*  charm-nest-efficiency-person.js — one employee, full page (Paul, 5 Oct 2026; plans/employee-hr/plan.md, worker E5).
 *  window.EfficiencyEmployee.mount(el, { name, onBack, range?, day?, custom?, call?, mode?, now?, onLive?, live?, onState?, onAuth? })
 *      -> { unmount, destroy, refresh, state, go, el }       (the console, charm-nest-efficiency.js, mounts it at #efficiency/person/<name>)
 *  A page, not a pop-up: header (who, where signed in now or last seen, stations), date chips (Day | Week | Month | 3 months | Year | Custom, with
 *  earlier/later arrows), a live "Now working on" card, KPI cards (grouped Production / Speed / Time / Attendance / Quality / Contact) with the
 *  change against the period before, sparklines and a hover card that says how each number is worked out (and whether it is estimated), charts
 *  with crosshair read-outs (throughput, speed, active vs signed in, busiest hours, station mix), a days-worked calendar (a click on a day opens
 *  that Day), issues grouped by kind, success and contact rates, and a real-time order search over everything this person handled.
 *  PARTS: this file is the shell (header, chips, KPIs, layout, the live card). The order list is EfficiencyOrders.mount (E8,
 *  charm-nest-efficiency-orders.js) when that file is loaded, else a small list of its own here. The charts are drawn here for now; they are the
 *  part E7's EfficiencyCharts (charm-nest-efficiency-charts.js) is to take over (see plans/employee-hr/api.md, section E5).
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
  const NAMES = { shipping: "Shipping", assembly: "Assembly", welding: "Welding", sorting: "Sorting", design: "Design", laser: "Laser", sorter: "Sorter", qr: "QR printer", inbox: "Inbox" };
  const RANGES = [["day", "Day"], ["week", "Week"], ["month", "Month"], ["quarter", "3 months"], ["year", "Year"], ["custom", "Custom"]];
  const PALETTE = ["#a9823f", "#5f7a5b", "#4a6b78", "#b0563f", "#8d95a0", "#c08578", "#7d6a9a", "#3f8a86"];
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
  const durMs = ms => { ms = num(ms); if (ms == null) return "—"; ms = Math.max(0, ms); if (ms < 1000) return ms > 0 ? "<1 s" : "0 m"; if (ms < 60000) return `${Math.round(ms / 1000)} s`; const m = Math.round(ms / 60000); if (m < 60) return `${m} m`; const h = Math.floor(m / 60); return `${h} h${m % 60 ? ` ${m % 60} m` : ""}`; };
  const hoursTxt = ms => { ms = num(ms); if (ms == null) return "—"; const h = ms / HOUR_MS; return h >= 100 ? `${nf(h)} h` : h >= 10 ? `${nf1(h)} h` : h > 0 ? durMs(ms) : "0 m"; };
  const secTxt = s => { s = num(s); if (s == null || s <= 0) return "—"; return s < 90 ? `${Math.round(s)} s` : s < 5400 ? `${(Math.round(s / 6) / 10).toString()} min` : `${(Math.round(s / 360) / 10).toString()} h`; };
  const rateTxt = v => { v = num(v); return v == null || v <= 0 ? "—" : v >= 10 ? nf(v) : (Math.round(v * 10) / 10).toString(); };
  const pctTxt = f => (num(f) == null ? "—" : `${Math.round(f * 100)}%`);
  const ago = s => (s < 2 ? "just now" : s < 60 ? `${Math.round(s)}s ago` : s < 3600 ? `${Math.floor(s / 60)}m ago` : s < 86400 ? `${Math.floor(s / 3600)}h ago` : `${Math.floor(s / 86400)}d ago`);
  const stName = s => NAMES[String(s || "").toLowerCase()] || (s ? String(s).charAt(0).toUpperCase() + String(s).slice(1) : "Station");
  const initials = name => { const w = String(name || "?").replace(/[^\p{L}\p{N} ._-]/gu, "").split(/[ ._-]+/).filter(Boolean); return ((w[0] || "?").charAt(0) + (w.length > 1 ? w[w.length - 1].charAt(0) : "")).toUpperCase(); };
  const tint = name => { let h = 0; for (const c of String(name)) h = (h * 31 + c.charCodeAt(0)) >>> 0; return h % 4; };
  const stamp = (t, today) => (nyDay(t) === today ? clock(t) : `${mdLbl(nyDay(t))}, ${clock(t)}`);
  const niceMax = v => { const NICE = [1, 2, 4, 5, 8, 10]; if (!(v > 0)) return 4; let p = 1; while (p * 10 < v) p *= 10; for (const f of NICE) if (f * p >= v) return f * p; return 10 * p; };
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
.efp{container-type:inline-size;container-name:efp;position:relative;display:grid;gap:12px;min-width:0;color:var(--ink);font-size:12.5px;padding-bottom:8px;align-content:start}
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
.efpDot{width:7px;height:7px;border-radius:50%;background:var(--ink25);flex:0 0 7px}
.efpLive[data-s=live] .efpDot{background:var(--sage);animation:efpPulse 2.4s ease-out infinite}.efpLive[data-s=slow] .efpDot{background:var(--gold2)}.efpLive[data-s=load] .efpDot{display:none}
.efpLive .spin{display:none}.efpLive[data-s=load] .spin{display:inline-block}
@keyframes efpPulse{0%{box-shadow:0 0 0 0 rgba(95,122,91,.4)}70%,100%{box-shadow:0 0 0 6px rgba(95,122,91,0)}}
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
.efpLabel{font-size:10.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--ink45);font-weight:750;display:flex;align-items:center;gap:8px;margin:0 2px 7px}
.efpLabel:after{content:"";flex:1;height:1px;background:var(--line);order:1}.efpLabel b{color:var(--ink70);letter-spacing:0;font-weight:700}.efpLabel .efpLr{order:2;letter-spacing:0;text-transform:none;font-weight:500;font-size:11.5px}
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
.efpGroup{display:grid;gap:8px}.efpGroup+.efpGroup{margin-top:4px}
.efpKpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.efpK{position:relative;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 16px 10px;display:grid;gap:2px;min-width:0;outline:none;transition:transform .2s ease,box-shadow .2s ease,border-color .2s ease;cursor:default;overflow:hidden}
.efpK:hover,.efpK:focus-visible,.efpK.hov{transform:translateY(-1px);box-shadow:var(--sh);border-color:var(--ink25)}
.efpK:focus-visible{outline:2px solid var(--gold);outline-offset:2px}
.efpKL{font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:flex;gap:6px;align-items:center}
.efpKL i{font-style:normal;font-weight:700;font-size:8.5px;letter-spacing:.04em;padding:1px 4px;border-radius:4px;background:var(--goldSoft);color:#7a5a1d}
.efpKV{font:600 29px/1.1 var(--sans);letter-spacing:-.022em;font-variant-numeric:tabular-nums;white-space:nowrap}.efpKV small{font-size:13px;font-weight:600;color:var(--ink45);letter-spacing:0;margin-left:3px}
.efpKD{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--ink45);min-height:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efpKD b{display:inline-flex;align-items:center;gap:2px;font-weight:700;font-variant-numeric:tabular-nums;padding:0 5px;border-radius:5px;background:var(--paper2);color:var(--ink70)}
.efpKD b.up{background:var(--sageSoft);color:#46623f}.efpKD b.down{background:var(--claySoft);color:#8a3f2b}
.efpKS{font-size:11px;color:var(--ink45);min-height:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efpKsp{position:absolute;right:12px;top:14px;width:76px;height:26px;pointer-events:none;opacity:.95}
.efpSpark{display:block;overflow:visible}.efpSL{fill:none;stroke:#6f6a62;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}.efpSA{fill:rgba(93,90,82,.08);stroke:none}.efpSD{fill:var(--gold);stroke:var(--card);stroke-width:1.5}
.efpHC{position:absolute;z-index:12;pointer-events:none;width:max-content;max-width:min(272px,calc(100% - 16px));background:var(--velvet);color:#f6f1e6;border-radius:10px;padding:9px 12px 10px;font-size:11.5px;line-height:1.45;box-shadow:0 10px 28px rgba(20,16,10,.28);opacity:0;visibility:hidden;transform:translateY(4px);transition:opacity .14s ease,transform .14s ease,visibility 0s .14s}
.efpHC.on{opacity:1;visibility:visible;transform:none;transition:opacity .14s ease,transform .14s ease}
.efpHC b{display:block;font:650 12.5px var(--sans);color:#fff;margin-bottom:2px}.efpHC p{margin:0;color:#e3dccd}.efpHC .tag{display:inline-block;margin-top:6px;font:700 9px var(--sans);letter-spacing:.07em;text-transform:uppercase;padding:2px 6px;border-radius:4px;background:rgba(202,168,97,.2);color:#ecd596}.efpHC .tag.ok{background:rgba(95,122,91,.3);color:#cfe3c8}.efpHC .prev{display:block;margin-top:5px;color:#b9b09f;font-size:11px}
.efpGrid{display:grid;gap:12px;min-width:0}.efpGrid.g2{grid-template-columns:repeat(2,minmax(0,1fr))}.efpGrid.g2>.wide{grid-column:1/-1}.efpGrid.g75{grid-template-columns:minmax(0,7fr) minmax(0,5fr)}.efpGrid.g57{grid-template-columns:minmax(0,5fr) minmax(0,7fr)}
.efpChart{padding:12px 18px 14px;display:grid;gap:6px;min-width:0;align-content:start}
.efpCH{display:flex;align-items:center;gap:6px 12px;flex-wrap:wrap;min-height:26px}
.efpCT{font-weight:700;letter-spacing:.07em;text-transform:uppercase;font-size:10.5px;color:var(--ink45)}.efpCP{margin-left:auto;font-size:11.5px;color:var(--ink45);display:inline-flex;gap:10px;align-items:center;flex-wrap:wrap}
.efpLeg{display:inline-flex;align-items:center;gap:5px}.efpLeg i{width:9px;height:9px;border-radius:3px;display:inline-block}
.efpTog{display:inline-flex}.efpTog button{padding:2px 9px;font-size:10.5px}
.efpXY{position:relative;min-width:0}.efpSvg{display:block;overflow:visible;max-width:100%;outline:none;touch-action:pan-y}.efpSvg:focus-visible{outline:2px solid var(--gold);outline-offset:3px;border-radius:4px}
.efpGridL{stroke:var(--line2);stroke-width:1}.efpBase{stroke:var(--line);stroke-width:1}
.efpTick,.efpXl{font:10px var(--sans);fill:var(--ink45)}.efpXl.on{fill:var(--gold);font-weight:700}
.efpBar0{fill:#85807a;transition:fill .15s}.efpBar0.under{fill:var(--line)}.efpBar0.hi{fill:var(--gold)}.efpBar0.under.hi{fill:var(--goldLine)}.efpBar0.hov{fill:var(--ink)}.efpBar0.hi.hov{fill:var(--gold2)}.efpBar0.under.hov{fill:var(--ink25)}.efpBar0.pickable{cursor:pointer}
.efpLn{fill:none;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}.efpLn.a{stroke:var(--ink70)}.efpLn.b{stroke:var(--gold2)}
.efpBand{fill:rgba(169,130,63,.1);stroke:none}
.efpRef{stroke:var(--gold2);stroke-width:1;stroke-dasharray:3 4;opacity:.9}.efpRefT{font:600 10px var(--sans);fill:var(--gold)}
.efpCross{stroke:var(--ink45);stroke-width:1;stroke-dasharray:2 3;opacity:0;transition:opacity .12s}.efpCross.on{opacity:.8}
.efpPt{stroke:var(--card);stroke-width:2;opacity:0;transition:opacity .12s}.efpPt.on{opacity:1}.efpPt.a{fill:var(--ink)}.efpPt.b{fill:var(--gold)}
.efpEmpty{font:500 12px var(--sans);fill:var(--ink45)}
.efpTip{position:absolute;top:2px;z-index:4;pointer-events:none;background:var(--velvet);color:#f6f1e6;border-radius:9px;padding:7px 10px;font-size:11px;box-shadow:0 8px 24px rgba(20,16,10,.22);white-space:nowrap;display:grid;gap:1px;opacity:0;visibility:hidden;transition:opacity .1s}
.efpTip.on{opacity:1;visibility:visible}.efpTipT{color:#cdc4b2;font-size:10.5px}.efpTipV{font:650 14px var(--sans)}.efpTipR{display:flex;justify-content:space-between;gap:16px;color:#cdc4b2}.efpTipR b{color:#fff;font-weight:650}.efpTipH{margin-top:3px;color:#ecd596;font-size:10.5px}
.efpHeat{display:grid;gap:3px;min-width:0}
.efpHeatRow{display:grid;grid-template-columns:34px repeat(24,minmax(0,1fr));gap:3px;align-items:center}.efpHeatRow.one{grid-template-columns:repeat(24,minmax(0,1fr))}
.efpHeatRow>span{font-size:10px;color:var(--ink45);white-space:nowrap}
.efpHc{position:relative;height:26px;border-radius:5px;background:var(--paper2);overflow:hidden;cursor:default;outline:none}
.efpHc:before{content:"";position:absolute;inset:0;background:var(--gold);opacity:var(--a,0);transition:opacity .45s ease}.efpHc.x{background:repeating-linear-gradient(45deg,var(--line2) 0 3px,transparent 3px 6px)}
.efpHc:hover,.efpHc:focus-visible{box-shadow:0 0 0 2px var(--ink)}
.efpHeatAx{display:grid;grid-template-columns:34px repeat(24,minmax(0,1fr));gap:3px;font-size:10px;color:var(--ink45)}.efpHeatAx.one{grid-template-columns:repeat(24,minmax(0,1fr))}.efpHeatAx span{text-align:center;white-space:nowrap;overflow:visible}
.efpMix{display:grid;grid-template-columns:150px minmax(0,1fr);gap:8px 20px;align-items:center}
.efpDonut{position:relative;width:150px;height:150px}.efpDonut svg{display:block;width:150px;height:150px;overflow:visible}
.efpArc{fill:none;stroke-width:17;transition:transform .2s ease,opacity .2s ease;transform-origin:75px 75px;cursor:default}.efpDonut.has .efpArc{opacity:.45}.efpDonut.has .efpArc.on{opacity:1;transform:scale(1.045)}
.efpDc{position:absolute;inset:0;display:grid;place-content:center;text-align:center;pointer-events:none;gap:1px}.efpDc b{font:600 22px/1 var(--sans);letter-spacing:-.02em;font-variant-numeric:tabular-nums}.efpDc span{font-size:10.5px;color:var(--ink45);max-width:84px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.efpMixL{display:grid;gap:2px;min-width:0}
.efpMr{display:grid;grid-template-columns:10px minmax(0,1fr) auto auto;gap:4px 9px;align-items:center;padding:5px 7px;margin:0 -7px;border-radius:7px;font-size:12px;cursor:default;transition:background .15s}.efpMr:hover,.efpMr.on{background:var(--paper2)}
.efpMr i{width:9px;height:9px;border-radius:3px}.efpMr b{font-variant-numeric:tabular-nums;font-weight:650}.efpMr em{font-style:normal;color:var(--ink45);font-size:11px;min-width:34px;text-align:right;font-variant-numeric:tabular-nums}.efpMr span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.efpMr u{grid-column:2/-1;height:3px;border-radius:2px;background:var(--line2);text-decoration:none;display:block;overflow:hidden;margin-top:-1px}.efpMr u s{display:block;height:100%;border-radius:2px;transition:width .5s ease}
.efpCal{display:grid;gap:4px;min-width:0}
.efpCalHd,.efpCalRow{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:4px}.efpCalHd span{font-size:10px;color:var(--ink45);text-align:center;letter-spacing:.05em}
.efpDy{position:relative;height:34px;border:1px solid transparent;border-radius:8px;background:var(--card2);color:var(--ink70);font:600 11px var(--sans);display:grid;place-items:center;padding:0;cursor:pointer;transition:transform .15s ease,box-shadow .15s ease,border-color .15s ease;font-variant-numeric:tabular-nums}
.efpDy:hover,.efpDy:focus-visible{transform:scale(1.07);z-index:2;box-shadow:0 4px 14px rgba(30,26,20,.16);outline:none;border-color:var(--ink)}
.efpDy.worked{background:#dfe8d6;border-color:#c5d5b9;color:#2f4a2b}.efpDy.partial{background:var(--goldSoft);border-color:var(--goldLine);color:#6a4d17}
.efpDy.off{background:var(--card2);border-color:var(--line);color:var(--ink45);background-image:repeating-linear-gradient(135deg,transparent 0 5px,rgba(176,86,63,.1) 5px 6px)}
.efpDy.future{background:transparent;border:1px dashed var(--line);color:var(--ink25);cursor:default}.efpDy.future:hover{transform:none;box-shadow:none;border-color:var(--line)}
.efpDy.unknown,.efpDy.before{background:transparent;border:1px solid var(--line2);color:var(--ink25)}
.efpDy.closed{background:var(--paper2);border-color:var(--line);color:var(--ink45);background-image:repeating-linear-gradient(45deg,transparent 0 4px,rgba(30,26,20,.08) 4px 5px)}
.efpDy.pending{background:transparent;border:1px solid var(--goldLine);color:var(--ink45)}
.efpDy.pad{visibility:hidden;pointer-events:none}.efpDy.sel{box-shadow:0 0 0 2px var(--gold)}.efpDy.today{border-color:var(--gold)}
.efpCal.long .efpDy{height:auto;aspect-ratio:1;min-height:11px;border-radius:3px;font-size:0}
.efpCal.long{grid-template-columns:auto minmax(0,1fr);gap:3px 6px}.efpCalW{display:grid;gap:3px;grid-auto-flow:column;grid-auto-columns:minmax(0,1fr);min-width:0}.efpCalW>div{display:grid;gap:3px;grid-template-rows:repeat(7,auto)}
.efpCalD{display:grid;gap:3px;grid-template-rows:repeat(7,auto);font-size:9.5px;color:var(--ink45);margin-top:14px}.efpCalD span{aspect-ratio:auto;display:flex;align-items:center;line-height:1}
.efpCalM{display:grid;grid-auto-flow:column;grid-auto-columns:minmax(0,1fr);gap:3px;height:12px;font-size:9.5px;color:var(--ink45);grid-column:2}.efpCalM span{white-space:nowrap;overflow:visible}
.efpKey{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:11px;color:var(--ink45);margin-top:2px}.efpKey span{display:inline-flex;align-items:center;gap:5px}.efpKey i{width:11px;height:11px;border-radius:3px;border:1px solid var(--line);display:inline-block}
.efpKey .w{background:#dfe8d6;border-color:#c5d5b9}.efpKey .p{background:var(--goldSoft);border-color:var(--goldLine)}.efpKey .o{background:repeating-linear-gradient(135deg,transparent 0 3px,rgba(176,86,63,.25) 3px 4px)}.efpKey .f{border-style:dashed;background:transparent}.efpKey .c{background:var(--paper2) repeating-linear-gradient(45deg,transparent 0 3px,rgba(30,26,20,.14) 3px 4px)}
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
.efpRtH{display:flex;align-items:baseline;gap:8px;font-size:12px}.efpRtH b{font-weight:700}.efpRtH span{margin-left:auto;color:var(--ink70);font-variant-numeric:tabular-nums}.efpRtH em{font-style:normal;color:var(--ink45);font-size:11px}.efpRtH em.efpD{font-weight:700;font-variant-numeric:tabular-nums;padding:0 5px;border-radius:5px;background:var(--paper2);color:var(--ink70)}.efpRtH em.efpD.up{background:var(--sageSoft);color:#46623f}.efpRtH em.efpD.down{background:var(--claySoft);color:#8a3f2b}
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
.efpK .efpKT{position:absolute;right:12px;bottom:9px;font:700 8.5px var(--sans);letter-spacing:.05em;text-transform:uppercase;color:var(--ink25)}
.efpShift{display:grid;gap:10px;padding:2px 0}.efpShiftT{display:flex;flex-wrap:wrap;gap:4px 18px;font-size:12.5px;color:var(--ink70)}.efpShiftT b{color:var(--ink);font-weight:650;font-variant-numeric:tabular-nums}
.efpSB{display:flex;height:12px;border-radius:6px;overflow:hidden;background:var(--line2);gap:2px;position:relative}.efpSB i{display:block;height:100%;width:0;transition:width .6s cubic-bezier(.2,.8,.2,1);cursor:default}.efpSB .a{background:#6f6a62}.efpSB .i{background:var(--gold2)}.efpSB .u{background:var(--line)}
.efpSLeg{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:11.5px;color:var(--ink45)}.efpSLeg span{display:inline-flex;gap:6px;align-items:center}.efpSLeg i{width:9px;height:9px;border-radius:3px;display:inline-block}
.efpSum{display:flex;flex-wrap:wrap;gap:6px;padding:10px 18px 4px}.efpSum span{display:inline-flex;gap:5px;align-items:baseline;border:1px solid var(--line);border-radius:999px;padding:2px 10px;font-size:11px;color:var(--ink70);background:var(--card2)}.efpSum b{color:var(--ink);font-weight:700;font-variant-numeric:tabular-nums}
.efpAt{display:inline-block;font:700 9px var(--sans);letter-spacing:.06em;text-transform:uppercase;padding:1px 6px;border-radius:4px;margin-left:8px;vertical-align:1px;background:var(--paper2);color:var(--ink70)}.efpAt.own{background:var(--goldSoft);color:#7a5a1d}.efpAt.system{background:var(--slateSoft);color:#35525d}
.efpHow{padding:2px 0 6px;color:var(--ink45);font-size:11.5px;line-height:1.5}
.efpSub{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--ink45);font-weight:750;margin:6px 0 -4px}
.efpStat{display:flex;flex-wrap:wrap;gap:4px 18px;font-size:12px;color:var(--ink70)}.efpStat b{color:var(--ink);font-weight:650;font-variant-numeric:tabular-nums;margin-left:4px}
.efpCannot{display:grid;gap:3px;margin:2px 2px 0;color:var(--ink45);font-size:11.5px;line-height:1.5}.efpCannot b{color:var(--ink70);font-weight:650}
.efpFound{display:flex;align-items:center;gap:9px;padding:9px 14px;border:1px solid var(--line);background:var(--card2);border-radius:10px;color:var(--ink70);font-size:12px}
@container efp (max-width:1180px){.efpKpis{grid-template-columns:repeat(4,minmax(0,1fr))}.efpO{grid-template-columns:88px minmax(130px,1fr) 104px minmax(0,1.6fr) 48px}.efpDay{min-width:132px}}
@container efp (max-width:900px){.efpGrid.g2,.efpGrid.g75,.efpGrid.g57{grid-template-columns:minmax(0,1fr)}.efpKpis{grid-template-columns:repeat(2,minmax(0,1fr))}.efpO{grid-template-columns:78px minmax(0,1fr) 92px 44px;grid-template-areas:"t n s q" "p p p p";gap:6px 10px}.efpO>.efpOt{grid-area:t}.efpO>.efpOn{grid-area:n}.efpO>.efpOs{grid-area:s}.efpO>.efpOp{grid-area:p}.efpO>.efpQr{grid-area:q;width:44px;height:44px}}
@container efp (max-width:640px){.efpAv{width:42px;height:42px;flex-basis:42px;font-size:14px}.efpName{font-size:18px}.efpChips{flex:1 1 100%}.efpBar{gap:6px 8px;padding:6px 8px}.efpSeg{order:1;max-width:100%;overflow-x:auto;scrollbar-width:none}.efpSeg button{padding:4px 9px;flex:0 0 auto}.efpNav{order:2;flex:1 1 100%}.efpDay{flex:1;min-width:0}.efpBusy{order:3}
.efpKpis{gap:8px}.efpK{padding:11px 12px 9px}.efpKV{font-size:25px}.efpKsp{width:56px;right:8px;top:12px}.efpChart{padding:11px 12px 12px}.efpMix{grid-template-columns:minmax(0,1fr);justify-items:center}.efpMixL{width:100%}.efpNowCard{grid-template-columns:auto minmax(0,1fr);padding:11px 12px}.efpNowCard>.efpQr{display:none}
.efpO{grid-template-columns:70px minmax(0,1fr) 44px;grid-template-areas:"t n q" "s s s" "p p p"}.efpO>.efpOs{grid-area:s;display:flex;gap:10px;align-items:baseline}.efpIr{grid-template-columns:78px minmax(0,1fr);grid-template-areas:"t n" "r r"}.efpIr>time{grid-area:t}.efpIr>.efpOid{grid-area:n}.efpIr>span{grid-area:r}.efpIgh,.efpIgl,.efpRates{padding-left:12px;padding-right:12px}.efpFind{padding:10px 12px 6px}.efpOl{padding:0 2px 4px}
.efpHeatRow{grid-template-columns:28px repeat(24,minmax(0,1fr));gap:2px}.efpHeatRow.one{grid-template-columns:repeat(24,minmax(0,1fr))}.efpHeatAx{grid-template-columns:28px repeat(24,minmax(0,1fr));gap:2px}.efpHeatAx.one{grid-template-columns:repeat(24,minmax(0,1fr))}.efpHc{height:22px;border-radius:3px}.efpDy{height:30px}}
@media (prefers-reduced-motion:reduce){.efp *{transition:none!important;animation:none!important}}`;
    doc.head.appendChild(s);
  }

  /* ── the answer, normalised (a figure the data does not know is null = a dash, never a made-up zero) ── */
  const A = v => (Array.isArray(v) ? v : []);
  const first = (o, ...keys) => { if (o && typeof o === "object") for (const k of keys) if (o[k] != null) return o[k]; return undefined; };
  const pct1 = v => { v = num(v); return v == null ? null : Math.round(v * 10) / 10; };
  /** One number worth showing: how to say it (unit), which way is good, its plain definition, whether it is an estimate. (Plain words here are the fallback; the server's own `label` and `def` win.) */
  const CARD = {
    parts: { label: "Parts", unit: "pieces", better: "up", def: "Pieces this person finished at a station (net of undos) in the period.", sp: "parts" },
    orders: { label: "Orders", unit: "orders", better: "up", def: "Distinct orders this person worked on in the period.", sp: "orders" },
    ordersCompleted: { label: "Orders completed", unit: "orders", better: "up", def: "Orders this person finished at a station.", sp: "completes" },
    scans: { label: "Scans", unit: "scans", better: "up", def: "Scan actions logged under this person's name.", sp: "scans" },
    partsPerDay: { label: "Parts per day worked", unit: "pieces/day", better: "up", def: "Parts divided by days worked.", sp: "parts" },
    ordersPerDay: { label: "Orders per day worked", unit: "orders/day", better: "up", def: "Orders divided by days worked.", sp: "orders" },
    partsPerActiveHour: { label: "Parts per active hour", unit: "pieces/hour", better: "up", def: "Parts divided by active hours: time between actions that were less than 5 minutes apart.", sp: "perActiveHour" },
    partsPerSignedHour: { label: "Parts per signed-in hour", unit: "pieces/hour", better: "up", def: "Parts divided by every hour signed in, active or not.", sp: "perSignedHour" },
    bestDay: { label: "Best day", unit: "pieces", better: "up", def: "The day with the most parts in the period." },
    peakHour: { label: "Busiest hour", unit: "hour", better: null, def: "The hour of the day with the most parts." },
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
    daysWorked: { label: "Days worked", unit: "days", better: "up", def: "Days signed in at a station.", sp: "worked" },
    daysOff: { label: "Days off", unit: "days", better: "down", def: "Team working days on which this person never signed in. Weekends and days nobody worked are not counted.", sp: "off" },
    weekdayDaysOff: { label: "Weekday days off", unit: "days", better: "down", def: "Days off that fell Monday to Friday." },
    weekendDaysOff: { label: "Weekend days off", unit: "days", better: null, def: "Days off that fell on a Saturday or Sunday, when the team did work." },
    lateDays: { label: "Late starts", unit: "days", better: "down", def: "Days this person first signed in more than 30 minutes after the rest of the team's usual start." },
    shortDays: { label: "Short days", unit: "days", better: "down", def: "Days signed in for less than half of what the rest of the team did." },
    currentStreak: { label: "Current streak", unit: "days", better: "up", def: "Working days in a row, up to today, signed in." },
    longestStreak: { label: "Longest streak", unit: "days", better: null, def: "The most working days in a row signed in, in this period." },
    attendanceRate: { label: "Attendance", unit: "percent", better: "up", def: "Days worked as a share of the team's working days." },
    issues: { label: "Issues", unit: "count", better: "down", def: "Rejects, errors and undos logged against this person's work, and reprints and re-scans that name them. Signals to look at, not verdicts." },
    issuesPer100Orders: { label: "Issues per 100 orders", unit: "count", better: "down", def: "Issues divided by orders, times 100, so busy and quiet periods compare fairly." },
    firstPassRate: { label: "First-pass success", unit: "percent", better: "up", def: "Orders finished with no reject, error or undo along the way, as a share of orders worked." },
    reversalRate: { label: "Reversed work", unit: "percent", better: "down", def: "Completions that were undone, as a share of completions." },
    rejectRate: { label: "Rejected", unit: "percent", better: "down", def: "Rejects as a share of orders worked." },
    errorRate: { label: "Errors", unit: "percent", better: "down", def: "Errors shown to this person as a share of orders worked." },
    repliesDrafted: { label: "Drafts made", unit: "count", better: null, def: "Times this person asked the Inbox for an AI draft." },
    repliesSent: { label: "Replies sent", unit: "count", better: "up", def: "Replies this person sent to customers from the Inbox." },
    repliesDelivered: { label: "Replies delivered", unit: "count", better: "up", def: "Replies Etsy confirmed were sent." },
    repliesFailed: { label: "Replies failed", unit: "count", better: "down", def: "Replies that did not go through. Each one is listed with its order." },
    repliesEdited: { label: "AI drafts edited", unit: "count", better: null, def: "Replies that started as an AI draft and were changed by this person before sending." },
    deliveredRate: { label: "Delivered", unit: "percent", better: "up", def: "Replies delivered as a share of replies delivered or failed." },
    failureRate: { label: "Failed", unit: "percent", better: "down", def: "Replies failed as a share of replies delivered or failed." },
    timeToFirstReplyMin: { label: "First reply", unit: "minutes", better: "down", def: "The middle time from a customer's first message to this person's first reply." },
    conversationsDone: { label: "Conversations done", unit: "count", better: "up", def: "Conversations this person archived as done." },
    reopened: { label: "Reopened", unit: "count", better: "down", def: "Conversations this person reopened after marking them done." }
  };
  /** Which cards the page shows: [group, [primary keys], [more keys]], and where each one is looked up (kpis | att | rates | contact | issues). */
  const GROUPS = [
    ["Production", ["kpis.parts", "kpis.orders", "kpis.partsPerActiveHour", "kpis.partsPerDay"], ["kpis.partsPerSignedHour", "kpis.ordersPerDay", "kpis.ordersCompleted", "kpis.scans", "kpis.bestDay", "kpis.peakHour"]],
    ["Speed", ["kpis.secPerOrderMedian", "kpis.secPerOrderP90", "kpis.secPerOrderMean", "kpis.secBetweenScansMedian"], ["kpis.secPerScanMean"]],
    ["Time", ["kpis.activeShare", "kpis.signedHours", "kpis.activeHours", "kpis.avgShiftHours"], ["kpis.idleHours", "kpis.unloggedHours", "kpis.avgStart", "kpis.avgEnd"]],
    ["Attendance", ["att.daysWorked", "att.daysOff", "att.lateDays", "att.shortDays"], ["att.weekdayDaysOff", "att.weekendDaysOff", "att.currentStreak", "att.longestStreak", "att.attendanceRate"]],
    ["Quality", ["issues.issues", "issues.issuesPer100Orders", "rates.firstPassRate", "rates.reversalRate"], ["rates.rejectRate", "rates.errorRate"]],
    ["Contact", ["contact.repliesSent", "contact.deliveredRate", "contact.repliesFailed", "contact.timeToFirstReplyMin"], ["contact.repliesDrafted", "contact.repliesDelivered", "contact.repliesEdited", "contact.failureRate", "contact.conversationsDone", "contact.reopened"]]
  ];
  const ALIAS = { "rates.firstPassRate": ["rates.firstPassRate", "rates.firstPass", "kpis.firstPassRate"], "rates.reversalRate": ["rates.reversalRate", "rates.reworkRate"], "kpis.activeShare": ["kpis.activeShare", "rates.activeShare"], "att.attendanceRate": ["att.attendanceRate", "rates.attendanceRate"] };
  /** A server METRIC / RATE (or a bare number) as the page reads it. */
  function metric(key, raw, over) {
    const d = CARD[key] || {}, o = raw && typeof raw === "object" ? raw : null, v0 = o ? first(o, "value", "v") : raw;
    const m = { key, label: String((o && o.label) || d.label || kindWords(key)), unit: String((o && o.unit) || d.unit || "count"), v: num(v0), prev: o ? num(first(o, "prev", "previous")) : null, delta: o ? num(o.delta) : null, deltaPct: o ? num(o.deltaPct) : null, better: o && "better" in o ? o.better : d.better != null ? d.better : null,
      def: String((o && first(o, "def", "definition", "text")) || d.def || ""), est: !!(o && first(o, "estimated", "est")), why: String((o && o.why) || ""), n: o ? num(o.n) : null, window: !!(o && o.window), day: o && o.day ? String(o.day) : "", num: o ? num(first(o, "num", "numerator")) : null, den: o ? num(first(o, "den", "denominator")) : null };
    if (m.delta == null && m.v != null && m.prev != null) m.delta = m.v - m.prev;
    if (m.deltaPct == null && m.v != null && m.prev != null && m.prev !== 0) m.deltaPct = (m.v - m.prev) / Math.abs(m.prev) * 100;
    return Object.assign(m, over || {});
  }
  const kindWords = k => String(k || "").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").toLowerCase().replace(/^./, c => c.toUpperCase());
  const sumK = (list, f) => { let s = null; for (const x of list) { const v = f(x); if (v != null) s = (s || 0) + v; } return s; };
  function normPoint(p) {
    p = p || {};
    return { day: String(p.day || ""), to: isDay(p.to) ? p.to : String(p.day || ""), days: num(p.days) || 1, workedDays: num(p.workedDays), hasData: p.hasData !== false && p.parts !== null, parts: num(p.parts), orders: num(p.orders), scans: num(p.scans), completes: num(p.completes), prints: num(p.prints), rejects: num(p.rejects), errors: num(p.errors), undos: num(p.undos),
      activeMs: num(p.activeMs), idleMs: num(p.idleMs), signedMs: num(p.signedMs), perActiveHour: num(p.perActiveHour), perSignedHour: num(p.perSignedHour), secPerOrder: num(p.secPerOrder), medianSecPerOrder: num(p.medianSecPerOrder), firstIn: num(p.firstIn), lastOut: num(p.lastOut), shiftMs: num(p.shiftMs), state: p.state ? String(p.state) : "" };
  }
  const CAL = { worked: 1, partial: 1, off: 1, closed: 1, before: 1, future: 1, pending: 1 };
  /** The `person` answer, in the shapes of plans/employee-hr/api.md; every field is optional. */
  function norm(r, req) {
    r = r || {}; req = req || {};
    const series = A(r.series).filter(p => p && p.day).map(normPoint), hoursRaw = A(r.hours);
    const hours = hoursRaw.length ? Array.from({ length: 24 }, (_, h) => { const x = hoursRaw.find(e => e && +e.hour === h) || hoursRaw[h] || {}; return { hour: h, parts: num(x.parts), scans: num(x.scans), perDay: num(x.perDay) }; }) : [];
    const stations = A(r.stations).filter(s => s && s.station).map(s => ({ station: String(s.station), label: String(s.label || stName(s.station)), parts: num(s.parts), orders: num(s.orders), scans: num(s.scans), completes: num(s.completes), prints: num(s.prints), minutes: num(s.minutes), shareParts: num(s.shareParts), shareMinutes: num(s.shareMinutes), perActiveHour: num(s.perActiveHour) }));
    const cal = A(r.calendar).filter(c => c && c.day).map(c => ({ day: String(c.day), state: CAL[c.state] ? String(c.state) : "before", signedMs: num(c.signedMs), activeMs: num(c.activeMs), firstIn: num(c.firstIn), lastOut: num(c.lastOut), parts: num(c.parts), orders: num(c.orders), late: !!c.late, short: !!c.short, weekend: !!c.weekend, others: num(c.others) })).sort((a, b) => (a.day < b.day ? -1 : 1));
    const src = { kpis: {}, att: {}, rates: {}, contact: {}, issues: {} };
    for (const k of Object.keys(r.kpis || {})) src.kpis[k] = metric(k, r.kpis[k]);
    // attendance (E9): metrics as METRIC objects or plain numbers, streaks, the average shift
    const at = r.attendance && typeof r.attendance === "object" ? r.attendance : {}, am = at.metrics && typeof at.metrics === "object" ? at.metrics : at;
    for (const k of ["daysWorked", "daysOff", "weekdayDaysOff", "weekendDaysOff", "teamDays", "workingDays", "lateDays", "shortDays", "currentStreak", "longestStreak", "attendanceRate"]) if (am[k] != null) src.att[k] = metric(k, am[k]);
    if (at.streaks && typeof at.streaks === "object") { if (src.att.currentStreak == null && at.streaks.current != null) src.att.currentStreak = metric("currentStreak", at.streaks.current); if (src.att.longestStreak == null && at.streaks.best != null) src.att.longestStreak = metric("longestStreak", at.streaks.best); }
    if (at.avgShiftMs != null && !src.kpis.avgShiftHours) src.kpis.avgShiftHours = metric("avgShiftHours", num(at.avgShiftMs) == null ? null : at.avgShiftMs / HOUR_MS);
    const defs = at.definitions || at.defs || {}; for (const k of Object.keys(src.att)) if (!src.att[k].def && typeof defs[k] === "string") src.att[k].def = defs[k];
    // issues (E10)
    const iraw = r.issues && typeof r.issues === "object" ? r.issues : null, items = A(iraw && iraw.items).filter(i => i && i.kind != null).map(i => ({ at: num(i.at), day: String(i.day || ""), rid: i.rid != null ? String(i.rid) : "", number: i.number != null ? String(i.number) : "", kind: String(i.kind), label: String(i.label || kindWords(i.kind)), station: String(i.station || ""), note: String(i.note || i.reason || "") })).sort((a, b) => nz(b.at) - nz(a.at));
    const byKind = A(iraw && iraw.byKind).filter(k => k && k.kind != null).map(k => ({ kind: String(k.kind), label: String(k.label || kindWords(k.kind)), count: nz(k.count), per100: num(k.per100Orders), coverage: String(k.coverage || "range"), attribution: String(k.attribution || ""), def: String(k.def || k.definition || ""), how: String(k.how || ""), est: !!k.estimated, prev: num(k.prev), delta: num(k.delta) })).sort((a, b) => b.count - a.count);
    const issues = iraw ? { total: num(iraw.total), own: num(iraw.own), system: num(iraw.system), per100: num(iraw.per100Orders), byKind, items, capped: !!iraw.itemsCapped } : null;
    if (iraw) { src.issues.issues = metric("issues", iraw.metric || { value: iraw.total, prev: iraw.prevTotal, delta: iraw.delta, deltaPct: iraw.deltaPct, better: "down", def: iraw.def }); src.issues.issuesPer100Orders = metric("issuesPer100Orders", { value: iraw.per100Orders, prev: iraw.prevPer100Orders, better: "down" }); }
    // rates (E10 / E4): each a RATE { label, value (percent), num, den, prev, delta, better, def, estimated }
    const rr = r.rates && typeof r.rates === "object" && !Array.isArray(r.rates) ? r.rates : {};
    for (const k of Object.keys(rr)) src.rates[k] = metric(k, rr[k] && typeof rr[k] === "object" ? Object.assign({ unit: "percent" }, rr[k]) : { value: rr[k], unit: "percent" }, { unit: "percent" });
    // contact (E10): METRICs under .metrics, or plain fields (sent, delivered, failed, edited, medianFirstReplyMs ...)
    const cr = r.contact && typeof r.contact === "object" ? r.contact : null, cm = cr && cr.metrics && typeof cr.metrics === "object" ? cr.metrics : {};
    if (cr) {
      for (const k of Object.keys(cm)) src.contact[k] = metric(k, cm[k]);
      const flat = { repliesSent: "sent", repliesDelivered: "delivered", repliesFailed: "failed", repliesEdited: "edited", repliesDrafted: "drafted", conversationsDone: "conversationsDone", reopened: "reopened" };
      for (const k of Object.keys(flat)) if (!src.contact[k] && cr[flat[k]] != null) src.contact[k] = metric(k, cr[flat[k]]);
      if (!src.contact.timeToFirstReplyMin && cr.medianFirstReplyMs != null) src.contact.timeToFirstReplyMin = metric("timeToFirstReplyMin", num(cr.medianFirstReplyMs) == null ? null : cr.medianFirstReplyMs / 60000);
      const s = src.contact.repliesSent && src.contact.repliesSent.v, dl = src.contact.repliesDelivered && src.contact.repliesDelivered.v, fl = src.contact.repliesFailed && src.contact.repliesFailed.v;
      if (!src.contact.deliveredRate && dl != null && fl != null && dl + fl > 0) src.contact.deliveredRate = metric("deliveredRate", dl / (dl + fl) * 100, { unit: "percent" });
      if (!src.contact.failureRate && dl != null && fl != null && dl + fl > 0) src.contact.failureRate = metric("failureRate", fl / (dl + fl) * 100, { unit: "percent" });
    }
    const contact = cr ? { available: cr.available !== false, source: String(cr.source || ""), metrics: src.contact } : null;
    // what the days add up to, where the server sent none
    const fill = (ns, k, v) => { if (!src[ns][k]) src[ns][k] = metric(k, v, { derived: true }); else if (src[ns][k].v == null && v != null) { src[ns][k].v = v; src[ns][k].derived = true; } };
    if (series.length || cal.length) {
      if (series.length) { fill("kpis", "parts", sumK(series, p => p.parts)); fill("kpis", "orders", sumK(series, p => p.orders)); fill("kpis", "scans", sumK(series, p => p.scans)); const sg = sumK(series, p => p.signedMs), ac = sumK(series, p => p.activeMs); if (sg != null) fill("kpis", "signedHours", sg / HOUR_MS); if (ac != null) fill("kpis", "activeHours", ac / HOUR_MS); if (sg > 0 && ac != null) fill("kpis", "activeShare", Math.min(100, ac / sg * 100)); }
      if (cal.length) { fill("att", "daysWorked", cal.filter(c => c.state === "worked" || c.state === "partial").length); fill("att", "daysOff", cal.filter(c => c.state === "off").length); }
    }
    const last = cal.filter(c => c.signedMs > 0 || c.lastOut || c.firstIn).pop(), lastSeen = last ? (last.lastOut || last.firstIn) : null;
    return { name: String(r.name || req.name || ""), found: r.found !== false, spellings: A(r.spellings).map(String), mode: String(r.mode || ""), range: r.range, from: isDay(r.from) ? r.from : req.from, to: isDay(r.to) ? r.to : req.to, days: num(r.days), today: isDay(r.today) ? r.today : "", live: !!r.live, prev: r.prev && r.prev.from ? { from: String(r.prev.from), to: String(r.prev.to), days: num(r.prev.days) } : null,
      granularity: r.granularity === "week" ? "week" : "day", trackingStart: isDay(r.trackingStart) ? r.trackingStart : "", firstDay: isDay(r.firstDay) ? r.firstDay : "", eventWindow: r.eventWindow && r.eventWindow.from ? r.eventWindow : null, rules: r.rules || {},
      src, series, hours, stations, cal, att: r.attendance || null, issues, contact, cannotTell: A(r.cannotTell).filter(c => c && c.text).map(c => ({ topic: String(c.topic || ""), text: String(c.text) })), notes: A(r.notes).map(String).filter(Boolean), partial: !!r.partial, errors: A(r.errors).map(String), now: num(r.now), lastSeen, lastStation: "" };
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
  function pickLive(r, name) {
    const me = slug(name), out = { at: num(r && r.at), where: null, current: [], stationKey: "" };
    if (!r) return out;
    const sig = A(r.signedIn).find(s => s && slug(s.name) === me);
    if (sig) { out.where = { name: String(sig.name), stationKey: String(sig.stationKey || ""), since: num(sig.since), lastSeenAt: num(sig.lastSeenAt) }; out.stationKey = out.where.stationKey; }
    const seen = new Set(), add = (c, st) => { if (!c || slug(c.person) !== me) return; const id = String(c.rid || c.orderNumber || ""); if (seen.has(id)) return; seen.add(id); out.current.push(Object.assign({}, c, { stationKey: c.station || (st && st.key) || "", stationLabel: c.stationLabel || (st && st.label) || stName(c.station || (st && st.key)) })); };
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

  /* ── charts: inline SVG, thin marks, one baseline, the current period in gold ── */
  const SVG = "http://www.w3.org/2000/svg";
  const mk = (parent, name, attrs) => { const e = doc.createElementNS(SVG, name); for (const k in attrs) e.setAttribute(k, attrs[k]); parent.appendChild(e); return e; };
  function resample(vals, n) {
    const m = vals.length; if (m === n) return vals.slice(); if (!m) return new Array(n).fill(null);
    const out = []; for (let j = 0; j < n; j++) { const p = n === 1 ? 0 : (j / (n - 1)) * (m - 1), a = Math.floor(p), b = Math.min(m - 1, a + 1), t = p - a, va = vals[a], vb = vals[b]; out.push(va == null || vb == null ? (t < .5 ? va : vb) : va + (vb - va) * t); }
    return out;
  }
  const lerpArr = (a, b, k) => b.map((v, i) => (v == null || a[i] == null ? v : a[i] + (v - a[i]) * k));
  /** Columns, lines and a band over one shared x axis. .set({ labels, layers:[{ type:'bars'|'line'|'band', cls, a, b, w }], ref, refLabel, hi, tips, yFmt, pick, empty, name }) */
  function xy(host, o) {
    const H = o.height || 150, L = o.left || 40, R = 6, T0 = 12, B = 22;
    const S = { W: 0, n: 0, svg: null, d: null, cur: null, stop: null, idx: -1, geo: null, lsig: "" };
    host.classList.add("efpXY");
    const tip = el("div", "efpTip"); host.appendChild(tip);
    function build(W, n, layers) {
      if (S.svg) S.svg.remove();
      const svg = S.svg = doc.createElementNS(SVG, "svg"); for (const [k, v] of Object.entries({ viewBox: `0 0 ${W} ${H}`, width: W, height: H, class: "efpSvg", tabindex: "0", role: "img" })) svg.setAttribute(k, v);
      host.insertBefore(svg, tip);
      const pw = W - L - R, ph = H - T0 - B, slot = pw / n; S.W = W; S.n = n;
      S.geo = { pw, ph, slot, cw: Math.max(2, Math.min(o.maxW || 26, slot - (o.gap == null ? 4 : o.gap))), base: T0 + ph };
      S.grid = [0, .5, 1].map(f => { const y = T0 + ph * (1 - f); return { ln: mk(svg, "line", { x1: L, x2: W - R, y1: y, y2: y, class: f ? "efpGridL" : "efpBase" }), tx: f ? mk(svg, "text", { x: L - 6, y: y + 3.5, class: "efpTick", "text-anchor": "end" }) : null }; });
      S.xl = []; for (let i = 0; i < n; i++) S.xl[i] = mk(svg, "text", { x: L + slot * (i + .5), y: H - 6, class: "efpXl", "text-anchor": "middle" });
      S.els = layers.map(l => l.type === "bars" ? { bars: Array.from({ length: n }, () => mk(svg, "path", { class: "efpBar0 " + (l.cls || "") })) } : l.type === "band" ? { band: mk(svg, "path", { class: "efpBand" }) } : { line: mk(svg, "path", { class: "efpLn " + (l.cls || "a") }) });
      S.ref = mk(svg, "line", { class: "efpRef", display: "none" }); S.refT = mk(svg, "text", { class: "efpRefT", "text-anchor": "end", display: "none" });
      S.emp = mk(svg, "text", { class: "efpEmpty", "text-anchor": "middle", x: L + pw / 2, y: T0 + ph / 2 + 4, display: "none" });
      S.cross = mk(svg, "line", { class: "efpCross", y1: T0, y2: T0 + ph });
      S.pts = layers.map(l => (l.type === "line" ? mk(svg, "circle", { r: 4, class: "efpPt " + (l.cls || "a") }) : null));
      S.hit = mk(svg, "rect", { x: L, y: 0, width: pw, height: H, fill: "transparent" });
      const at = e => { const r = svg.getBoundingClientRect(), x = (e.clientX - r.left) * (W / (r.width || W)); return Math.max(0, Math.min(n - 1, Math.floor((x - L) / slot))); };
      S.hit.addEventListener("pointermove", e => show(at(e))); S.hit.addEventListener("pointerdown", e => show(at(e)));
      S.hit.addEventListener("click", e => { const i = at(e), t = S.d && S.d.tips && S.d.tips[i]; if (S.d && S.d.pick && t && t.pick) S.d.pick(i); });
      svg.addEventListener("pointerleave", hide); svg.addEventListener("blur", hide);
      svg.addEventListener("keydown", e => {
        const k = e.key; if (!["ArrowLeft", "ArrowRight", "Home", "End", "Escape", "Enter"].includes(k)) return;
        e.preventDefault(); if (k === "Escape") return hide();
        if (k === "Enter") { const t = S.d && S.d.tips && S.d.tips[S.idx]; if (S.d && S.d.pick && t && t.pick) S.d.pick(S.idx); return; }
        show(k === "Home" ? 0 : k === "End" ? n - 1 : Math.max(0, Math.min(n - 1, (S.idx < 0 ? (S.d && S.d.hi >= 0 ? S.d.hi : 0) : S.idx) + (k === "ArrowRight" ? 1 : -1))));
      });
    }
    const yOf = (v, max) => S.geo.base - (Math.max(0, v) / max) * S.geo.ph;
    function paint(cur, d) {
      const { ph, slot, cw, base } = S.geo, max = cur.max;
      cur.layers.forEach((cl, li) => {
        const ld = d.layers[li], e = S.els[li];
        if (ld.type === "bars") {
          const w = cw * (ld.w || 1);
          for (let i = 0; i < S.n; i++) {
            const v = cl.a[i], h = v != null && v > 0 ? Math.max(1.5, v / max * ph) : 0, x = L + slot * i + (slot - w) / 2, y = base - h, r = Math.min(3, h, w / 2);
            e.bars[i].setAttribute("d", h ? `M${x.toFixed(1)},${base}V${(y + r).toFixed(1)}Q${x.toFixed(1)},${y.toFixed(1)} ${(x + r).toFixed(1)},${y.toFixed(1)}H${(x + w - r).toFixed(1)}Q${(x + w).toFixed(1)},${y.toFixed(1)} ${(x + w).toFixed(1)},${(y + r).toFixed(1)}V${base}Z` : "");
          }
        } else if (ld.type === "line") {
          let p = "", pen = false; for (let i = 0; i < S.n; i++) { const v = cl.a[i]; if (v == null) { pen = false; continue; } p += `${pen ? "L" : "M"}${(L + slot * (i + .5)).toFixed(1)},${yOf(v, max).toFixed(1)}`; pen = true; }
          e.line.setAttribute("d", p);
        } else {
          let p = "", run = []; const flush = () => { if (run.length > 1) { p += "M" + run.map(i => `${(L + slot * (i + .5)).toFixed(1)},${yOf(cl.b[i], max).toFixed(1)}`).join("L") + "L" + run.slice().reverse().map(i => `${(L + slot * (i + .5)).toFixed(1)},${yOf(cl.a[i], max).toFixed(1)}`).join("L") + "Z"; } run = []; };
          for (let i = 0; i < S.n; i++) { if (cl.a[i] == null || cl.b[i] == null) flush(); else run.push(i); } flush(); e.band.setAttribute("d", p);
        }
      });
      const fy = d.yFmt || nf; setText(S.grid[1].tx, fy(max / 2)); setText(S.grid[2].tx, fy(max));
      if (d.ref != null && d.ref > 0 && d.ref <= max) { const y = yOf(d.ref, max); for (const [k, v] of Object.entries({ x1: L, x2: S.W - R, y1: y, y2: y })) S.ref.setAttribute(k, v); S.ref.removeAttribute("display"); S.refT.setAttribute("x", S.W - R - 2); S.refT.setAttribute("y", Math.max(9, y - 4)); setText(S.refT, d.refLabel || ""); S.refT.removeAttribute("display"); } else { S.ref.setAttribute("display", "none"); S.refT.setAttribute("display", "none"); }
      if (S.idx >= 0) marks(S.idx);
    }
    function marks(i) {
      const cx = L + S.geo.slot * (i + .5); S.cross.setAttribute("x1", cx); S.cross.setAttribute("x2", cx); S.cross.classList.add("on");
      S.d.layers.forEach((l, li) => { if (l.type === "bars") S.els[li].bars.forEach((b, bi) => b.classList.toggle("hov", bi === i)); else if (l.type === "line" && S.pts[li]) { const v = S.cur.layers[li].a[i], p = S.pts[li]; if (v == null) p.classList.remove("on"); else { p.setAttribute("cx", cx); p.setAttribute("cy", yOf(v, S.cur.max)); p.classList.add("on"); } } });
    }
    function show(i) {
      const d = S.d; if (!d || i < 0 || i >= S.n) return hide();
      S.idx = i; marks(i);
      const t = (d.tips && d.tips[i]) || { t: d.labels[i], v: "" };
      tip.textContent = ""; tip.appendChild(el("div", "efpTipT")).textContent = t.t; if (t.v) tip.appendChild(el("div", "efpTipV")).textContent = t.v;
      for (const [k, v] of t.rows || []) { const r = tip.appendChild(el("div", "efpTipR")); r.appendChild(el("span")).textContent = k; r.appendChild(el("b")).textContent = v; }
      if (t.hint) tip.appendChild(el("div", "efpTipH")).textContent = t.hint;
      tip.classList.add("on");
      const w = tip.offsetWidth || 130, cx = L + S.geo.slot * (i + .5);
      tip.style.left = Math.max(2, Math.min(S.W - w - 2, cx > S.W * .55 ? cx - w - 14 : cx + 14)) + "px";
      S.hit.style.cursor = d.pick && t.pick ? "pointer" : "default";
    }
    function hide() { S.idx = -1; tip.classList.remove("on"); if (S.cross) S.cross.classList.remove("on"); if (S.pts) S.pts.forEach(p => p && p.classList.remove("on")); if (S.els && S.d) S.d.layers.forEach((l, li) => { if (l.type === "bars") S.els[li].bars.forEach(b => b.classList.remove("hov")); }); }
    function set(d) {
      const n = d.labels.length || 1, W = Math.max(200, Math.floor(host.clientWidth || 0)), lsig = d.layers.map(l => l.type + (l.cls || "")).join();
      const prev = S.cur && S.lsig === lsig ? S.cur : null; S.d = d;
      const to = { lsig, max: d.max || niceMax(Math.max(0, ...d.layers.flatMap(l => (l.type === "band" ? l.b : l.a).filter(v => v != null)), d.ref || 0)), layers: d.layers.map(l => ({ a: l.a.slice(), b: l.b ? l.b.slice() : null })) };
      to.max = Math.max(to.max, 1e-9);
      const rebuilt = !S.svg || S.W !== W || S.n !== n || S.lsig !== lsig;
      if (rebuilt) { build(W, n, d.layers); S.lsig = lsig; }
      const step = d.step || Math.max(1, Math.ceil((o.labelW || 40) / S.geo.slot));
      S.xl.forEach((t, i) => { const lab = i % step === 0 || i === d.hi ? d.labels[i] || "" : ""; setText(t, lab); t.classList.toggle("on", i === d.hi); });
      S.svg.setAttribute("aria-label", `${d.name || o.name}: ` + d.labels.map((t, i) => (d.tips && d.tips[i] ? `${d.tips[i].t} ${d.tips[i].v || ""}` : t)).filter(Boolean).slice(0, 60).join(", "));
      if (d.empty) { setText(S.emp, d.empty); S.emp.removeAttribute("display"); } else S.emp.setAttribute("display", "none");
      const sig = JSON.stringify([to.layers, to.max, d.ref, d.hi, W]);
      if (!rebuilt && S.sig === sig) { if (S.idx >= 0) show(Math.min(S.idx, S.n - 1)); return; }
      S.sig = sig; if (S.stop) S.stop();
      S.layersD = d.layers; S.els.forEach((e, li) => { if (e.bars) e.bars.forEach((b, i) => { b.classList.toggle("hi", i === d.hi); b.classList.toggle("pickable", !!(d.pick && d.tips && d.tips[i] && d.tips[i].pick)); }); });
      const from = prev ? { max: prev.max, layers: prev.layers.map((pl, li) => ({ a: resample(pl.a, n), b: pl.b ? resample(pl.b, n) : null })) } : { max: to.max, layers: to.layers.map((l, li) => ({ a: l.a.map(v => (d.layers[li].type === "line" ? 0 : 0)), b: l.b ? l.b.map(() => 0) : null })) };
      S.stop = tween(options.growMs, k => { S.cur = { lsig, max: from.max + (to.max - from.max) * k, layers: to.layers.map((l, li) => ({ a: lerpArr(from.layers[li].a, l.a, k).map((v, i) => (l.a[i] == null ? null : v)), b: l.b ? lerpArr(from.layers[li].b, l.b, k).map((v, i) => (l.b[i] == null ? null : v)) : null })) }; paint(S.cur, d); }, () => { S.cur = to; paint(to, d); });
      if (S.idx >= 0) show(Math.min(S.idx, S.n - 1));
    }
    if (root.ResizeObserver) { const ro = new ResizeObserver(() => { if (S.d && host.clientWidth && Math.floor(host.clientWidth) !== S.W) set(S.d); }); ro.observe(host); S.ro = ro; }
    return { set, hide, show, destroy() { if (S.stop) S.stop(); if (S.ro) S.ro.disconnect(); }, get svg() { return S.svg; } };
  }
  /** A small line (its own scale, a dot on the last known point); a gap in the data is a gap in the line. */
  function spark(host, o) {
    const w = o.w || 76, h = o.h || 26, S = { cur: null, stop: null, svg: mk(host, "svg", { viewBox: `0 0 ${w} ${h}`, width: w, height: h, class: "efpSpark", "aria-hidden": "true" }) };
    const area = mk(S.svg, "path", { class: "efpSA" }), line = mk(S.svg, "path", { class: "efpSL" }), dot = mk(S.svg, "circle", { r: 2.6, class: "efpSD" });
    function paint(v) {
      const n = v.length, known = v.filter(x => x != null), max = Math.max(1e-9, ...known), min = Math.min(0, ...known), x = i => 2 + (n < 2 ? (w - 4) / 2 : i * (w - 4) / (n - 1)), y = a => h - 2.5 - ((a - min) / (max - min)) * (h - 6);
      let d = "", pen = false, last = -1; for (let i = 0; i < n; i++) { if (v[i] == null) { pen = false; continue; } d += (pen ? "L" : "M") + x(i).toFixed(1) + "," + y(v[i]).toFixed(1); pen = true; last = i; }
      line.setAttribute("d", d); area.setAttribute("d", d && last > 0 && v.every(a => a != null) ? `${d}L${x(last).toFixed(1)},${h - 2}L${x(0).toFixed(1)},${h - 2}Z` : "");
      if (last >= 0) { dot.setAttribute("cx", x(last).toFixed(1)); dot.setAttribute("cy", y(v[last]).toFixed(1)); dot.removeAttribute("display"); } else dot.setAttribute("display", "none");
    }
    return { set(vals) {
      const sig = vals.join(); if (S.sig === sig) return; S.sig = sig;
      if (S.stop) S.stop(); const from = S.cur && S.cur.length === vals.length ? S.cur : vals.map(v => (v == null ? null : 0));
      S.stop = tween(options.growMs, k => { S.cur = vals.map((v, i) => (v == null ? null : (from[i] == null ? v : from[i] + (v - from[i]) * k))); paint(S.cur); }, () => { S.cur = vals.slice(); paint(vals); });
    }, destroy() { if (S.stop) S.stop(); } };
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
    const a = root.Efficiency && root.Efficiency.api, f = a && a.fmt && typeof a.fmt.since === "function" ? a.fmt.since : t => durMs(t).replace("<1 s", "0 s").replace(/^0 m$/, "0 s");
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
  function fmtFor(key, unit) {
    if (key === "timeToFirstReplyMin" || unit === "minutes") return v => durMs(v * 60000);
    if (key === "peakHour") return v => (unit === "clock" ? clockMin(v) : hourLabel(Math.round(v) % 24));
    if (key === "issuesPer100Orders") return nf1;
    switch (unit) {
      case "hours": return v => hoursTxt(v * HOUR_MS); case "seconds": return secTxt; case "percent": return pctVal; case "clock": return clockMin;
      case "pieces/hour": case "orders/hour": return rateTxt; case "pieces/day": case "orders/day": return nf1; case "hour": return v => hourLabel(Math.round(v) % 24);
      default: return nf;
    }
  }
  const metricAt = (M, full) => { for (const name of ALIAS[full] || [full]) { const [ns, k] = name.split("."); const m = M.src[ns] && M.src[ns][k]; if (m) return m; } return null; };
  const shortKey = full => full.split(".")[1];

  /* ════════════ the page ════════════ */
  function mount(host, o) {
    o = o || {}; if (!host || !doc) return null; style();
    const EA = () => (root.Efficiency && root.Efficiency.api) || null;
    const now = () => (typeof o.now === "function" ? o.now() : EA() && typeof EA().now === "function" ? EA().now() : Date.now() + S.off), today = () => nyDay(now());
    const S = { name: String(o.name || ""), range: "week", anchor: null, custom: null, following: true, metric: "parts", M: null, cache: new Map(), gen: 0, busy: false, err: "", errShort: "", fails: 0, at: 0, off: 0, ctl: null, locked: false, dead: false,
      live: null, liveAt: 0, liveFails: 0, liveBusy: false, calx: null, calBusy: false, now: new Map(), openKinds: new Set(), kinds: false, moreOpen: new Set(), calW: 0,
      orders: { q: "", list: [], seen: new Set(), next: "", busy: false, gen: 0, err: "", total: null, done: false, loaded: false, typing: false, reset: false, at: 0 } };
    const want = o.range || store.get(RANGE_STORE) || "week"; S.range = RANGES.some(r => r[0] === want && r[0] !== "custom") || want === "custom" && o.custom ? want : "week";
    S.anchor = isDay(o.day) ? o.day : today(); S.custom = o.custom || null; S.following = S.anchor >= today();
    const T = { range: 0, live: 0, cal: 0, orders: 0, tick: 0, deb: 0 };
    const root0 = el("div", "efp in"); root0.setAttribute("aria-label", `${S.name}, employee page`);
    host.textContent = ""; host.appendChild(root0);
    const E = {}; let charts = {}, mini = {}, io = null, ro = null, unsubLive = null, ordersH = null;

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
    <div class="efpCard efpChart" data-c="tp"><div class="efpCH"><span class="efpCT">Throughput</span><span class="seg efpTog" role="group" aria-label="Measure"><button type="button" data-metric="parts">Parts</button><button type="button" data-metric="scans">Scans</button><button type="button" data-metric="orders">Orders</button><button type="button" data-metric="perActiveHour">Per hour</button></span><span class="efpCP"></span></div><div class="efpCh"></div></div></section>
  <div class="efpGrid g2">
    <div class="efpCard efpChart" data-c="sp"><div class="efpCH"><span class="efpCT">Speed per order</span><span class="efpCP"><span class="efpLeg" data-lg="med"><i style="background:var(--ink70)"></i>Median</span><span class="efpLeg"><i style="background:var(--gold2)"></i>Average</span></span></div><div class="efpCh"></div></div>
    <div class="efpCard efpChart" data-c="tm"><div class="efpCH"><span class="efpCT">Active vs signed in</span><span class="efpCP"><span class="efpLeg"><i style="background:var(--line)"></i>Signed in</span><span class="efpLeg"><i style="background:#85807a"></i>Active</span></span></div><div class="efpCh"></div></div>
    <div class="efpCard efpChart wide hidden" data-c="shift"><div class="efpCH"><span class="efpCT">The shift</span><span class="efpCP"></span></div><div class="efpShiftH efpXY"></div></div>
  </div>
  <div class="efpGrid g75">
    <div class="efpCard efpChart" data-c="cal"><div class="efpCH"><span class="efpCT">Days worked</span><span class="efpCP"></span></div><div class="efpCalH"></div><div class="efpKey"><span><i class="w"></i>Worked</span><span><i class="p"></i>Part day</span><span><i class="o"></i>Day off</span><span><i class="c"></i>Team closed</span><span><i class="f"></i>Not yet</span></div></div>
    <div class="efpCard efpChart" data-c="mix"><div class="efpCH"><span class="efpCT">Station mix</span><span class="efpCP"></span></div><div class="efpMixH"></div></div>
  </div>
  <div class="efpCard efpChart" data-c="heat"><div class="efpCH"><span class="efpCT">Busiest hours</span><span class="efpCP"></span></div><div class="efpHeatH"></div></div>
  <div class="efpGrid g57">
    <section aria-label="Issues"><div class="efpLabel">Issues <b class="efpIN"></b></div><div class="efpCard efpIs"></div></section>
    <section aria-label="Success and contact rates"><div class="efpLabel">Success and contact</div><div class="efpCard efpRates"></div></section>
  </div>
  <section aria-label="Orders"><div class="efpLabel">Orders <b class="efpON"></b><span class="efpLr">Everything this person handled</span></div>
    <div class="efpCard efpOrdersHost"><div class="efpOrdersOwn"><div class="efpFind"><label class="efpSearch">${SEARCH}<input type="text" name="q" inputmode="search" autocomplete="off" spellcheck="false" placeholder="Search orders: number, customer or station" aria-label="Search this person's orders"><button type="button" class="efpIcon hidden" data-clear aria-label="Clear the search">✕</button></label><span class="efpSt" role="status"></span></div>
      <div class="efpOl"></div><div class="efpMore"></div></div></div></section>
  <div class="efpCannot hidden"></div>
  <p class="efpFoot">These figures are logged activity, not effort: a phone scan is credited to the signed-in desktop's person, the sorter's name is typed, and label reprints and QA notes are signals, not verdicts. Hover any number for how it is worked out.</p>
</div>
<div class="efpHC" role="tooltip"></div>`;
      const q = s => root0.querySelector(s);
      Object.assign(E, { back: q(".efpBack"), av: q(".efpAv"), name: q(".efpName"), where: q(".efpWhere"), chips: q(".efpChips"), live: q(".efpLive"), liveT: q(".efpLiveT"), bar: q(".efpBar"), day: q(".efpDay"), prev: q('[data-nav="-1"]'), next: q('[data-nav="1"]'), today: q(".efpToday"),
        custom: q(".efpCustom"), busy: q(".efpBusy"), busyT: q(".efpBusyT"), msg: q(".efpMsg"), wait: q(".efpWait"), waitT: q(".efpWaitT"), nowS: q(".efpNowS"), now: q(".efpNow"), body: q(".efpBody"), note: q(".efpNote"), kg: q(".efpKGroups"),
        hc: q(".efpHC"), tpP: q('[data-c="tp"] .efpCP'), calP: q('[data-c="cal"] .efpCP'), mixP: q('[data-c="mix"] .efpCP'), heatP: q('[data-c="heat"] .efpCP'), shiftP: q('[data-c="shift"] .efpCP'), calH: q(".efpCalH"), mixH: q(".efpMixH"), heatH: q(".efpHeatH"), shiftH: q(".efpShiftH"), iN: q(".efpIN"), is: q(".efpIs"), rates: q(".efpRates"),
        oN: q(".efpON"), find: q(".efpSearch input"), clear: q("[data-clear]"), st: q(".efpFind .efpSt"), ol: q(".efpOl"), more: q(".efpMore"), heat: q('[data-c="heat"]'), cSp: q('[data-c="sp"]'), cTm: q('[data-c="tm"]'), cShift: q('[data-c="shift"]'), ordersHost: q(".efpOrdersHost"), ordersOwn: q(".efpOrdersOwn"), cannot: q(".efpCannot"), lg: q('[data-lg="med"]') });
      E.k = {};
      for (const [gname, prim, more] of GROUPS) {
        const g = el("div", "efpGroup"), grid = el("div", "efpKpis"), lab = g.appendChild(el("div", "efpLabel")); lab.appendChild(el("span")).textContent = gname;
        if (more.length) { const b = el("button", "efpLink"); b.type = "button"; b.dataset.moreGroup = gname; b.setAttribute("aria-expanded", "false"); b.textContent = "More figures"; lab.appendChild(b); }
        g.appendChild(grid);
        for (const full of prim.concat(more)) {
          const key = shortKey(full), d = CARD[key], c = el("div", "efpK", `<span class="efpKL"><span class="t"></span></span><b class="efpKV">—</b><span class="efpKD"></span><span class="efpKS"></span><span class="efpKsp"></span><span class="efpKT"></span>`);
          c.tabIndex = 0; c.dataset.k = full; c.setAttribute("role", "group"); setText(c.querySelector(".t"), d.label); if (more.includes(full)) c.classList.add("hidden", "extra");
          E.k[full] = { card: c, val: c.querySelector(".efpKV"), d: c.querySelector(".efpKD"), s: c.querySelector(".efpKS"), tag: c.querySelector(".efpKT"), sp: spark(c.querySelector(".efpKsp"), { w: 76, h: 26 }), t: c.querySelector(".t"), group: gname };
          grid.appendChild(c);
        }
        E.kg.appendChild(g);
      }
      charts = { tp: xy(q('[data-c="tp"] .efpCh'), { height: 168, maxW: 30, name: "Throughput", left: 42 }), sp: xy(q('[data-c="sp"] .efpCh'), { height: 150, maxW: 24, name: "Speed per order", left: 46 }), tm: xy(q('[data-c="tm"] .efpCh'), { height: 150, maxW: 30, name: "Active vs signed in", left: 42 }) };
      mini = { cal: miniTip(E.calH), mix: miniTip(E.mixH), heat: miniTip(E.heatH), rate: miniTip(E.rates), shift: miniTip(E.shiftH) };
      setText(E.name, S.name); setText(E.av, initials(S.name)); E.av.dataset.t = String(tint(S.name));
      root0.addEventListener("click", onClick); root0.addEventListener("submit", onSubmit);
      E.find.addEventListener("input", onSearchInput); E.find.addEventListener("keydown", e => { if (e.key === "Escape" && E.find.value) { e.preventDefault(); E.find.value = ""; onSearchInput(); } });
      E.ol.addEventListener("keydown", e => { if ((e.key === "Enter" || e.key === " ") && e.target.classList && e.target.classList.contains("efpO")) { e.preventDefault(); openOrder(e.target, e.target.dataset.rid); } });
      for (const k of Object.keys(E.k)) { const c = E.k[k].card; c.addEventListener("pointerenter", () => showHC(k)); c.addEventListener("pointerleave", hideHC); c.addEventListener("focus", () => showHC(k)); c.addEventListener("blur", hideHC); }
      if (root.ResizeObserver) { ro = new ResizeObserver(() => { if (S.M && E.calH.clientWidth && Math.abs(E.calH.clientWidth - S.calW) > 24) renderCal(); }); ro.observe(E.calH); }
      setTimeout(() => root0.classList.remove("in"), 900);
    }

    /* ── hover card of a figure: its plain definition, and whether it is counted or estimated ── */
    function showHC(full) {
      const k = E.k[full], M = S.M, m = M && metricAt(M, full), key = shortKey(full), d = CARD[key]; if (!k) return;
      const fmt = fmtFor(key, (m && m.unit) || d.unit), def = (m && m.def) || d.def;
      const tag = m && m.est ? `<span class="tag">Estimated</span>` : m && m.window && M.eventWindow ? `<span class="tag">From the newest ${nf(M.eventWindow.days)} days</span>` : m && m.derived ? `<span class="tag">Worked out from the days shown</span>` : `<span class="tag ok">Counted from logged activity</span>`;
      const why = m && m.est && m.why ? `<p>${esc(m.why)}</p>` : "", n = m && m.n != null ? `<span class="prev">Based on ${esc(nf(m.n))}</span>` : "";
      const pv = M && M.prev ? `<span class="prev">${esc(PREV_WORD[S.range] ? PREV_WORD[S.range].replace(/^./, c => c.toUpperCase()) : "Before")}: ${m && m.prev != null ? esc(fmt(m.prev)) : "no data"}</span>` : "";
      E.hc.innerHTML = `<b>${esc((m && m.label) || d.label)}</b><p>${esc(def)}</p>${why}${tag}${pv}${n}`;
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
      if (w) txt = `Signed in${w.stationKey ? ` at <b>${esc(stName(w.stationKey))}</b>` : ""}${w.since ? ` · since ${esc(stamp(w.since, td))}` : ""}`;
      else if (seen) txt = `${L ? "Not signed in · last" : "Last"} seen <b>${esc(stamp(seen, td))}</b>`;
      else txt = L || M ? "Not signed in now" : "";
      if (E.where._h !== txt) { E.where._h = txt; E.where.innerHTML = txt; }
      const here = L ? L.stationKey : "", st = (M && M.stations.length ? M.stations : []).slice(0, 5), sig = st.map(x => x.station + x.minutes + x.parts).join() + "|" + here + !!w;
      if (E.chips._sig !== sig) { E.chips._sig = sig; E.chips.innerHTML = st.map(x => `<span class="efpChip${w && here && x.station === here ? " now" : ""}" title="${esc(x.parts != null ? nf(x.parts) + " parts" : "")}"><b>${esc(x.label)}</b>${esc(x.minutes != null ? durMs(x.minutes * 60000) : x.parts != null ? nf(x.parts) + " parts" : "")}</span>`).join(""); }
      if (M && M.spellings.length > 1) E.name.title = `Also seen as: ${M.spellings.filter(x => x !== M.name).join(", ")}`;
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
      const key = shortKey(full), work = kv(M, "att.daysWorked"), off = M.cal.filter(c => c.state === "off");
      switch (key) {
        case "parts": return m.v != null && work > 0 ? `${nf(m.v / work)} per day worked` : "";
        case "orders": return m.v > 0 && kv(M, "kpis.parts") != null ? `${nf1(kv(M, "kpis.parts") / m.v)} parts per order` : "";
        case "bestDay": return m.day ? dayLbl(m.day) : "";
        case "activeShare": { const a = kv(M, "kpis.activeHours"), s = kv(M, "kpis.signedHours"); return a != null && s != null ? `${hoursTxt(a * HOUR_MS)} of ${hoursTxt(s * HOUR_MS)}` : ""; }
        case "daysWorked": { const wd = kv(M, "att.workingDays") != null ? kv(M, "att.workingDays") : kv(M, "att.teamDays") != null ? kv(M, "att.teamDays") : work != null && kv(M, "att.daysOff") != null ? work + kv(M, "att.daysOff") : null; return wd ? `of ${nf(wd)} working days` : ""; }
        case "daysOff": return off.length ? off.slice(-3).map(c => mdLbl(c.day)).join(", ") + (off.length > 3 ? ` +${off.length - 3}` : "") : m.v === 0 ? "none" : "";
        case "issues": { const I = M.issues; return I && I.byKind.length ? `most: ${I.byKind[0].label} (${nf(I.byKind[0].count)})` : m.v === 0 ? "none logged" : ""; }
        case "issuesPer100Orders": return kv(M, "issues.issues") != null && kv(M, "kpis.orders") != null ? `${nf(kv(M, "issues.issues"))} in ${nf(kv(M, "kpis.orders"))} orders` : "";
        case "firstPassRate": case "deliveredRate": case "failureRate": case "reversalRate": case "rejectRate": case "errorRate": return m.num != null && m.den ? `${nf(m.num)} of ${nf(m.den)}` : "";
        case "lateDays": case "shortDays": return m.v === 0 ? "none" : "";
        default: return "";
      }
    }
    function sparkFor(full, B, M) {
      const key = shortKey(full), sp = CARD[key].sp; if (!sp) return [];
      const it = B.items;
      if (B.kind === "hour") return sp === "parts" || sp === "scans" ? it.map(x => (x.future ? null : x[sp])) : [];
      const f = g => it.map(x => (x.future ? null : g(x)));
      switch (sp) {
        case "share": return f(x => (x.activeMs != null && x.signedMs > 0 ? Math.min(100, x.activeMs / x.signedMs * 100) : null));
        case "worked": return B.kind === "day" ? f(x => (x.state === "worked" || x.state === "partial" ? 1 : x.state === "off" ? 0 : null)) : [];
        case "off": return B.kind === "day" ? f(x => (x.state === "off" ? 1 : x.state === "worked" || x.state === "partial" ? 0 : null)) : [];
        default: return f(x => x[sp]);
      }
    }
    function renderKpis(M, B, first) {
      const evs = M.eventWindow;
      for (const full of Object.keys(E.k)) {
        const k = E.k[full], key = shortKey(full), d = CARD[key], m = metricAt(M, full), unit = (m && m.unit) || d.unit, fmt = fmtFor(key, unit);
        const contactOff = full.startsWith("contact.") && M.contact && M.contact.available === false;
        setNum(k.val, m ? m.v : null, fmt, first); setText(k.t, (m && m.label) || d.label);
        const dl = M.prev && m ? deltaOf(m, key, unit) : { txt: "", cls: "" }, sig = dl.txt + "|" + dl.cls + "|" + dl.none + "|" + S.range + "|" + (m ? m.v : "x");
        if (k.d._sig !== sig) { k.d._sig = sig; const pw = PREV_WORD[S.range] || "before"; k.d.innerHTML = !m || m.v == null ? "" : dl.txt ? `<b class="${dl.cls}">${esc(dl.txt)}</b><span>vs ${esc(pw)}</span>` : dl.none ? `<span>vs ${esc(pw)}: no data</span>` : ""; }
        setText(k.s, contactOff ? "Not recorded in this range" : m && m.v != null ? subFor(full, m, M) : "");
        const tag = m && m.est ? "est." : m && m.window && evs ? "recent" : m && m.derived ? "sum" : ""; setText(k.tag, tag);
        const sp = m ? sparkFor(full, B, M) : [], known = sp.filter(x => x != null).length; k.sp.set(known >= 3 ? sp : []); k.card.querySelector(".efpKsp").style.visibility = known >= 3 ? "" : "hidden";
        k.card.setAttribute("aria-label", `${(m && m.label) || d.label}: ${m && m.v != null ? fmt(m.v) : "no data"}. ${(m && m.def) || d.def}`);
      }
    }
    const tickFmt = v => (Math.abs(v - Math.round(v)) > 0.01 ? nf1(v) : nf(v));
    function renderCharts(M, B) {
      const it = B.items, kind = B.kind, pick = i => { const x = it[i]; if (!x || !x.pickable) return; if (kind === "day") go({ range: "day", anchor: x.key }); else if (kind === "week") go({ range: "week", anchor: x.span[1] > today() ? today() : x.span[1] }); };
      const hint = x => (x.pickable ? (kind === "day" ? "Click to open this day" : "Click to open this week") : "");
      const stepF = kind === "day" && it.length > 7 ? { step: 1 } : {}, labels = it.map(x => x.label);
      const avail = kind === "hour" ? ["parts", "scans"] : ["parts", "orders", "perActiveHour"]; if (!avail.includes(S.metric)) S.metric = "parts";
      root0.querySelectorAll("[data-metric]").forEach(b => { const ok = avail.includes(b.dataset.metric), on = b.dataset.metric === S.metric; b.classList.toggle("hidden", !ok); b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
      // throughput
      const mt = S.metric, MET = { parts: ["parts", nf], scans: ["scans", nf], orders: ["orders", nf], perActiveHour: ["per active hour", rateTxt] }, [noun, fmt] = MET[mt];
      const vals = it.map(x => (x.future ? null : x[mt])), known = vals.filter(v => v != null), all0 = known.length && known.every(v => !v);
      const worked = it.filter(x => !x.future && x[mt] != null && (x[mt] > 0 || nz(x.signedMs) > 0)), avg = kind !== "hour" && worked.length > 1 ? worked.reduce((n, x) => n + x[mt], 0) / worked.length : null;
      const row = (k, v) => [k, v == null ? "—" : nf(v)];
      charts.tp.set(Object.assign({ name: `Throughput, ${noun} per ${B.noun}`, labels, hi: B.hi, layers: [{ type: "bars", cls: "", a: vals }], ref: avg, refLabel: avg != null ? `avg ${fmt(avg)}` : "", yFmt: tickFmt, pick,
        empty: !known.length ? "Not logged for this range" : all0 ? "No activity in this range" : "",
        tips: it.map(x => ({ t: x.title, v: x.future ? "Not yet" : x[mt] == null ? "Not logged" : `${fmt(x[mt])} ${noun}`, pick: !!x.pickable, hint: hint(x),
          rows: x.future || x[mt] == null ? [] : kind === "hour" ? [row(mt === "parts" ? "Scans" : "Parts", mt === "parts" ? x.scans : x.parts)] : [mt !== "orders" ? row("Orders", x.orders) : row("Parts", x.parts), mt !== "perActiveHour" ? ["Per active hour", rateTxt(x.perActiveHour)] : row("Parts", x.parts), ["Signed in", durMs(x.signedMs)], ["Active", durMs(x.activeMs)]] })) }, stepF));
      setText(E.tpP, avg != null ? `average ${fmt(avg)} per ${B.noun} worked` : kind === "hour" && known.length ? (() => { let pk = -1; vals.forEach((v, i) => { if (v > 0 && (pk < 0 || v > vals[pk])) pk = i; }); return pk >= 0 ? `busiest hour ${hourLabel(+it[pk].key)}` : ""; })() : "");
      const day = kind === "hour"; E.cSp.classList.toggle("hidden", day); E.cTm.classList.toggle("hidden", day); E.cShift.classList.toggle("hidden", !day);
      if (day) { renderShift(M); return; }
      // speed: the median (days with single events) and the average
      const med = it.map(x => (x.future ? null : x.medianSecPerOrder)), avgS = it.map(x => (x.future ? null : x.secPerOrder)), sp = med.some(v => v != null) || avgS.some(v => v != null), hasMed = med.some(v => v != null);
      E.lg.classList.toggle("hidden", !hasMed);
      charts.sp.set(Object.assign({ name: "Speed per order", labels, hi: B.hi, yFmt: v => secTxt(v).replace(" min", "m"), pick, empty: sp ? "" : "No speed data in this range",
        layers: hasMed ? [{ type: "line", cls: "b", a: avgS }, { type: "line", cls: "a", a: med }] : [{ type: "line", cls: "b", a: avgS }],
        tips: it.map(x => ({ t: x.title, v: x.future ? "Not yet" : x.secPerOrder == null && x.medianSecPerOrder == null ? "No orders timed" : `${secTxt(x.secPerOrder)} average`, pick: !!x.pickable, hint: hint(x), rows: x.future || x.secPerOrder == null && x.medianSecPerOrder == null ? [] : [].concat(x.medianSecPerOrder != null ? [["Median", secTxt(x.medianSecPerOrder)]] : [], [["Orders", x.orders == null ? "—" : nf(x.orders)]]) })) }, stepF));
      // active vs signed in, in hours
      const sg = it.map(x => (x.future || x.signedMs == null ? null : x.signedMs / HOUR_MS)), ac = it.map(x => (x.future || x.activeMs == null ? null : x.activeMs / HOUR_MS)), tm = sg.some(v => v != null) || ac.some(v => v != null);
      charts.tm.set(Object.assign({ name: "Active vs signed in", labels, hi: B.hi, yFmt: v => `${tickFmt(v)} h`, pick, empty: tm ? "" : "No sign-in time in this range",
        layers: [{ type: "bars", cls: "under", a: sg, w: 1 }, { type: "bars", cls: "", a: ac, w: .56 }],
        tips: it.map(x => ({ t: x.title, v: x.future ? "Not yet" : x.signedMs == null && x.activeMs == null ? "Not logged" : `${durMs(x.signedMs)} signed in`, pick: !!x.pickable, hint: hint(x), rows: x.future || x.signedMs == null && x.activeMs == null ? [] : [["Active", durMs(x.activeMs)], ["Active share", x.signedMs > 0 && x.activeMs != null ? pctTxt(Math.min(1, x.activeMs / x.signedMs)) : "—"], ["Parts", x.parts == null ? "—" : nf(x.parts)]] })) }, stepF));
    }
    /* the Day view: one shift, in, out, and what the signed-in time was made of */
    function renderShift(M) {
      const c = M.cal[0] || {}, p = M.series[0] || {}, td = today(), host0 = E.shiftH, tipEl = host0.querySelector(".efpTip");
      const sg = c.signedMs != null ? c.signedMs : p.signedMs, ac = c.activeMs != null ? c.activeMs : p.activeMs, id = p.idleMs, ul = sg != null && ac != null ? Math.max(0, sg - ac - nz(id)) : null;
      const sig = JSON.stringify([c, p.idleMs, M.from]); if (host0._sig === sig) return; host0._sig = sig; host0.querySelectorAll(":scope>:not(.efpTip)").forEach(x => x.remove());
      const box = el("div", "efpShift");
      if (!(sg > 0)) { const why = { off: "A day off: a team working day on which this person never signed in.", closed: "The team did not work this day.", before: "Before this person's first record.", future: "Not yet.", pending: "Not signed in yet today." }[c.state] || "Not signed in this day."; box.innerHTML = `<div class="efpEmptyBox" style="padding:12px 0">${esc(why)}</div>`; setText(E.shiftP, ""); host0.insertBefore(box, tipEl); return; }
      setText(E.shiftP, c.late ? "late start" : c.short ? "short day" : "");
      const pc = v => (v > 0 && sg > 0 ? Math.max(0, Math.min(100, v / sg * 100)) : 0);
      box.innerHTML = `<div class="efpShiftT"><span>In <b>${c.firstIn ? esc(clock(c.firstIn)) : "—"}</b></span><span>Out <b>${c.lastOut ? esc(clock(c.lastOut)) : M.from === td ? "still in" : "—"}</b></span><span>Signed in <b>${esc(durMs(sg))}</b></span>${ac != null ? `<span>Active <b>${esc(durMs(ac))}</b>${sg > 0 ? ` (${pctTxt(Math.min(1, ac / sg))})` : ""}</span>` : ""}</div>
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
    const DAY_WORDS = { off: "off", worked: "worked", partial: "part day", closed: "team closed", before: "no record", future: "not yet", pending: "not signed in yet" };
    function renderCal() {
      const M = S.M; if (!M) return; const src = calSource(M), td = today(), by = new Map(src.cal.map(c => [c.day, c])), n = diffDays(src.from, src.to) + 1, long = n > 42;
      S.calW = E.calH.clientWidth;
      const worked = src.cal.filter(c => c.state === "worked" || c.state === "partial").length, off = src.cal.filter(c => c.state === "off").length;
      setText(E.calP, src.wait && n < 28 ? "" : worked || off ? `${nf(worked)} worked · ${nf(off)} off` : "");
      const sig = JSON.stringify([src.from, src.to, src.sel, long, [...by.values()].map(c => c.day + c.state + c.signedMs + c.parts), td, !!src.wait]);
      if (E.calH._sig === sig) return; E.calH._sig = sig;
      const host0 = E.calH, tipEl = host0.querySelector(".efpTip"); host0.querySelectorAll(":scope>:not(.efpTip)").forEach(x => x.remove());
      const cell = day => {
        const c = by.get(day), state = day > td ? "future" : c ? c.state : "before";
        const b = el("button", `efpDy ${state}${day === td ? " today" : ""}${day >= src.sel[0] && day <= src.sel[1] ? " sel" : ""}`); b.type = "button"; b.dataset.day = day; b.textContent = long ? "" : String(+day.slice(8));
        b.setAttribute("aria-label", `${dayLbl(day)}: ${DAY_WORDS[state]}`); if (state === "future") b.tabIndex = -1;
        const rows = []; if (c && (state === "worked" || state === "partial")) { if (c.activeMs != null) rows.push(["Active", durMs(c.activeMs)]); if (c.firstIn) rows.push(["In · out", `${clock(c.firstIn)}${c.lastOut ? " · " + clock(c.lastOut) : ""}`]); rows.push(["Parts", c.parts == null ? "—" : nf(c.parts)], ["Orders", c.orders == null ? "—" : nf(c.orders)]); if (c.late) rows.push(["", "Late start"]); if (c.short || state === "partial") rows.push(["", "Short day"]); }
        if (c && c.others != null && state !== "future" && state !== "closed" && state !== "before") rows.push(["Team", `${nf(c.others)} others in`]);
        const t = { t: wdLong.format(dayDate(day)) + ", " + mdLbl(day), v: state === "off" ? "Day off" : state === "future" ? "Not yet" : state === "closed" ? "The team did not work" : state === "before" ? "No record" : state === "pending" ? "Not signed in yet" : `${durMs(c && c.signedMs)} signed in`, rows, hint: state === "before" ? "Before records began" : state === "future" ? "" : "Click to open this day" };
        b.addEventListener("pointerenter", () => mini.cal.show(b, t)); b.addEventListener("focus", () => mini.cal.show(b, t)); b.addEventListener("pointerleave", () => mini.cal.hide()); b.addEventListener("blur", () => mini.cal.hide());
        return b;
      };
      if (src.wait && n < 28) { const w = el("div"); w.innerHTML = `<span class="spin" aria-hidden="true"></span> Reading the month…`; w.style.cssText = "display:flex;gap:8px;align-items:center;color:var(--ink45);padding:18px 0"; host0.insertBefore(w, tipEl); return; }
      const cal = el("div", "efpCal" + (long ? " long" : ""));
      if (!long) {
        const hd = el("div", "efpCalHd"); "MTWTFSS".split("").forEach(c => hd.appendChild(el("span")).textContent = c); cal.appendChild(hd);
        let d = addDays(src.from, -dowMon(src.from)), row = null; const stop = addDays(src.to, 6 - dowMon(src.to));
        for (let i = 0; d <= stop && i < 60; d = addDays(d, 1), i++) { if (i % 7 === 0) { row = el("div", "efpCalRow"); cal.appendChild(row); } row.appendChild(d < src.from || d > src.to ? el("span", "efpDy pad") : cell(d)); }
      } else {
        const start = mondayOf(src.from), weeks = Math.floor(diffDays(start, src.to) / 7) + 1; cal.style.gridTemplateColumns = `auto repeat(${weeks}, minmax(0,1fr))`;
        ["M", "", "W", "", "F", "", "S"].forEach((t, r) => { const s = el("span", "efpCalD"); s.textContent = t; s.style.cssText = `grid-column:1;grid-row:${r + 2};margin:0`; cal.appendChild(s); });
        let lastM = "";
        for (let w = 0; w < weeks; w++) { const mon = addDays(start, w * 7), mlab = mon.slice(0, 7); if ((mlab !== lastM && +mon.slice(8) <= 14) || w === 0) { const m = el("span", "efpCalM"); m.textContent = monShort.format(dayDate(mon)); m.style.cssText = `grid-column:${w + 2}/span 4;grid-row:1;display:block;height:auto`; cal.appendChild(m); lastM = mlab; }
          for (let r = 0; r < 7; r++) { const day = addDays(mon, r); if (day < src.from || day > src.to) continue; const c = cell(day); c.style.gridColumn = String(w + 2); c.style.gridRow = String(r + 2); cal.appendChild(c); } }
      }
      host0.insertBefore(cal, tipEl);
    }

    /* station mix: a donut that answers the pointer, and the list beside it */
    function renderMix(M) {
      const st = M.stations.filter(s => nz(s.parts) > 0 || nz(s.minutes) > 0), byParts = st.some(s => nz(s.parts) > 0), val = s => (byParts ? nz(s.parts) : nz(s.minutes)), tot = st.reduce((n, s) => n + val(s), 0), txt = v => (byParts ? nf(v) : durMs(v * 60000));
      const sig = JSON.stringify([st, byParts]); setText(E.mixP, tot ? (byParts ? `${nf(tot)} parts` : durMs(tot * 60000)) : "");
      if (E.mixH._sig === sig) return; E.mixH._sig = sig; const tipEl = E.mixH.querySelector(".efpTip"); E.mixH.querySelectorAll(":scope>:not(.efpTip)").forEach(x => x.remove());
      if (!tot) { const e = el("div", "efpEmptyBox"); e.textContent = M.stations.length ? "No station activity in this range" : "No station data in this range"; E.mixH.insertBefore(e, tipEl); return; }
      const wrap = el("div", "efpMix"), don = el("div", "efpDonut"), svg = doc.createElementNS(SVG, "svg"); svg.setAttribute("viewBox", "0 0 150 150"); svg.setAttribute("role", "img"); svg.setAttribute("aria-label", `Station mix: ` + st.map(s => `${s.label} ${Math.round(val(s) / tot * 100)}%`).join(", "));
      const g = mk(svg, "g", { transform: "rotate(-90 75 75)" }), center = el("div", "efpDc"), list = el("div", "efpMixL");
      center.innerHTML = `<b>${esc(txt(tot))}</b><span>${byParts ? "parts" : "time"}</span>`;
      let cum = 0; const arcs = [], rows = [];
      st.forEach((s, i) => {
        const share = val(s) / tot * 100, col = PALETTE[i % PALETTE.length], gap = st.length > 1 ? Math.min(.9, share / 3) : 0;
        const arc = mk(g, "circle", { cx: 75, cy: 75, r: 57, pathLength: 100, class: "efpArc", stroke: col, "stroke-dasharray": `0 100`, "stroke-dashoffset": -cum }); arcs.push([arc, Math.max(0, share - gap)]); cum += share;
        const row = el("div", "efpMr", `<i style="background:${col}"></i><span>${esc(s.label)}</span><b>${esc(txt(val(s)))}</b><em>${Math.round(share)}%</em><u><s style="width:0;background:${col}"></s></u>`); rows.push(row); list.appendChild(row);
        const t = { t: s.label, v: byParts ? `${nf(s.parts)} parts` : durMs(s.minutes * 60000), rows: [["Share", `${Math.round(share)}%`], ["Orders", s.orders == null ? "—" : nf(s.orders)], ["Time", s.minutes == null ? "—" : durMs(s.minutes * 60000)], ["Per active hour", rateTxt(s.perActiveHour)]] };
        const on = () => { don.classList.add("has"); arc.classList.add("on"); row.classList.add("on"); center.innerHTML = `<b>${esc(txt(val(s)))}</b><span>${esc(s.label)}</span>`; mini.mix.show(row, t); };
        const off = () => { don.classList.remove("has"); arc.classList.remove("on"); row.classList.remove("on"); center.innerHTML = `<b>${esc(txt(tot))}</b><span>${byParts ? "parts" : "time"}</span>`; mini.mix.hide(); };
        for (const e of [arc, row]) { e.addEventListener("pointerenter", on); e.addEventListener("pointerleave", off); }
      });
      don.append(svg, center); wrap.append(don, list); E.mixH.insertBefore(wrap, tipEl);
      tween(options.growMs * 1.4, k => { arcs.forEach(([a, len]) => a.setAttribute("stroke-dasharray", `${(len * k).toFixed(2)} ${(100 - len * k).toFixed(2)}`)); rows.forEach((r, i) => { r.querySelector("s").style.width = `${Math.round(val(st[i]) / val(st[0]) * 100 * k)}%`; }); });
    }

    /* busiest hours: the hours of the day over the range */
    function renderHeat(M, B) {
      const dayRange = B.kind === "hour", has = M.hours.length && M.hours.some(h => h.parts != null);
      E.heat.classList.toggle("hidden", dayRange || !has); if (dayRange || !has) return;
      const vals = M.hours.map(h => h.parts), max = Math.max(1, ...vals.map(nz)); let peak = -1; vals.forEach((v, h) => { if (v > 0 && (peak < 0 || v > vals[peak])) peak = h; });
      setText(E.heatP, peak >= 0 ? `busiest around ${hourLabel(peak)}` : "");
      const sig = JSON.stringify(M.hours); if (E.heatH._sig === sig) return; E.heatH._sig = sig; const tipEl = E.heatH.querySelector(".efpTip"); E.heatH.querySelectorAll(":scope>:not(.efpTip)").forEach(x => x.remove());
      const wrap = el("div", "efpHeat"); wrap.setAttribute("role", "img"); wrap.setAttribute("aria-label", "Busiest hours of the day");
      const row = el("div", "efpHeatRow one");
      for (let h = 0; h < 24; h++) {
        const v = vals[h], x = M.hours[h], c = el("div", "efpHc" + (v == null ? " x" : "")); c.tabIndex = v == null ? -1 : 0; c.style.setProperty("--a", v ? String(Math.max(.1, v / max)) : "0");
        const t = { t: hourLabel(h), v: v == null ? "No data" : `${nf(v)} parts`, rows: [["Scans", x.scans == null ? "—" : nf(x.scans)], ["Per day worked", x.perDay == null ? "—" : nf1(x.perDay)]] };
        c.setAttribute("aria-label", `${t.t}: ${t.v}`); c.addEventListener("pointerenter", () => mini.heat.show(c, t)); c.addEventListener("pointerleave", () => mini.heat.hide()); c.addEventListener("focus", () => mini.heat.show(c, t)); c.addEventListener("blur", () => mini.heat.hide());
        row.appendChild(c);
      }
      wrap.appendChild(row); const ax = el("div", "efpHeatAx one"); for (let h = 0; h < 24; h++) ax.appendChild(el("span")).textContent = h % 6 === 0 ? hourShort(h) : ""; wrap.appendChild(ax);
      E.heatH.insertBefore(wrap, tipEl);
    }

    /* issues, by kind, each with its order and the plain reason */
    const ATTR = { own: "Their action", system: "System failure", order: "About the order" };
    function renderIssues(M) {
      const I = M.issues; setText(E.iN, I && I.total != null ? nf(I.total) : "");
      const sig = JSON.stringify([I, [...S.openKinds], S.kinds, M.eventWindow]); if (E.is._sig === sig) return; E.is._sig = sig;
      if (!I) { E.is.innerHTML = `<div class="efpEmptyBox">No issue data in this range</div>`; return; }
      if (!I.byKind.length) { E.is.innerHTML = `<div class="efpEmptyBox">${I.total === 0 ? "No issues logged in this range" : "No issue data in this range"}</div>`; return; }
      const td = today(), ew = M.eventWindow ? M.eventWindow.days : null, sum = [["Total", I.total], ["Their action", I.own], ["System failure", I.system], ["Per 100 orders", I.per100]].filter(x => x[1] != null);
      E.is.innerHTML = `<div class="efpSum">${sum.map(([k, v]) => `<span>${esc(k)} <b>${nf1(v)}</b></span>`).join("")}</div>` + I.byKind.map((k, gi) => {
        const items = I.items.filter(x => x.kind === k.kind), open = S.openKinds.has(k.kind) || (!S.kinds && gi === 0), shown = items.slice(0, 8);
        return `<div class="efpIg${open ? " open" : ""}" data-kind="${esc(k.kind)}"><button type="button" class="efpIgh" aria-expanded="${open}" data-kindbtn="${esc(k.kind)}"><span><b>${esc(k.label)}</b>${k.attribution && ATTR[k.attribution] ? `<span class="efpAt ${esc(k.attribution)}">${esc(ATTR[k.attribution])}</span>` : ""}${k.def ? `<small>${esc(k.def)}</small>` : ""}</span><em>${nf(k.count)}</em><i aria-hidden="true">▼</i></button><div class="efpIgw"><div class="efpIgi"><div class="efpIgl">${k.how ? `<div class="efpHow">${esc(k.how)}${k.est ? " Estimated." : ""}</div>` : ""}${k.coverage === "window" && ew ? `<div class="efpHow">Counted from the newest ${nf(ew)} days of the range only.</div>` : ""}${shown.map(x => `<div class="efpIr"><time>${x.at ? esc(stamp(x.at, td)) : x.day ? esc(mdLbl(x.day)) : "—"}</time>${x.rid ? `<button type="button" class="efpOid" data-order="${esc(x.rid)}" title="Open this order">${esc(x.number || x.rid)}</button>` : "<span>—</span>"}<span>${x.station ? esc(stName(x.station)) + " · " : ""}${esc(x.note || "No reason recorded")}</span></div>`).join("")}${items.length > shown.length ? `<div class="efpHow">+${nf(items.length - shown.length)} more in this range</div>` : ""}${!items.length ? `<div class="efpHow">${nf(k.count)} counted. The single events are not listed for this range.</div>` : ""}</div></div></div></div>`;
      }).join("");
    }
    const RATE_ORDER = ["firstPassRate", "firstPass", "successRate", "activeShare", "attendanceRate", "reversalRate", "reworkRate", "rejectRate", "errorRate", "failureRate"];
    function renderRates(M) {
      const R = Object.values(M.src.rates).sort((a, b) => { const i = RATE_ORDER.indexOf(a.key), j = RATE_ORDER.indexOf(b.key); return (i < 0 ? 99 : i) - (j < 0 ? 99 : j); }), C = M.contact, cm = C && C.available ? C.metrics : null;
      const sig = JSON.stringify([R, C]); if (E.rates._sig === sig) return; E.rates._sig = sig;
      const rateRow = (r, i) => { const known = r.v != null, w = known ? Math.max(0, Math.min(100, r.v)) : 0, cls = r.better === "down" ? "bad" : r.better === "up" ? "ok" : "un", dl = M.prev ? deltaOf(r, "x", "percent") : { txt: "" };
        return `<div class="efpRt" data-r="${i}" tabindex="0" aria-label="${esc(r.label)}: ${known ? pctVal(r.v) : "no data"}. ${esc(r.def)}"><div class="efpRtH"><b>${esc(r.label)}</b>${r.est ? `<em>est.</em>` : ""}<span>${known ? pctVal(r.v) : "—"}</span>${r.num != null && r.den ? `<em>${nf(r.num)} of ${nf(r.den)}</em>` : ""}${dl.txt ? `<em class="efpD ${dl.cls}">${esc(dl.txt)}</em>` : ""}</div><div class="efpRtB"><i class="${cls}" data-w="${w.toFixed(1)}"></i></div></div>`; };
      let html = R.length ? R.map((r, i) => rateRow(r, i)).join("") : `<div class="efpEmptyBox" style="padding:12px 0">No rates in this range yet</div>`;
      html += `<div class="efpSub">Inbox replies</div>`;
      if (!C) html += `<div class="efpHow">Contact figures are not read yet.</div>`;
      else if (!C.available) html += `<div class="efpHow">Inbox replies are not recorded for this person in this range.</div>`;
      else { const f = (k, l, fmt) => (cm[k] && cm[k].v != null ? `<span>${esc(l)}<b>${esc(fmt(cm[k].v))}</b></span>` : ""); html += `<div class="efpStat">${f("repliesSent", "Sent", nf)}${f("repliesDelivered", "Delivered", nf)}${f("repliesFailed", "Failed", nf)}${f("repliesDrafted", "Drafts", nf)}${f("repliesEdited", "Edited", nf)}${f("timeToFirstReplyMin", "First reply", v => durMs(v * 60000))}</div>`; }
      E.rates.innerHTML = html;
      E.rates.querySelectorAll(".efpRt").forEach(n => { const r = R[+n.dataset.r], t = { t: r.label, v: r.v != null ? pctVal(r.v) : "No data", rows: [].concat(r.num != null ? [["Good", nf(r.num)]] : [], r.den != null ? [["Of", nf(r.den)]] : [], r.prev != null ? [[(PREV_WORD[S.range] || "Before").replace(/^./, c => c.toUpperCase()), pctVal(r.prev)]] : []), hint: r.def + (r.est && r.why ? " " + r.why : "") }; n.addEventListener("pointerenter", () => mini.rate.show(n, t)); n.addEventListener("pointerleave", () => mini.rate.hide()); n.addEventListener("focus", () => mini.rate.show(n, t)); n.addEventListener("blur", () => mini.rate.hide()); });
      requestAnimationFrame(() => E.rates.querySelectorAll(".efpRtB i").forEach(b => { b.style.width = b.dataset.w + "%"; }));
    }
    function renderNote(M) {
      const lines = M.notes.slice(); if (M.partial && !lines.length) lines.push("Part of the data could not be read just now, so some figures may be low.");
      if (!M.found) lines.push("No sign-ins or activity were found for this name.");
      else if (!Object.values(M.src.kpis).some(k => k.v != null) && !lines.length) lines.push("No activity in this range.");
      const sig = lines.join("|"); if (E.note._sig === sig) return; E.note._sig = sig; E.note.textContent = ""; for (const l of lines) E.note.appendChild(el("span")).textContent = l; E.note.classList.toggle("hidden", !lines.length);
      const ct = M.cannotTell, csig = JSON.stringify(ct); if (E.cannot._sig !== csig) { E.cannot._sig = csig; E.cannot.innerHTML = ct.length ? `<div><b>What this data cannot show</b></div>` + ct.map(c => `<div>${c.topic ? `<b>${esc(c.topic)}.</b> ` : ""}${esc(c.text)}</div>`).join("") : ""; E.cannot.classList.toggle("hidden", !ct.length); }
    }
    function render(M, first) {
      const B = buckets(M, today(), now()); paintBar(); renderHead(); renderNote(M); renderKpis(M, B, first); renderCharts(M, B); renderCal(); renderMix(M); renderHeat(M, B); renderIssues(M); renderRates(M); paintLive();
    }

    /* ── the live card: the order in this person's hands right now ── */
    function renderLive() {
      const L = S.live; if (!L) return; E.nowS.classList.remove("hidden");
      const cur = L.current, ids = new Set(cur.map(c => String(c.rid || c.orderNumber)));
      for (const [id, n] of [...S.now]) if (!ids.has(id)) { S.now.delete(id); const e = n.el; if (e.animate && !still()) { const a = e.animate([{ opacity: 1 }, { opacity: 0, transform: "translateY(-4px)" }], { duration: 180, easing: "ease-in" }); a.onfinish = () => e.remove(); } else e.remove(); }
      const idle = E.now.querySelector(".efpNowIdle");
      if (!cur.length) {
        const w = L.where, td = today(), last = (w && w.lastSeenAt) || S.seenAt || null;
        const html = w ? `<i></i><span><b>Not on an order right now</b>${last ? ` · last activity ${esc(ago((now() - last) / 1000))}` : ""}${w.stationKey ? ` at ${esc(stName(w.stationKey))}` : ""}</span>` : `<i></i><span><b>Not signed in</b>${last ? ` · last seen ${esc(stamp(last, td))}` : ""}</span>`;
        if (!idle) { const e = el("div", "efpCard efpNowIdle", html); e._h = html; E.now.appendChild(e); if (!still() && e.animate) e.animate([{ opacity: 0, transform: "translateY(4px)" }, { opacity: 1, transform: "none" }], { duration: 260, easing: "cubic-bezier(.2,.8,.2,1)" }); } else if (idle._h !== html) { idle._h = html; idle.innerHTML = html; }
        return;
      }
      if (idle) idle.remove();
      for (const c of cur) {
        const id = String(c.rid || c.orderNumber), sig = JSON.stringify(c); let n = S.now.get(id);
        if (n && n.sig === sig) continue;
        let node = null; const OC = root.EfficiencyStations && root.EfficiencyStations.orderCard;
        if (typeof OC === "function") { try { const r = OC(c); node = r && r.nodeType === 1 ? r : r && (r.el || r.node) || null; if (!node && typeof r === "string") node = el("div", "", r); } catch (e) { console.warn("[efficiency person] shared order card:", e && e.message); } }
        if (!node) node = nowCard(c, now());
        if (n) n.el.replaceWith(node); else { E.now.appendChild(node); if (!still() && node.animate) node.animate([{ opacity: 0, transform: "translateY(6px)" }, { opacity: 1, transform: "none" }], { duration: 300, easing: "cubic-bezier(.2,.8,.2,1)" }); }
        S.now.set(id, { el: node, sig });
      }
    }

    /* ── reading: the period (a stored answer is shown at once, then made fresh), the live card, the month for the calendar ── */
    function accept(r, fromCache, at) {
      const p = period(), M = norm(r, { name: S.name, from: p.from, to: p.to }); if (!M.from || !M.to) { M.from = p.from; M.to = p.to; }
      const first = !S.M || !!fromCache; if (num(r.now) && !fromCache && typeof o.now !== "function" && !(EA() && EA().now)) S.off = num(r.now) - Date.now();
      if (M.lastSeen && M.to >= today()) S.seenAt = Math.max(S.seenAt || 0, M.lastSeen);       // "last seen" survives a switch to a day that has none (only windows that reach today say anything about now)
      S.M = M; S.at = at || Date.now(); E.wait.classList.add("hidden"); E.body.classList.remove("hidden"); E.body.classList.remove("dim"); if (!S.locked) E.msg.classList.add("hidden");
      render(M, first);
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
    function applyLive(snap) { S.live = pickLive(snap, S.name); S.liveAt = Date.now(); S.liveFails = 0; renderLive(); renderHead(); paintLive(); }
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
      S.gen++; if (S.ctl) { try { S.ctl.abort(); } catch (_) {} } S.busy = false; S.fails = 0; S.err = ""; paintBar(); hideHC(); S.calx = S.calx && S.M && S.calx.to === period().to ? S.calx : null;
      try { o.onState && o.onState({ range: S.range, day: S.anchor, from: period().from, to: period().to }); } catch (_) {}
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
    function mountOrders() {
      const EO = root.EfficiencyOrders;
      if (EO && typeof EO.mount === "function") {
        try { const host1 = el("div", "efpOrdersMod"); E.ordersHost.appendChild(host1); const h = EO.mount(host1, { name: S.name, onOpen: (rid, btn) => openOrder(btn || host1, rid) }); if (h) { ordersH = h; E.ordersOwn.classList.add("hidden"); if (io) io.disconnect(); return; } host1.remove(); } catch (e) { console.warn("[efficiency person] order list module:", e && e.message); }
      }
      if (root.IntersectionObserver) { io = new IntersectionObserver(es => { if (es.some(x => x.isIntersecting)) loadOrders(false); }, { rootMargin: "400px 0px" }); io.observe(E.more); }
      loadOrders(true); schedule("orders", options.ordersMs);
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
      if (b && b.hasAttribute("data-more")) { loadOrders(false); return; }
      const ob = t.closest("[data-order]"); if (ob && ob.dataset.order) { e.stopPropagation(); return openOrder(ob, ob.dataset.order); }
      const dy = t.closest(".efpDy[data-day]"); if (dy && !dy.classList.contains("future") && !dy.classList.contains("pad")) { mini.cal.hide(); return go({ range: "day", anchor: dy.dataset.day }); }
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
      if (S.ctl) { try { S.ctl.abort(); } catch (_) {} } if (io) io.disconnect(); if (ro) ro.disconnect(); Object.values(charts).forEach(c => c && c.destroy && c.destroy()); for (const k of Object.keys(E.k || {})) E.k[k].sp.destroy();
      if (typeof unsubLive === "function") { try { unsubLive(); } catch (_) {} } if (ordersH) { try { (ordersH.unmount || ordersH.destroy || ordersH).call(ordersH); } catch (_) {} }
      doc.removeEventListener("visibilitychange", onVisible); try { if (root.Seal && root.Seal.zoom && root.Seal.zoom.away) root.Seal.zoom.away(); } catch (_) {}
      if (root0.parentNode) root0.remove(); instances.delete(api);
    }
    function refresh() { if (S.dead) return; S.cache.clear(); S.gen++; S.fails = 0; fetchRange(S.gen, false); pollLive(); pollCal(); if (ordersH && typeof ordersH.refresh === "function") ordersH.refresh(); else loadOrders(true); }

    build(); paintBar(); setText(E.waitT, `Reading ${S.name}'s history…`);
    if (!o.onBack) E.back.classList.add("hidden");
    doc.addEventListener("visibilitychange", onVisible); T.tick = setInterval(tick, Math.max(250, options.tickMs));
    const api = { unmount, destroy: unmount, refresh, get state() { return { name: S.name, range: S.range, anchor: S.anchor, from: period().from, to: period().to, metric: S.metric, locked: S.locked, fails: S.fails, liveFails: S.liveFails, orders: S.orders.list.length, query: S.orders.q, loaded: !!S.M, at: S.at, ordersModule: !!ordersH }; }, go, el: root0 };
    instances.add(api);
    // the live card: the console's own live read when it offers one, else this page asks for `live` itself
    const subscribe = typeof o.onLive === "function" ? o.onLive : EA() && typeof EA().onLive === "function" ? EA().onLive : null;
    if (subscribe) { try { unsubLive = subscribe(applyLive) || true; } catch (_) { unsubLive = null; } const cur = typeof o.live === "function" ? o.live() : EA() && typeof EA().live === "function" ? EA().live() : null; if (cur) applyLive(cur); }
    go({}); if (!unsubLive) pollLive(); mountOrders();
    return api;
  }
  const instances = new Set();
  root.EfficiencyEmployee = { mount, options, norm, normOrders, pickLive, buckets, periodOf, shiftAnchor, periodLabel, niceMax, CARD, GROUPS, get instances() { return [...instances]; } };
})(window);
