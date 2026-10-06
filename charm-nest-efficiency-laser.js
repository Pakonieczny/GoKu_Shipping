/*  charm-nest-efficiency-laser.js — "Laser sheets" on an employee's page: how long each laser-cut sheet took (Paul, 6 Oct 2026; plans/stations-round2/plan.md R7, worker LS1).
 *  window.EfficiencyLaser.mount(host, { name, range?, call, now?, expected?, onError?, onFound? }) -> { setRange, refresh, unmount, state, el }
 *  A sheet's time is kept for good on its completion record (netlify/functions/_laserSheetTime.js, collection Laser_Sheet_Times): the moment the Laser person marked it completed
 *  (cut AND every back engraving done) minus the LATER of that person's sign-in as Laser and that person's previous sheet. This file only READS it, op `laserSheets`
 *  of employeeEfficiency ({ name, range: { from, to } } -> sheets newest first, totals, series; see plans/stations-round2/api.md, section LS1) through `call(body, signal)`
 *  (the person page hands its own, which adds the manager passcode and Real | Sandbox; nothing is stored here).
 *  It draws, inside `host`, the person's own section: average, fastest and slowest sheet, sheets, pieces and orders; a chart of the time per sheet (a bar per sheet for one day,
 *  the day's average for a week to three months, the week's average beyond); and the list of sheets (sheet, pieces, orders, minutes, from the sign-in or from the previous
 *  sheet). It follows the page's date chips (setRange). A sheet marked completed with no Laser sign-in is listed with a dash: its time is unknown, never invented, and it is
 *  left out of the average, fastest and slowest. The section is hidden when this person has no sheet in the range (unless `expected()` says they work at the laser).
 *  Waits show a small labelled spinner; a failed read keeps what is shown and says so once. Motion: transform and opacity only; reduced motion is honoured. "Pieces", never "lines". */
(function (root) {
  "use strict";
  if (root.EfficiencyLaser) return;
  const doc = root.document, TZ = "America/New_York";
  const options = { pollMs: 30000, firstWaitMs: 700, listFirst: 15, listMore: 30 };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const num = v => { if (v == null || v === "" || typeof v === "boolean") return null; v = +v; return Number.isFinite(v) ? v : null; };
  const nf = n => Math.round(num(n) || 0).toLocaleString("en-US");
  const isDay = s => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));

  /* ── times ── */
  const dayFmt = new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" });
  const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hour: "numeric", minute: "2-digit" });
  const mdFmt = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" });
  const nyDay = t => dayFmt.format(new Date(t));
  const mdLbl = day => { const [y, m, d] = String(day).split("-").map(Number); return mdFmt.format(new Date(Date.UTC(y, m - 1, d, 12))); };
  const diffDays = (a, b) => Math.round((Date.parse(b + "T12:00:00Z") - Date.parse(a + "T12:00:00Z")) / 86400e3);
  /** Seconds as the shop reads a sheet's time: "45 s", "12.5 min", "1 h 5 m". A dash when the time is not known (null), never a zero. */
  function secTxt(s) {
    s = num(s); if (s == null) return "—"; s = Math.max(0, s);
    if (s < 60) return `${Math.round(s)} s`;
    if (s < 3570) { const m = s / 60; return `${m < 9.95 ? Math.round(m * 10) / 10 : Math.round(m)} min`; }
    const m = Math.round(s / 60), h = Math.floor(m / 60); return `${h} h${m % 60 ? ` ${m % 60} m` : ""}`;
  }
  const FROM = { login: "From sign-in", previousSheet: "From previous sheet" };
  const fromTxt = r => (r.seconds == null ? "No time · not signed in as Laser" : FROM[r.startedFrom] || "");

  /* ── the answer, cleaned ── */
  function norm(r) {
    r = r && typeof r === "object" ? r : {};
    const rows = (Array.isArray(r.sheets) ? r.sheets : []).filter(x => x && num(x.at) > 0).map(x => ({ at: +x.at, sheetId: String(x.sheetId || ""), sheet: String(x.sheet || x.sheetId || "Sheet"), person: String(x.person || ""), seconds: num(x.seconds),
      startedFrom: x.startedFrom === "login" || x.startedFrom === "previousSheet" ? x.startedFrom : "unknown", pieces: Math.max(0, num(x.pieces) || 0), orders: Math.max(0, num(x.orders) || 0), together: Math.max(1, num(x.together) || 1), day: isDay(x.day) ? x.day : nyDay(+x.at) }));
    const t = r.totals && typeof r.totals === "object" ? r.totals : {};
    const totals = { sheets: num(t.sheets) || 0, timed: num(t.timed) || 0, unknown: num(t.unknown) || 0, avgSec: num(t.avgSec), fastestSec: num(t.fastestSec), slowestSec: num(t.slowestSec), pieces: num(t.pieces) || 0, orders: num(t.orders) || 0 };
    const series = (Array.isArray(r.series) ? r.series : []).filter(x => x && isDay(x.day)).map(x => ({ day: x.day, to: isDay(x.to) ? x.to : x.day, days: num(x.days) || 1, sheets: num(x.sheets) || 0, timed: num(x.timed) || 0, avgSec: num(x.avgSec), fastestSec: num(x.fastestSec), slowestSec: num(x.slowestSec) }));
    return { found: !!r.found && (totals.sheets > 0 || rows.length > 0), from: isDay(r.from) ? r.from : "", to: isDay(r.to) ? r.to : "", rows, totals, series, notes: (Array.isArray(r.notes) ? r.notes : []).map(String).slice(0, 4), partial: !!r.partial, at: num(r.now) };
  }

  /* ── styles (the console's tokens; the person page's own classes where they exist) ── */
  function style() {
    if (!doc || doc.getElementById("efpLaserStyle")) return;
    const s = doc.createElement("style"); s.id = "efpLaserStyle";
    s.textContent = `
.efpLs{display:grid;gap:0;min-width:0}
.efpLs .efpLabel .efpLsBusy{order:3;display:inline-flex;align-items:center;gap:6px;letter-spacing:0;text-transform:none;font-weight:500;font-size:11.5px;color:var(--ink45)}
.efpLs [hidden]{display:none!important}
.efpLs .efpLabel .efpLsLr{order:2;letter-spacing:0;text-transform:none;font-weight:500;font-size:11.5px}
.efpLs .spin{width:11px;height:11px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:spin .7s linear infinite;flex:0 0 11px;display:inline-block}
.efpLsBody{display:grid;gap:0;transition:opacity .25s ease}.efpLsBody.dim{opacity:.55}
.efpLsStats{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:0;padding:6px 6px 2px}
.efpLsK{display:grid;gap:2px;padding:10px 14px 8px;min-width:0}
.efpLsK+.efpLsK{border-left:1px solid var(--line2)}
.efpLsKL{font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--ink45);font-weight:700;line-height:1.3}
.efpLsKV{font:600 22px/1.15 var(--sans,inherit);letter-spacing:-.018em;font-variant-numeric:tabular-nums;white-space:nowrap;color:var(--ink)}
.efpLsKS{font-size:11px;color:var(--ink45);min-height:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.efpLsChart{padding:4px 18px 8px;border-top:1px solid var(--line2);min-width:0}
.efpLsChart .efpLsCT{display:flex;align-items:center;gap:8px;min-height:26px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;font-size:10.5px;color:var(--ink45);padding-top:8px}
.efpLsChart .efpLsCT span{margin-left:auto;text-transform:none;letter-spacing:0;font-weight:500;font-size:11.5px}
.efpLsList{margin:0;padding:0 6px 4px;list-style:none;border-top:1px solid var(--line2);display:grid}
.efpLsR{display:grid;grid-template-columns:96px minmax(130px,1.4fr) 84px 84px 90px minmax(150px,1.2fr);grid-template-areas:"t n pc od m f";gap:12px;align-items:center;padding:9px 10px;border-top:1px solid var(--line2);border-radius:10px}
.efpLsR:first-child{border-top-color:transparent}
.efpLsR.ent{animation:efpLsIn .34s cubic-bezier(.2,.8,.2,1) both}
@keyframes efpLsIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.efpLsT{grid-area:t;color:var(--ink45);font-size:11.5px;font-variant-numeric:tabular-nums;line-height:1.35}.efpLsT b{display:block;color:var(--ink70);font-weight:650;font-size:12px}
.efpLsN{grid-area:n;display:grid;gap:1px;min-width:0;font-weight:650;color:var(--ink)}.efpLsN span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.efpLsN small{font-weight:400;color:var(--ink45);font-size:11px;white-space:normal}
.efpLsS{display:contents}
.efpLsP{grid-area:pc;color:var(--ink70);font-variant-numeric:tabular-nums}.efpLsO{grid-area:od;color:var(--ink70);font-variant-numeric:tabular-nums}
.efpLsM{grid-area:m;font:650 14px var(--sans,inherit);font-variant-numeric:tabular-nums;color:var(--ink);white-space:nowrap}.efpLsM.none{color:var(--ink25);font-weight:500}
.efpLsF{grid-area:f;color:var(--ink45);font-size:11.5px}
.efpLsMore{display:flex;align-items:center;justify-content:center;gap:9px;padding:6px 0 12px;color:var(--ink45);font-size:11.5px}
.efpLsMore button{border:1px solid var(--line);background:var(--card2);border-radius:999px;padding:5px 14px;font:650 11.5px var(--sans,inherit);color:var(--ink70);cursor:pointer}.efpLsMore button:hover{background:var(--paper2);color:var(--ink)}
.efpLsNote{margin:0;padding:6px 18px 12px;color:var(--ink45);font-size:11.5px;line-height:1.5}
.efpLsEmpty{padding:22px 18px;text-align:center;color:var(--ink45);font-size:12.5px}
.efpLsWait{display:flex;align-items:center;gap:8px;padding:18px;color:var(--ink45);font-size:12px}
@container efp (max-width:900px){.efpLsStats{grid-template-columns:repeat(3,minmax(0,1fr))}.efpLsK:nth-child(4){border-left:0}.efpLsR{grid-template-columns:88px minmax(0,1fr) 84px 84px 80px;grid-template-areas:"t n pc od m" "t f f f f"}}
@container efp (max-width:640px){.efpLsStats{grid-template-columns:repeat(2,minmax(0,1fr))}.efpLsK:nth-child(n){border-left:0}.efpLsK:nth-child(even){border-left:1px solid var(--line2)}.efpLsKV{font-size:19px}.efpLsK{padding:9px 12px 7px}.efpLsChart{padding:4px 12px 8px}
.efpLsR{grid-template-columns:70px minmax(0,1fr) auto;grid-template-areas:"t n m" "t s s";gap:3px 10px}.efpLsS{display:flex;flex-wrap:wrap;gap:2px 10px;grid-area:s;font-size:11.5px}.efpLsS>*{grid-area:auto}.efpLsNote{padding:6px 12px 12px}}
@media (prefers-reduced-motion:reduce){.efpLs *,.efpLs *:before,.efpLs *:after{transition:none!important;animation:none!important}}`;
    doc.head.appendChild(s);
  }

  /* ── the section ── */
  function mount(host, o) {
    o = o || {}; if (!host || !doc || typeof o.call !== "function") return null; style();
    const now = () => (typeof o.now === "function" ? o.now() : Date.now());
    const S = { name: String(o.name || ""), range: o.range && isDay(o.range.from) && isDay(o.range.to) ? { from: o.range.from, to: o.range.to } : null, gen: 0, busy: false, data: null, key: "", err: "", shown: options.listFirst, dead: false, ctl: null, poll: 0, wait: 0, slow: false, cache: new Map(), rowsSig: "", chart: null, loadedAt: 0 };
    const sigOf = r => (r ? `${r.from}|${r.to}` : "");
    const root0 = doc.createElement("div"); root0.className = "efpLs";
    root0.innerHTML = `<div class="efpLabel">Laser sheets <b data-n></b><span class="efpLsLr" data-lr></span><span class="efpLsBusy" role="status" hidden><span class="spin" aria-hidden="true"></span><span data-bt></span></span></div>
<div class="efpCard efpLsCard"><div class="efpLsWait" data-wait hidden role="status"><span class="spin" aria-hidden="true"></span><span>Reading laser sheets…</span></div>
<div class="efpLsBody" data-body hidden>
<div class="efpLsStats" data-stats></div>
<div class="efpLsChart" data-chartbox><div class="efpLsCT"><b data-ct>Time per sheet</b><span data-cs></span></div><div data-chart></div></div>
<ol class="efpLsList" data-list aria-label="Laser sheets, newest first"></ol>
<div class="efpLsMore" data-more></div>
<p class="efpLsNote" data-note hidden></p>
</div><div class="efpLsEmpty" data-empty hidden></div></div>`;
    host.textContent = ""; host.appendChild(root0);
    const q = s => root0.querySelector(s);
    const E = { n: q("[data-n]"), lr: q("[data-lr]"), busy: q(".efpLsBusy"), bt: q("[data-bt]"), wait: q("[data-wait]"), body: q("[data-body]"), stats: q("[data-stats]"), chartBox: q("[data-chartbox]"), ct: q("[data-ct]"), cs: q("[data-cs]"), chart: q("[data-chart]"),
      list: q("[data-list]"), more: q("[data-more]"), note: q("[data-note]"), empty: q("[data-empty]") };
    const setText = (e, s) => { if (e && e.textContent !== s) e.textContent = s; };
    const show = on => { host.classList.toggle("hidden", !on); host.hidden = !on; try { o.onFound && o.onFound(on); } catch (_) {} };
    const expected = () => { try { return typeof o.expected === "function" ? !!o.expected() : false; } catch (_) { return false; } };
    const still = () => !!(root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches);
    const visible = () => !S.dead && host.isConnected && doc.visibilityState !== "hidden";

    function rangeLabel() {
      const r = S.data && S.data.from ? { from: S.data.from, to: S.data.to } : S.range; if (!r) return "";
      const td = nyDay(now()), n = diffDays(r.from, r.to) + 1;
      return n === 1 ? (r.from === td ? "Today" : r.from === nyDay(now() - 86400e3) ? "Yesterday" : mdLbl(r.from)) : `${mdLbl(r.from)} – ${r.to === td ? "today" : mdLbl(r.to)}`;
    }
    function paintBusy() {
      const on = S.busy && !!S.data;     // (the first read shows the card's own spinner)
      E.busy.hidden = !on; setText(E.bt, on ? "Reading laser sheets…" : "");
    }
    function paintStats(T) {
      const k = [["Average", secTxt(T.avgSec), T.timed ? `of ${nf(T.timed)} timed` : "no time yet"], ["Fastest", secTxt(T.fastestSec), ""], ["Slowest", secTxt(T.slowestSec), ""],
        ["Sheets", nf(T.sheets), T.unknown ? `${nf(T.unknown)} with no time` : "marked completed"], ["Pieces", nf(T.pieces), ""], ["Orders", nf(T.orders), ""]];
      const sig = JSON.stringify(k); if (E.stats._sig === sig) return; E.stats._sig = sig;
      E.stats.innerHTML = k.map(([l, v, s]) => `<div class="efpLsK"><span class="efpLsKL">${l}</span><b class="efpLsKV">${esc(v)}</b><span class="efpLsKS">${esc(s)}</span></div>`).join("");
    }
    /** One bar per sheet for a single day; the day's (or week's) average otherwise. */
    function chartData(D) {
      const single = D.from && D.from === D.to;
      if (single) {
        const rows = D.rows.slice().reverse();
        return { single, x: rows.map(r => clockFmt.format(new Date(r.at))), series: [{ key: "t", label: "Time", values: rows.map(r => r.seconds) }], meta: rows, xKind: "cat", n: rows.filter(r => r.seconds != null).length, label: "Time per sheet" };
      }
      const weekly = D.series.some(s => s.days === 7);
      return { single, x: D.series.map(s => s.day), series: [{ key: "t", label: weekly ? "Average per sheet" : "Average per sheet", values: D.series.map(s => s.avgSec) }], meta: D.series, xKind: weekly ? "week" : "day", n: D.series.filter(s => s.avgSec != null).length, label: weekly ? "Average time per sheet, by week" : "Average time per sheet, by day" };
    }
    function paintChart(D) {
      const lib = root.EfficiencyCharts, cd = chartData(D), hasData = cd.n > 0 && lib && typeof lib.bars === "function";
      E.chartBox.hidden = !hasData; if (!hasData) return;
      setText(E.ct, cd.label); setText(E.cs, "");
      if (!S.chart) { try { E.chart.textContent = ""; S.chart = lib.bars(E.chart, { height: 150, unit: "seconds", name: "Laser sheet times", emptyText: "No timed sheets in this range" }); } catch (e) { console.warn("[efficiency laser] chart:", e && e.message); S.chart = null; E.chartBox.hidden = true; return; } }
      const tip = p => {
        const m = p.meta; if (!m) return [];
        return cd.single ? [["Sheet", m.sheet], ["Pieces", nf(m.pieces)], ["Orders", nf(m.orders)], ["Counted from", m.seconds == null ? "no Laser sign-in" : (FROM[m.startedFrom] || "").replace(/^From /, "")]]
          : [["Sheets", nf(m.sheets)], ["Fastest", secTxt(m.fastestSec)], ["Slowest", secTxt(m.slowestSec)]].concat(m.timed < m.sheets ? [["With no time", nf(m.sheets - m.timed)]] : []);
      };
      try { S.chart.update({ x: cd.x, series: cd.series, meta: cd.meta }, { unit: "seconds", xKind: cd.xKind, tipRows: tip, name: cd.label, emptyText: "No timed sheets in this range" }); } catch (e) { console.warn("[efficiency laser] chart update:", e && e.message); }
    }
    function rowEl(r, td, fresh) {
      const li = doc.createElement("li"); li.className = "efpLsR" + (fresh && !still() ? " ent" : ""); li.dataset.sheet = r.sheetId;
      const d = nyDay(r.at);
      li.innerHTML = `<div class="efpLsT"><b>${esc(clockFmt.format(new Date(r.at)))}</b>${esc(d === td ? "Today" : mdLbl(d))}</div><div class="efpLsN"><span>${esc(r.sheet)}</span>${r.together > 1 ? `<small>Marked completed together with ${nf(r.together - 1)} other sheet${r.together > 2 ? "s" : ""}: the time is shared equally</small>` : ""}</div>`
        + `<span class="efpLsS"><span class="efpLsP">${nf(r.pieces)} ${r.pieces === 1 ? "piece" : "pieces"}</span><span class="efpLsO">${nf(r.orders)} ${r.orders === 1 ? "order" : "orders"}</span><span class="efpLsF">${esc(fromTxt(r))}</span></span>`
        + `<b class="efpLsM${r.seconds == null ? " none" : ""}">${esc(secTxt(r.seconds))}</b>`;
      return li;
    }
    function paintList(D) {
      const td = nyDay(now()), rows = D.rows, shown = rows.slice(0, S.shown), sig = shown.map(r => r.sheetId + "|" + r.at + "|" + r.seconds + "|" + r.startedFrom + "|" + r.together).join(",") + "|" + td;
      if (sig !== S.rowsSig) {
        const had = new Set([...E.list.children].map(c => c.dataset.sheet + "|" + c.dataset.at)), first = !S.rowsSig;
        E.list.textContent = ""; const frag = doc.createDocumentFragment();
        shown.forEach((r, i) => { const li = rowEl(r, td, !first && !had.has(r.sheetId + "|" + r.at) && i < 6); li.dataset.at = r.at; frag.appendChild(li); });
        E.list.appendChild(frag); S.rowsSig = sig;
      }
      const more = rows.length > shown.length ? `<button type="button" data-more>${rows.length - shown.length > options.listMore ? `Show ${options.listMore} more sheets` : `Show the other ${rows.length - shown.length} sheet${rows.length - shown.length === 1 ? "" : "s"}`}</button>` : rows.length > options.listFirst ? `<span>That is every sheet in this range</span>` : "";
      if (E.more._h !== more) { E.more._h = more; E.more.innerHTML = more; }
    }
    function paint() {
      if (S.dead) return;
      const D = S.data, exp = expected();
      if (!D) {
        const slow = S.busy && (S.slow || exp), failed = !S.busy && !!S.err && exp;
        show(slow || failed); E.wait.hidden = !slow; E.body.hidden = true; E.empty.hidden = !failed; if (failed) setText(E.empty, "Laser sheets not read · trying again");
        paintBusy(); setText(E.n, ""); setText(E.lr, failed ? rangeLabel() : ""); return;
      }
      E.wait.hidden = true;
      if (!D.found) {
        // nothing in this range: the section is not shown (a person who never cut a sheet has no laser section), except for somebody who works at the laser
        const on = exp; show(on); E.body.hidden = true; E.empty.hidden = !on;
        if (on) { setText(E.empty, S.err ? "Laser sheets not read · trying again" : `No laser sheet was marked completed in this range${S.range && S.range.from === S.range.to ? " day" : ""}.`); setText(E.lr, rangeLabel()); setText(E.n, ""); }
        paintBusy(); return;
      }
      show(true); E.empty.hidden = true; E.body.hidden = false; E.body.classList.toggle("dim", S.busy && S.key !== sigOf(S.range));
      setText(E.n, nf(D.totals.sheets)); setText(E.lr, rangeLabel());
      paintStats(D.totals); paintChart(D); paintList(D);
      const notes = D.notes.slice(); if (S.err) notes.push("Not read just now · trying again");
      const nt = notes.join(" "); E.note.hidden = !nt; setText(E.note, nt);
      paintBusy();
    }

    async function load() {
      if (S.dead || !S.range) return;
      clearTimeout(S.poll); S.poll = 0;
      const gen = ++S.gen, key = sigOf(S.range), range = { from: S.range.from, to: S.range.to };
      if (S.ctl) { try { S.ctl.abort(); } catch (_) {} } S.ctl = root.AbortController ? new root.AbortController() : null;
      S.busy = true; if (!S.data) { clearTimeout(S.wait); S.slow = false; S.wait = setTimeout(() => { S.slow = true; paint(); }, options.firstWaitMs); } paint();
      try {
        const r = await o.call({ op: "laserSheets", name: S.name, range }, S.ctl ? S.ctl.signal : undefined);
        if (gen !== S.gen || S.dead) return;
        if (!r || r.ok === false) throw new Error((r && r.error) || "not read");
        S.data = norm(r); S.key = key; S.err = ""; S.loadedAt = Date.now(); S.cache.set(key, S.data); if (S.cache.size > 10) S.cache.delete(S.cache.keys().next().value);
      } catch (e) {
        if (gen !== S.gen || S.dead || (e && e.name === "AbortError")) return;
        S.err = String((e && e.message) || e).slice(0, 100); try { o.onError && o.onError(e); } catch (_) {}
      } finally {
        if (gen === S.gen && !S.dead) { S.busy = false; clearTimeout(S.wait); S.wait = 0; paint(); schedule(); }
      }
    }
    function schedule() { clearTimeout(S.poll); S.poll = 0; if (S.dead || !S.range) return; S.poll = setTimeout(() => { S.poll = 0; if (visible()) load(); else schedule(); }, options.pollMs); }

    function setRange(r) {
      if (S.dead) return; const next = r && isDay(r.from) && isDay(r.to) ? { from: r.from, to: r.to } : null; if (!next || sigOf(next) === sigOf(S.range)) return;
      S.range = next; S.shown = options.listFirst; S.rowsSig = "";
      const hit = S.cache.get(sigOf(next)); if (hit) { S.data = hit; S.key = sigOf(next); }
      load();
    }
    function refresh() { if (!S.dead) { S.cache.clear(); load(); } }
    function unmount() {
      if (S.dead) return; S.dead = true; clearTimeout(S.poll); clearTimeout(S.wait); if (S.ctl) { try { S.ctl.abort(); } catch (_) {} }
      if (S.chart && S.chart.destroy) { try { S.chart.destroy(); } catch (_) {} } doc.removeEventListener("visibilitychange", onVisible); if (root0.parentNode) root0.remove(); instances.delete(api);
    }
    function onVisible() { if (S.dead || doc.visibilityState === "hidden" || !host.isConnected) return; if (Date.now() - S.loadedAt > 5000 && !S.busy) load(); }
    root0.addEventListener("click", e => { const b = e.target.closest && e.target.closest("button[data-more]"); if (!b || !S.data) return; S.shown += options.listMore; paintList(S.data); });

    show(false); doc.addEventListener("visibilitychange", onVisible);
    const api = { setRange, refresh, repaint: paint, unmount, destroy: unmount, el: root0, get state() { return { name: S.name, range: S.range, found: !!(S.data && S.data.found), sheets: S.data ? S.data.rows.length : 0, busy: S.busy, err: S.err, shown: S.shown, key: S.key }; } };
    instances.add(api);
    if (S.range) load();
    return api;
  }
  const instances = new Set();
  root.EfficiencyLaser = { mount, norm, secTxt, options, get instances() { return [...instances]; } };
})(typeof window !== "undefined" ? window : globalThis);
