/*  charm-nest-signins.js — the sorter's Sign-ins window (Paul, 28 Sep 23:53; plans/sign-in-sessions.md, part L).
 *  Who is signed in now, at which station and on which computer, and for how long (live); each day's sign-ins, per person
 *  or per computer, with start, end, how long and why it ended; and each person's time per day. Days are New York days,
 *  as the midnight sign-out's are.
 *  Read-only: charmNestLibrary op sessionsList over Station_Sessions, which the stations write (station-session.js,
 *  through firebaseOrders). Opened from the Workspace menu; reads only while the window is open (every 30 s while the tab
 *  is in sight), and every timer stops when it closes.
 *    SignIns.open()   SignIns.close()
 *    SignIns.model(sessions, now) → the grouped view (tests)   SignIns.nyDay(ms)   SignIns.nyMidnight("2026-09-28")
 *    SignIns.options = { pollMs, tickMs }   (read when the window opens)                                            */
(function (root) {
  "use strict";
  if (root.SignIns) return;
  const doc = root.document, TZ = "America/New_York", DAY_MS = 86400000;
  const options = { pollMs: 30000, tickMs: 15000 };
  const STATION = { sorting: "Sorting", welding: "Weld", assembly: "Assembly", shipping: "Shipping", design: "Design Station", laser: "Laser", sorter: "Sorter", qr: "QR Printer", inbox: "Inbox" };
  const ENDS = { signOut: ["Signed out", "neutral"], midnight: ["Midnight", "neutral"], switched: ["Switched person", "info"], closed: ["Closed · no heartbeat", "warn"],
    idle: ["Signed out · no input for 10 min", "neutral"], closing: ["Signed out at 5:00 pm", "neutral"] };       // idle and closing are normal sign-outs (the auto sign-out, Paul 6 Oct): the end time is the person's last input
  const RANGES = [[1, "Today"], [7, "7 days"], [30, "30 days"]];
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  /* ── New York days ── */
  const partsFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric" });
  function nyParts(t) { const o = {}; for (const p of partsFmt.formatToParts(new Date(t))) if (p.type !== "literal") o[p.type] = +p.value; return o; }
  const pad = n => String(n).padStart(2, "0");
  const nyDay = t => { const p = nyParts(t); return `${p.year}-${pad(p.month)}-${pad(p.day)}`; };
  /** The moment a New York day begins (its midnight), daylight saving included. */
  function nyMidnight(day) {
    const [y, m, d] = String(day).split("-").map(Number), want = Date.UTC(y, m - 1, d);
    let t = want;
    for (let i = 0; i < 3; i++) { const p = nyParts(t); t += want - Date.UTC(p.year, p.month - 1, p.day, p.hour % 24, p.minute, p.second); }
    return t;
  }
  const addDays = (day, n) => { const [y, m, d] = String(day).split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
  const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hour: "numeric", minute: "2-digit" });
  const dayFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, weekday: "short", month: "short", day: "numeric" });
  const clock = t => clockFmt.format(new Date(t));
  function dayName(day, today) {
    const d = dayFmt.format(new Date(nyMidnight(day) + DAY_MS / 2));
    return day === today ? `Today · ${d}` : day === addDays(today, -1) ? `Yesterday · ${d}` : d;
  }
  function dur(min) {
    const m = Math.round(Math.max(0, +min || 0));
    if (m < 1) return "under 1 min";
    return m < 60 ? `${m} min` : `${Math.floor(m / 60)} h${m % 60 ? ` ${m % 60} min` : ""}`;
  }

  /* ── the view: sessions grouped by day, then by person or computer; time per person per day ── */
  const personOf = s => String(s.person || "").trim() || "Unknown";
  const computerOf = s => String(s.computerLabel || "").trim() || (s.computerId ? `Computer ${String(s.computerId).slice(-4)}` : "Unknown computer");
  const stationOf = s => { const w = STATION[s.station] || s.station || "Station", d = String(s.device || "").trim(); return d && d.toLowerCase() !== String(s.station || "").toLowerCase() ? `${w} · ${d}` : w; };
  /** Minutes covered by these [start, end] spans, an overlap counted once (one person signed in on two computers). */
  function covered(spans) {
    let total = 0, a = -Infinity, z = -Infinity;
    for (const [s, e] of spans.slice().sort((x, y) => x[0] - y[0])) {
      if (s > z) { if (z > a) total += z - a; a = s; z = e; } else z = Math.max(z, e);
    }
    if (z > a) total += z - a;
    return total / 60000;
  }
  function model(list, now) {
    const today = nyDay(now);
    const rows = (Array.isArray(list) ? list : []).filter(s => s && +s.startAt > 0).map(s => {
      const start = +s.startAt, minutes = s.live ? Math.max(0, (now - start) / 60000) : Math.max(0, Number.isFinite(+s.minutes) ? +s.minutes : ((+s.endAt || start) - start) / 60000);
      return Object.assign({}, s, { start, end: start + minutes * 60000, minutes, day: nyDay(start), who: personOf(s), whoKey: personOf(s).toLowerCase(), where: computerOf(s), whereKey: String(s.computerId || computerOf(s)), station: stationOf(s) });
    });
    const live = rows.filter(r => r.live).sort((a, b) => a.start - b.start);
    const dayKeys = [...new Set(rows.map(r => r.day).concat(today))].sort().reverse();
    const group = (dayRows, by) => {
      const m = new Map();
      for (const r of dayRows) { const k = by === "computer" ? r.whereKey : r.whoKey; if (!m.has(k)) m.set(k, { key: k, label: by === "computer" ? r.where : r.who, rows: [] }); m.get(k).rows.push(r); }
      return [...m.values()].map(g => Object.assign(g, { rows: g.rows.sort((a, b) => a.start - b.start), minutes: covered(g.rows.map(r => [r.start, r.end])), live: g.rows.some(r => r.live),
        stations: [...new Set(g.rows.map(r => r.station))] })).sort((a, b) => b.minutes - a.minutes || a.label.localeCompare(b.label));
    };
    const days = dayKeys.map(day => { const dr = rows.filter(r => r.day === day); return { day, name: dayName(day, today), rows: dr, people: group(dr, "person"), computers: group(dr, "computer") }; });
    // time per person per day: one row per person, one column per day that had a sign-in (and today)
    const people = new Map();
    for (const r of rows) { if (!people.has(r.whoKey)) people.set(r.whoKey, { key: r.whoKey, label: r.who, days: {}, spans: [] }); const p = people.get(r.whoKey); (p.days[r.day] = p.days[r.day] || []).push([r.start, r.end]); p.spans.push([r.start, r.end]); }
    const totals = [...people.values()].map(p => ({ key: p.key, label: p.label, live: live.some(r => r.whoKey === p.key),
      days: Object.fromEntries(Object.entries(p.days).map(([d, s]) => [d, covered(s)])), total: Object.values(p.days).reduce((n, s) => n + covered(s), 0) })).sort((a, b) => b.total - a.total || a.label.localeCompare(b.label));
    return { today, live, days, totals, dayKeys };
  }

  /* ── the window ── */
  const st = { open: false, busy: false, gen: 0, data: null, off: 0, err: "", at: 0, range: 7, by: "person", poll: 0, tick: 0, sig: "", today: "" };
  const now = () => Date.now() + st.off;
  function style() {
    if (doc.getElementById("siStyle")) return;
    const s = doc.createElement("style"); s.id = "siStyle";
    s.textContent = `
dialog.siWin{width:min(1040px,95vw)}
dialog.siWin .dlg{height:min(80vh,760px)}
.siWin .dlgHead{flex-wrap:wrap;row-gap:8px}
.siWin .dlgHead .right{flex-wrap:wrap;justify-content:flex-end}
.siWin .seg button{padding:5px 10px;font-size:11.5px}
.siState{display:inline-flex;align-items:center;gap:7px;font-size:11.5px;color:var(--ink45);white-space:nowrap}
.siState.bad{color:var(--clay)}
.siWin .spin{width:11px;height:11px;border:2px solid var(--line);border-top-color:var(--ink70);border-radius:50%;animation:spin .7s linear infinite;flex:0 0 11px;display:inline-block}
.siBody{flex:1 1 auto;min-height:0}
.siBody>.section:first-child{margin-top:0}
.siWin .section b.n{color:var(--ink70);letter-spacing:0}
.siWin .section .aside{margin-left:auto;text-transform:none;letter-spacing:0;font-size:11.5px;color:var(--ink70);font-weight:650}
.siEmpty{display:flex;align-items:center;justify-content:center;gap:9px;padding:56px 0;color:var(--ink70);font-size:13px}
.siNote{color:var(--ink45);font-size:12px;padding:2px 0 4px}
.siNow{display:grid;gap:6px}
.siLive{display:grid;grid-template-columns:auto minmax(110px,1fr) minmax(120px,1.3fr) minmax(120px,1.3fr) auto auto;align-items:center;gap:12px;padding:9px 12px;border:1px solid var(--line);border-radius:10px;background:var(--card2);font-size:12.5px}
.siDot{width:8px;height:8px;border-radius:50%;background:var(--sage);box-shadow:0 0 0 3px var(--sageSoft)}
.siMuted{color:var(--ink70);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.siSince{color:var(--ink45);font-size:11.5px;white-space:nowrap}
.siDur{font-variant-numeric:tabular-nums;font-weight:700;white-space:nowrap;text-align:right}
.siScroll{overflow-x:auto;border:1px solid var(--line);border-radius:10px}
table.siT{width:100%;border-collapse:collapse;font-size:12.5px}
.siT th{font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--ink45);font-weight:700;text-align:left;padding:7px 10px;border-bottom:1px solid var(--line);white-space:nowrap;background:var(--card2)}
.siT td{padding:7px 10px;border-bottom:1px solid var(--line2);vertical-align:middle}
.siT tr:last-child td{border-bottom:0}
.siT .n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.siT td.n.none{color:var(--ink25)}
.siT .live{color:#3c5a39}
.siGroup{border:1px solid var(--line);border-radius:10px;margin:0 0 8px;overflow:hidden}
.siGHead{display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--card2);border-bottom:1px solid var(--line);font-size:12.5px;min-width:0}
.siGHead .siMuted{font-size:11.5px}
.siGHead .siDur{margin-left:auto}
.siGroup .siT th{background:transparent}
.siWin .pill{padding:3px 8px;font-size:10.5px}
@media (max-width:720px){.siLive{grid-template-columns:auto 1fr auto}.siLive>.siMuted,.siLive>.siSince{grid-column:2/-1}}
@media (prefers-reduced-motion:reduce){.siWin .spin{animation:none}}`;
    doc.head.appendChild(s);
  }
  let dlg = null;
  function dialog() {
    if (dlg) return dlg;
    dlg = doc.createElement("dialog"); dlg.id = "dlgSignins"; dlg.className = "wide siWin"; dlg.setAttribute("aria-labelledby", "siTitle");
    dlg.innerHTML = `<div class="dlg">
  <div class="dlgHead"><h3 id="siTitle">Sign-ins</h3><span class="siState" aria-live="polite"></span>
    <div class="right">
      <span class="seg" data-k="range" role="group" aria-label="Days shown">${RANGES.map(([v, t]) => `<button type="button" data-v="${v}">${t}</button>`).join("")}</span>
      <span class="seg" data-k="by" role="group" aria-label="Group sign-ins by"><button type="button" data-v="person">People</button><button type="button" data-v="computer">Computers</button></span>
      <button class="btn ghost xs" type="button" data-close>Close</button>
    </div></div>
  <div class="dlgBody siBody"></div>
</div>`;
    dlg.addEventListener("click", e => {
      const b = e.target.closest && e.target.closest("button"); if (!b || !dlg.contains(b)) return;
      if (b.hasAttribute("data-close")) return close();
      if (b.hasAttribute("data-retry")) return refresh();
      const seg = b.closest(".seg"); if (!seg) return;
      if (seg.dataset.k === "range" && +b.dataset.v !== st.range) { st.range = +b.dataset.v; st.gen++; st.busy = false; st.data = null; st.sig = ""; st.err = ""; segs(); render(); refresh(); }
      if (seg.dataset.k === "by" && b.dataset.v !== st.by) { st.by = b.dataset.v; segs(); render(true); }
    });
    dlg.addEventListener("close", stop);
    doc.body.appendChild(dlg);
    return dlg;
  }
  function segs() {
    dlg.querySelectorAll('.seg[data-k="range"] button').forEach(b => { const on = +b.dataset.v === st.range; b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
    dlg.querySelectorAll('.seg[data-k="by"] button').forEach(b => { const on = b.dataset.v === st.by; b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); });
  }
  function status() {
    const el = dlg && dlg.querySelector(".siState"); if (!el) return;
    el.classList.toggle("bad", !!st.err && !st.busy);
    el.innerHTML = st.busy ? `<span class="spin" aria-hidden="true"></span>Reading…` : st.err ? `Not read: ${esc(st.err.slice(0, 120))} · trying again` : st.at ? `Updated ${esc(clock(st.at + st.off))}` : "";
  }
  const pill = r => { if (r.live) return `<span class="pill ok">Signed in</span>`; const e = ENDS[r.endReason] || [r.endReason ? String(r.endReason) : "Ended", "neutral"]; return `<span class="pill ${e[1]}">${esc(e[0])}</span>`; };
  const durCell = (key, min, cls) => `<span class="${cls || "siDur"}" data-dur="${esc(key)}">${esc(dur(min))}</span>`;
  function sessionsTable(g, by) {
    const first = by === "computer" ? "Person" : "Station · computer";
    return `<table class="siT"><thead><tr><th>${first}</th><th>Start</th><th>End</th><th class="n">Duration</th><th>Ended</th></tr></thead><tbody>${g.rows.map(r => `<tr>
      <td>${by === "computer" ? `<b>${esc(r.who)}</b> <span class="siMuted">${esc(r.station)}</span>` : `${esc(r.station)} <span class="siMuted">· ${esc(r.where)}</span>`}</td>
      <td>${esc(clock(r.start))}</td><td>${r.live ? `<span class="live">now</span>` : esc(clock(r.end))}</td>
      <td class="n">${durCell("s:" + r.id, r.minutes, "")}</td><td>${pill(r)}</td></tr>`).join("")}</tbody></table>`;
  }
  function html(M) {
    const out = [];
    // signed in now
    out.push(`<div class="section">Signed in now <b class="n">${M.live.length}</b></div>`);
    out.push(M.live.length ? `<div class="siNow">${M.live.map(r => `<div class="siLive" data-id="${esc(r.id)}"><i class="siDot" aria-hidden="true"></i><b>${esc(r.who)}</b>
      <span class="siMuted" title="Station">${esc(r.station)}</span><span class="siMuted" title="Computer">${esc(r.where)}</span>
      <span class="siSince">since ${esc(clock(r.start))}</span>${durCell("s:" + r.id, r.minutes)}</div>`).join("")}</div>` : `<div class="siNote">Nobody is signed in right now.</div>`);
    // time per person per day
    const cols = M.dayKeys.filter(d => d === M.today || M.totals.some(p => p.days[d] != null));
    out.push(`<div class="section">Time signed in per person</div>`);
    out.push(M.totals.length ? `<div class="siScroll"><table class="siT"><thead><tr><th>Person</th>${cols.map(d => `<th class="n">${esc(d === M.today ? "Today" : dayFmt.format(new Date(nyMidnight(d) + DAY_MS / 2)))}</th>`).join("")}${cols.length > 1 ? `<th class="n">Total</th>` : ""}</tr></thead><tbody>${M.totals.map(p => `<tr><td><b>${esc(p.label)}</b></td>${cols.map(d => p.days[d] == null ? `<td class="n none">—</td>` : `<td class="n">${durCell(`t:${p.key}:${d}`, p.days[d], "")}</td>`).join("")}${cols.length > 1 ? `<td class="n"><b>${durCell("t:" + p.key, p.total, "")}</b></td>` : ""}</tr>`).join("")}</tbody></table></div>
      <div class="siNote">Time signed in anywhere; two computers at once count once.</div>` : `<div class="siNote">No sign-ins in these days.</div>`);
    // each day's sign-ins, per person or per computer
    for (const D of M.days) {
      const groups = st.by === "computer" ? D.computers : D.people;
      out.push(`<div class="section">${esc(D.name)} <b class="n">${D.rows.length} sign-in${D.rows.length === 1 ? "" : "s"}</b><span class="aside">${groups.length} ${st.by === "computer" ? (groups.length === 1 ? "computer" : "computers") : (groups.length === 1 ? "person" : "people")}</span></div>`);
      if (!D.rows.length) { out.push(`<div class="siNote">No sign-ins yet today.</div>`); continue; }
      for (const g of groups) out.push(`<div class="siGroup"><div class="siGHead"><b>${esc(g.label)}</b><span class="siMuted">${esc(st.by === "computer" ? g.stations.join(", ") : `${g.rows.length} sign-in${g.rows.length === 1 ? "" : "s"}`)}</span>${g.live ? `<span class="pill ok">now</span>` : ""}${durCell(`g:${st.by}:${D.day}:${g.key}`, g.minutes)}</div>${sessionsTable(g, st.by)}</div>`);
    }
    return out.join("");
  }
  /** Every duration the view shows, by its key: what a tick updates in place. */
  function durations(M) {
    const m = new Map();
    for (const D of M.days) for (const [by, gs] of [["person", D.people], ["computer", D.computers]]) for (const g of gs) { m.set(`g:${by}:${D.day}:${g.key}`, g.minutes); for (const r of g.rows) m.set("s:" + r.id, r.minutes); }
    for (const p of M.totals) { m.set("t:" + p.key, p.total); for (const [d, v] of Object.entries(p.days)) m.set(`t:${p.key}:${d}`, v); }
    return m;
  }
  function render(force) {
    if (!dlg) return;
    const body = dlg.querySelector(".siBody"), top = body.scrollTop;
    status();
    if (!st.data) {
      body.innerHTML = st.err && !st.busy ? `<div class="siEmpty">Sign-ins could not be read: ${esc(st.err.slice(0, 160))} <button class="btn ghost xs" type="button" data-retry>Try again</button></div>`
        : `<div class="siEmpty"><span class="spin" aria-hidden="true"></span>Reading sign-ins…</div>`;
      return;
    }
    const t = now(), M = model(st.data.sessions, t);
    st.today = M.today;
    const note = st.data.truncated ? `<div class="siNote">Showing the newest ${(st.data.sessions || []).length.toLocaleString()} sign-ins of these days.</div>` : "";
    body.innerHTML = note + html(M);
    if (!force) body.scrollTop = top;
  }
  function tick() {
    if (!st.open || !st.data || !dlg) return;
    const t = now();
    if (nyDay(t) !== st.today) return render();   // past midnight: today is a new day
    const d = durations(model(st.data.sessions, t));
    dlg.querySelectorAll("[data-dur]").forEach(el => { const v = d.get(el.dataset.dur); if (v != null) { const s = dur(v); if (el.textContent !== s) el.textContent = s; } });
  }
  async function read() {
    const CN = root.CN; if (!CN || typeof CN.api !== "function") throw new Error("the sorter is still starting");
    const since = nyMidnight(addDays(nyDay(now()), -(st.range - 1)));
    return CN.api("charmNestLibrary", { op: "sessionsList", since, limit: 1000 }, { quiet: true, timeoutMs: 30000 });
  }
  async function refresh() {
    if (!st.open || st.busy) return;
    const gen = st.gen; st.busy = true; status();
    if (!st.data) render();
    try {
      const r = await read(); if (gen !== st.gen || !st.open) return;
      st.off = Number.isFinite(+r.now) && +r.now > 0 ? +r.now - Date.now() : 0; st.err = ""; st.at = Date.now();
      const sig = JSON.stringify(r.sessions || []) + "|" + !!r.truncated;
      st.data = { sessions: Array.isArray(r.sessions) ? r.sessions : [], truncated: !!r.truncated };
      st.busy = false;
      if (sig !== st.sig) { st.sig = sig; render(); } else { status(); tick(); }
    } catch (e) {
      if (gen !== st.gen) return;
      st.err = String((e && e.message) || e || "no answer"); st.busy = false; render();
    } finally { if (gen === st.gen) { st.busy = false; status(); } }
  }
  const onVisible = () => { if (st.open && doc.visibilityState === "visible") { refresh(); tick(); } };
  function stop() {
    st.open = false; st.gen++; st.busy = false;
    clearInterval(st.poll); clearInterval(st.tick); st.poll = st.tick = 0;
    doc.removeEventListener("visibilitychange", onVisible);
  }
  function open() {
    try {
      style(); const d = dialog();
      if (d.open) return;
      st.open = true; st.gen++; segs(); render();
      try { d.showModal(); } catch (_) { d.setAttribute("open", ""); }
      refresh();
      st.poll = setInterval(() => { if (doc.visibilityState !== "hidden") refresh(); }, Math.max(1000, +options.pollMs || 30000));
      st.tick = setInterval(tick, Math.max(250, +options.tickMs || 15000));
      doc.addEventListener("visibilitychange", onVisible);
    } catch (e) { console.warn("[signins] not opened:", e); }
  }
  function close() {
    if (!dlg) return;
    stop();
    if (dlg.open) { try { dlg.close(); } catch (_) { dlg.removeAttribute("open"); } }
  }
  /** Its entry in the Workspace menu, just above Settings. */
  function mountMenu() {
    const list = doc.querySelector("#moreMenu .moreList"); if (!list) return false;
    if (doc.getElementById("btnSignins")) return true;
    const b = doc.createElement("button"); b.type = "button"; b.id = "btnSignins"; b.title = "Who is signed in where, and for how long";
    b.textContent = "Sign-ins";
    b.onclick = () => { const mm = doc.getElementById("moreMenu"); if (mm) mm.open = false; open(); };
    const settings = doc.getElementById("btnSettings");
    if (settings && settings.parentNode === list) list.insertBefore(b, settings); else list.appendChild(b);
    return true;
  }
  if (!mountMenu()) doc.addEventListener("DOMContentLoaded", mountMenu, { once: true });
  root.SignIns = { open, close, model, nyDay, nyMidnight, dur, options, get isOpen() { return st.open; } };
})(window);
