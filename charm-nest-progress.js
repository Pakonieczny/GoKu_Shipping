/*  charm-nest-progress.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  One place for "this is going to take a moment". Every wait the app can
 *  measure reports here and is drawn as a real bar with a percentage, a
 *  count, the time spent and the time left — from the first millisecond,
 *  not after a delay, because a wait you are not told about is the one
 *  that feels broken.
 *
 *      const t = CNProgress.start("Loading the charm library");
 *      t.set(done, total);           // a bar, a percentage, a count, an estimate
 *      t.note("fetching page 3");    // a line under the label
 *      t.end();                      // always, including on failure
 *
 *  A task with no total still shows: a moving bar, the label and the time
 *  spent. One bar only: the task that began first is drawn, the others are
 *  a "+N", and the bar goes when the last one ends. A task started with
 *  { quiet: true } is never drawn at all.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  if (root.CNProgress) return;
  const tasks = new Map();
  let seq = 0, host = null, timer = null;

  const CSS = `
/* One bar for the whole page. It sits on the bottom edge, clear of the rail and of the station dock, and never stands
   between a person and what they are doing: pointer events pass through it. Only the task that began first is drawn —
   the rest are a count — so nothing stacks, jumps or pops in while a hand is moving something on the screen. */
.cnp{position:fixed;left:50%;transform:translateX(-50%);bottom:10px;z-index:60;width:min(560px,calc(100vw - 32px));pointer-events:none;font:12.5px/1.35 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;opacity:0;transition:opacity .18s ease}
.cnp.on{opacity:1}
.cnp .cnpRow{background:var(--ink,#221f1b);color:var(--paper,#f7f4ee);border-radius:12px;padding:7px 14px 9px;box-shadow:0 8px 24px -8px rgba(0,0,0,.5)}
.cnp .cnpHead{display:flex;gap:10px;align-items:baseline;min-width:0}
.cnp .cnpLabel{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0;flex:1 1 auto}
.cnp .cnpNote{opacity:.7;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:34%}
.cnp .cnpPct{font-variant-numeric:tabular-nums;font-weight:700;flex:0 0 auto}
.cnp .cnpMeta{font-variant-numeric:tabular-nums;opacity:.7;white-space:nowrap;flex:0 0 auto}
.cnp .cnpMore{opacity:.7;white-space:nowrap;flex:0 0 auto}
.cnp .cnpTrack{position:relative;height:5px;border-radius:9px;background:rgba(255,255,255,.18);overflow:hidden;margin-top:5px}
.cnp .cnpFill{position:absolute;inset:0 auto 0 0;width:0;border-radius:9px;background:var(--accent,#caa861);transition:width .25s ease-out}
.cnp .cnpTrack.cnpIndet .cnpFill{width:30%;animation:cnpSlide 1.15s ease-in-out infinite;transition:none}
@keyframes cnpSlide{0%{left:-32%}100%{left:102%}}
@media (prefers-reduced-motion:reduce){.cnp .cnpTrack.cnpIndet .cnpFill{animation:none;width:100%;opacity:.35}}`;

  function mount() {
    if (typeof document === "undefined") return null;                       // outside a page (tests, tooling) this draws nothing
    if (host && host.isConnected) return host;
    const st = document.createElement("style"); st.textContent = CSS; document.head.appendChild(st);
    host = document.createElement("div"); host.className = "cnp"; host.setAttribute("role", "status"); host.setAttribute("aria-live", "polite");
    host.innerHTML = `<div class="cnpRow"><div class="cnpHead"><span class="cnpLabel"></span><span class="cnpNote"></span><span class="cnpPct"></span><span class="cnpMeta"></span><span class="cnpMore"></span></div><div class="cnpTrack"><i class="cnpFill"></i></div></div>`;
    document.body.appendChild(host);
    return host;
  }
  const secs = ms => { const s = Math.max(0, Math.round(ms / 1000)); return s < 60 ? s + "s" : Math.floor(s / 60) + "m " + String(s % 60).padStart(2, "0") + "s"; };
  const nice = n => n >= 1000 ? n.toLocaleString() : String(n);

  function draw() {
    const h = mount(); if (!h) return; const now = Date.now();
    const list = [...tasks.values()].filter(t => !t.quiet);
    if (!list.length) { h.classList.remove("on"); if (timer) { clearInterval(timer); timer = null; } return; }
    h.classList.add("on");
    const t = list[0];                                                        // the one that began first; the rest are a count
    const q = s => h.querySelector(s);
    const known = t.total > 0;
    const frac = known ? Math.max(0, Math.min(1, t.done / t.total)) : 0;
    const elapsed = now - t.t0;
    q(".cnpLabel").textContent = t.label;
    q(".cnpNote").textContent = t.noteText || "";
    q(".cnpPct").textContent = known ? Math.floor(frac * 100) + "%" : "";
    let meta = known ? `${nice(t.done)} / ${nice(t.total)} · ${secs(elapsed)}` : secs(elapsed);
    if (known && t.done > 0 && frac < 1 && elapsed > 2500) meta += ` · about ${secs(elapsed / frac - elapsed)} left`;
    q(".cnpMeta").textContent = meta;
    q(".cnpMore").textContent = list.length > 1 ? `+${list.length - 1}` : "";
    const track = q(".cnpTrack"), fill = q(".cnpFill");
    track.classList.toggle("cnpIndet", !known);
    fill.style.width = known ? (frac * 100).toFixed(1) + "%" : "";
    if (!timer) timer = setInterval(draw, 250);
  }

  /** start(label, opts?) → { set, note, end }. opts: { total, note, quiet } — a quiet task is counted but never drawn */
  function start(label, opts) {
    const t = { id: ++seq, label: String(label || "Working"), total: (opts && +opts.total) || 0, done: 0, noteText: (opts && opts.note) || "", t0: Date.now(), quiet: !!(opts && opts.quiet) };
    tasks.set(t.id, t);
    draw();
    const api = {
      set(done, total, note) { t.done = +done || 0; if (total != null) t.total = +total || 0; if (note != null) t.noteText = String(note); draw(); return api; },
      total(n) { t.total = +n || 0; draw(); return api; },
      note(text) { t.noteText = text == null ? "" : String(text); draw(); return api; },
      label(text) { t.label = String(text); draw(); return api; },
      end() { tasks.delete(t.id); draw(); }
    };
    return api;
  }
  /** Run a promise under a bar that ends whatever happens. */
  async function wrap(label, fn, opts) {
    const t = start(label, opts);
    try { return await (typeof fn === "function" ? fn(t) : fn); } finally { t.end(); }
  }
  const count = () => tasks.size;
  function reset() { tasks.clear(); draw(); }
  root.CNProgress = { start, wrap, count, reset };
})(typeof window !== "undefined" ? window : globalThis);
