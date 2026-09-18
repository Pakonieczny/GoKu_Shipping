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
 *  spent. Tasks stack, newest last, and the strip disappears when the last
 *  one ends.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  if (root.CNProgress) return;
  const tasks = new Map();
  let seq = 0, host = null, timer = null;

  const CSS = `
/* Bottom left, clear of the top bar and of the station dock in the bottom right: a bar that says what the app is doing
   must never stand between a person and the way out of the screen they are on. */
.cnp{position:fixed;left:calc(var(--railW,344px) + 12px);bottom:12px;z-index:60;width:min(520px,calc(100vw - var(--railW,344px) - 24px));display:grid;gap:6px;pointer-events:none;font:12px/1.35 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}
.cnp:empty{display:none}
.cnp .cnpRow{background:var(--paper,#f7f4ee);border:1px solid var(--line,rgba(0,0,0,.12));border-radius:12px;padding:6px 12px 8px;box-shadow:0 6px 18px -8px rgba(0,0,0,.45)}
.cnp .cnpHead{display:flex;gap:10px;align-items:baseline;color:var(--ink,#221f1b)}
.cnp .cnpLabel{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cnp .cnpPct{margin-left:auto;font-variant-numeric:tabular-nums;font-weight:700}
.cnp .cnpMeta{font-variant-numeric:tabular-nums;color:var(--ink45,rgba(0,0,0,.55));white-space:nowrap}
.cnp .cnpNote{color:var(--ink45,rgba(0,0,0,.55));white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:38%}
.cnp .cnpTrack{position:relative;height:6px;border-radius:9px;background:var(--paper2,rgba(0,0,0,.08));overflow:hidden;margin-top:4px}
.cnp .cnpFill{position:absolute;inset:0 auto 0 0;width:0;border-radius:9px;background:var(--accent,#8a6b2f);transition:width .25s ease-out}
.cnp .cnpTrack.cnpIndet .cnpFill{width:30%;animation:cnpSlide 1.15s ease-in-out infinite;transition:none}
@keyframes cnpSlide{0%{left:-32%}100%{left:102%}}
@media (prefers-reduced-motion:reduce){.cnp .cnpTrack.cnpIndet .cnpFill{animation:none;width:100%;opacity:.35}}`;

  function mount() {
    if (typeof document === "undefined") return null;                       // outside a page (tests, tooling) this draws nothing
    if (host && host.isConnected) return host;
    const st = document.createElement("style"); st.textContent = CSS; document.head.appendChild(st);
    host = document.createElement("div"); host.className = "cnp"; host.setAttribute("role", "status"); host.setAttribute("aria-live", "polite");
    document.body.appendChild(host);
    return host;
  }
  const secs = ms => { const s = Math.max(0, Math.round(ms / 1000)); return s < 60 ? s + "s" : Math.floor(s / 60) + "m " + String(s % 60).padStart(2, "0") + "s"; };
  const nice = n => n >= 1000 ? n.toLocaleString() : String(n);

  function draw() {
    const h = mount(); if (!h) return; const now = Date.now();
    const list = [...tasks.values()];
    if (!list.length) { h.textContent = ""; if (timer) { clearInterval(timer); timer = null; } return; }
    for (const t of list) {
      if (!t.el) {
        t.el = document.createElement("div"); t.el.className = "cnpRow";
        t.el.innerHTML = `<div class="cnpHead"><span class="cnpLabel"></span><span class="cnpNote"></span><span class="cnpPct"></span><span class="cnpMeta"></span></div><div class="cnpTrack"><i class="cnpFill"></i></div>`;
        h.appendChild(t.el);
      }
      const q = s => t.el.querySelector(s);
      const known = t.total > 0;
      const frac = known ? Math.max(0, Math.min(1, t.done / t.total)) : 0;
      const elapsed = now - t.t0;
      q(".cnpLabel").textContent = t.label;
      q(".cnpNote").textContent = t.noteText || "";
      q(".cnpPct").textContent = known ? Math.floor(frac * 100) + "%" : "";
      let meta = known ? `${nice(t.done)} / ${nice(t.total)} · ${secs(elapsed)}` : secs(elapsed);
      if (known && t.done > 0 && frac < 1 && elapsed > 2500) meta += ` · about ${secs(elapsed / frac - elapsed)} left`;
      q(".cnpMeta").textContent = meta;
      const track = q(".cnpTrack"), fill = q(".cnpFill");
      track.classList.toggle("cnpIndet", !known);
      if (known) fill.style.width = (frac * 100).toFixed(1) + "%";
    }
    for (const el of [...h.children]) if (!list.some(t => t.el === el)) el.remove();
    if (!timer) timer = setInterval(draw, 250);
  }

  /** start(label, opts?) → { set, note, end }. opts: { total } */
  function start(label, opts) {
    const t = { id: ++seq, label: String(label || "Working"), total: (opts && +opts.total) || 0, done: 0, noteText: (opts && opts.note) || "", t0: Date.now(), el: null };
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
