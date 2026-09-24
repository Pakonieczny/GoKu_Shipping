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
 *  spent. One bar only: the newest task a person asked for (one that began
 *  within a moment of a click or key press) is drawn, else the task that
 *  began first; the others are a "+N", and the bar goes when the last one
 *  ends. A task started with { quiet: true } is never drawn at all.
 *
 *  A page with a #cnpSlot (the sorter's top bar) gets the small version in
 *  it: one line, the label, the percentage or the time spent, and a hairline
 *  bar, with the count, the time left and the other tasks in its tooltip.
 *  Other pages keep the bar on the bottom edge.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root) {
  "use strict";
  if (root.CNProgress) return;
  const tasks = new Map();
  let seq = 0, host = null, timer = null, lastInput = -Infinity, styled = false;
  // A task that begins within a moment of a click or key press is the person's own, and it is the one drawn: under a
  // long background task (indexing a master, pulling orders) their own action used to show only as "+1".
  const ASKED_MS = 1500;
  if (typeof document !== "undefined") for (const type of ["pointerdown", "keydown"]) document.addEventListener(type, () => { lastInput = Date.now(); }, true);

  const CSS = `
/* One bar for the whole page. It sits on the bottom edge, clear of the rail and of the station dock, and never stands
   between a person and what they are doing: pointer events pass through it. One task is drawn (what the person just
   asked for, else the one that began first) and the rest are a count, so nothing stacks, jumps or pops in while a hand
   is moving something on the screen. */
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
@media (prefers-reduced-motion:reduce){.cnp .cnpTrack.cnpIndet .cnpFill{animation:none;width:100%;opacity:.35}}
/* The small version, in a slot the page gives it (the sorter's top bar, Paul 24 Sep: the bar at the bottom took too much
   room). It is part of the bar it sits in, so it takes the pointer for its tooltip. */
.cnp.mini{position:static;left:auto;bottom:auto;transform:none;width:auto;min-width:0;max-width:100%;z-index:auto;pointer-events:auto;font:600 11px/1.3 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;transition:none;cursor:default}
.cnp.mini:not(.on){display:none}
.cnp.mini .cnpRow{background:none;color:inherit;border-radius:0;padding:0;box-shadow:none}
.cnp.mini .cnpHead{gap:6px}
.cnp.mini .cnpNote{display:none}
.cnp.mini .cnpPct,.cnp.mini .cnpMeta,.cnp.mini .cnpMore{font-weight:600;opacity:.62}
.cnp.mini .cnpPct:empty,.cnp.mini .cnpMeta:empty,.cnp.mini .cnpMore:empty{display:none}
.cnp.mini .cnpTrack{height:2px;margin-top:3px;background:rgba(28,26,23,.1)}`;

  function mount() {
    if (typeof document === "undefined") return null;                       // outside a page (tests, tooling) this draws nothing
    // the sorter gives it a slot at the right of its top bar; a bar drawn before the slot was parsed moves into it
    const slot = document.getElementById("cnpSlot");
    if (host && host.isConnected && (!slot || host.parentNode === slot)) return host;
    if (!styled) { const st = document.createElement("style"); st.textContent = CSS; document.head.appendChild(st); styled = true; }
    if (!host) {
      host = document.createElement("div"); host.className = "cnp"; host.setAttribute("role", "status"); host.setAttribute("aria-live", "polite");
      host.innerHTML = `<div class="cnpRow"><div class="cnpHead"><span class="cnpLabel"></span><span class="cnpNote"></span><span class="cnpPct"></span><span class="cnpMeta"></span><span class="cnpMore"></span></div><div class="cnpTrack"><i class="cnpFill"></i></div></div>`;
    }
    host.classList.toggle("mini", !!slot);
    (slot || document.body).appendChild(host);
    return host;
  }
  const secs = ms => { const s = Math.max(0, Math.round(ms / 1000)); return s < 60 ? s + "s" : Math.floor(s / 60) + "m " + String(s % 60).padStart(2, "0") + "s"; };
  const nice = n => n >= 1000 ? n.toLocaleString() : String(n);

  function draw() {
    const h = mount(); if (!h) return; const now = Date.now();
    const list = [...tasks.values()].filter(t => !t.quiet);
    if (!list.length) { h.classList.remove("on"); h.removeAttribute("title"); if (timer) { clearInterval(timer); timer = null; } return; }
    h.classList.add("on");
    const t = list.filter(x => x.asked).pop() || list[0];                    // the newest one asked for, else the first; the rest are a count
    const q = s => h.querySelector(s), mini = h.classList.contains("mini");
    const known = t.total > 0;
    const frac = known ? Math.max(0, Math.min(1, t.done / t.total)) : 0;
    const elapsed = now - t.t0;
    q(".cnpLabel").textContent = t.label;
    q(".cnpNote").textContent = t.noteText || "";
    q(".cnpPct").textContent = known ? Math.floor(frac * 100) + "%" : "";
    let meta = known ? `${nice(t.done)} / ${nice(t.total)} · ${secs(elapsed)}` : secs(elapsed);
    if (known && t.done > 0 && frac < 1 && elapsed > 2500) meta += ` · about ${secs(elapsed / frac - elapsed)} left`;
    // small: the percentage, or the time spent while there is nothing to count; the rest is in the tooltip
    q(".cnpMeta").textContent = mini ? (known ? "" : secs(elapsed)) : meta;
    q(".cnpMore").textContent = list.length > 1 ? `+${list.length - 1}` : "";
    if (mini) {
      h.title = [t.label + (t.noteText ? ` · ${t.noteText}` : ""), meta, ...list.filter(x => x !== t).slice(0, 6).map(x => `also: ${x.label}`)].join("\n");
      // short of room beside the tabs the numbers go first, then the words (the page's CSS then draws the bar on its edge)
      const room = h.parentNode.clientWidth || 0;
      h.classList.toggle("snug", room < 150); h.classList.toggle("tight", room < 60);
    }
    const track = q(".cnpTrack"), fill = q(".cnpFill");
    track.classList.toggle("cnpIndet", !known);
    fill.style.width = known ? (frac * 100).toFixed(1) + "%" : "";
    if (!timer) timer = setInterval(draw, 250);
  }

  /** start(label, opts?) → { set, note, end }. opts: { total, note, quiet } — a quiet task is counted but never drawn */
  function start(label, opts) {
    const t = { id: ++seq, label: String(label || "Working"), total: (opts && +opts.total) || 0, done: 0, noteText: (opts && opts.note) || "", t0: Date.now(), quiet: !!(opts && opts.quiet) };
    t.asked = t.t0 - lastInput < ASKED_MS;
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
