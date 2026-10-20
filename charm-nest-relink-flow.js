/* charm-nest-relink-flow.js — WRONGCHARM2 (Paul, 20 Oct 2026): "there must be some sort of pop-up with a progress bar that clearly identifies each place that the charm is being updated so the
   user understands what just happened". One small panel for every "relink everywhere" (Review.relinkFlow in the bridge: a "Wrong charm?" pick, a Review card's pick for a listing, the designs
   window's "Save to the charm repository and link this listing"): a title, a progress bar, one line per place updated (the repository, the listing's link, each order whose piece was made again,
   each sheet nested again, the pictures, Etsy), each with its state: working, done, a note for a person (amber), failed (red, with its reason), or what a rehearsal WOULD do. A final summary and a
   Close button; closing never stops the work (the bridge goes on, and its lines are kept on the handle).
   It sits in the top layer (a manual popover) so it shows over an open order window or designs window without making them a second modal: the person can close it or carry on.

     const f = CNRelinkFlow.open({ title, sub, sandbox })
     f.line(id, text, state, why?)    state: "work" | "done" | "warn" | "failed" | "skip" | "would"; the same id updates its line
     f.finish(summary, tone?)         tone: "ok" | "warn" | "bad"; the bar fills
     f.close() · f.el · f.lines() → [{ id, text, state, why }] · f.finished()

   UMD-free: window.CNRelinkFlow (the page only). No network, no clock but the panel's own. */
(function (root) {
  "use strict";
  if (!root || !root.document) return;
  const doc = root.document;
  const CSS = `.rlFlow{position:fixed;inset:auto 16px 16px auto;margin:0;width:min(440px,calc(100vw - 32px));max-height:min(70vh,560px);display:flex;flex-direction:column;gap:8px;box-sizing:border-box;padding:14px 16px 12px;border:1px solid var(--line,#ddd5c6);border-radius:14px;background:var(--paper,#fffdf8);color:var(--ink,#1c1a17);box-shadow:0 18px 48px rgba(28,26,23,.22);font:13px/1.35 var(--sans,system-ui,sans-serif);overflow:hidden;z-index:2147483000}
.rlFlow:not(:popover-open):not(.rlOpen){display:none}
.rlFlow h3{margin:0;font:600 14px/1.3 var(--sans,system-ui,sans-serif)}
.rlFlow .rlSub{margin:-4px 0 0;color:var(--ink45,#7d766a);font-size:12px}
.rlFlow .rlBar{height:6px;border-radius:3px;background:var(--line,#ece6da);overflow:hidden}
.rlFlow .rlBar i{display:block;height:100%;width:0;background:var(--gold,#b88a2e);transition:width .25s ease}
.rlFlow[data-tone="bad"] .rlBar i{background:#b3261e}
.rlFlow ol{list-style:none;margin:0;padding:0;overflow:auto;display:flex;flex-direction:column;gap:4px}
.rlFlow li{display:grid;grid-template-columns:16px 1fr;gap:6px;align-items:start}
.rlFlow li .rlI{width:12px;height:12px;margin-top:2px;border-radius:50%;box-sizing:border-box;border:2px solid var(--ink45,#9a9286)}
.rlFlow li[data-s="work"] .rlI{border-color:var(--gold,#b88a2e);border-right-color:transparent;animation:rlSpin .8s linear infinite}
.rlFlow li[data-s="done"] .rlI{border:0;background:#2e7d4f}
.rlFlow li[data-s="would"] .rlI{border-style:dashed}
.rlFlow li[data-s="warn"] .rlI{border:0;background:#c18a00}
.rlFlow li[data-s="failed"] .rlI{border:0;background:#b3261e}
.rlFlow li[data-s="failed"] .rlT{color:#b3261e}
.rlFlow li[data-s="warn"] .rlT{color:#7a5600}
.rlFlow li[data-s="skip"]{opacity:.6}
.rlFlow .rlWhy{display:block;font-size:12px;opacity:.9}
.rlFlow .rlEnd{display:flex;align-items:center;gap:8px;justify-content:space-between;border-top:1px solid var(--line,#ece6da);padding-top:8px}
.rlFlow .rlSum{font-weight:600}
.rlFlow .rlSum:empty::before{content:"Working… you can close this; it carries on.";font-weight:400;color:var(--ink45,#7d766a)}
@keyframes rlSpin{to{transform:rotate(360deg)}}
@media (prefers-reduced-motion:reduce){.rlFlow li[data-s="work"] .rlI{animation:none}.rlFlow .rlBar i{transition:none}}`;
  let styled = false;
  const style = () => { if (styled) return; styled = true; const s = doc.createElement("style"); s.id = "rlFlowCss"; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); };
  const FINAL = new Set(["done", "warn", "failed", "skip", "would"]);
  let seq = 0;

  function open(o) {
    o = o || {}; style();
    const el = doc.createElement("section"), id = "rlFlow" + (++seq);
    el.className = "rlFlow"; el.id = id; el.setAttribute("role", "status"); el.setAttribute("aria-live", "polite"); el.setAttribute("aria-label", o.title || "Updating");
    if (o.sandbox) el.dataset.sandbox = "1";
    el.innerHTML = `<h3></h3><p class="rlSub"></p><div class="rlBar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"><i></i></div><ol></ol><div class="rlEnd"><span class="rlSum"></span><button type="button" class="btn ghost sm" data-rl="close">Close</button></div>`;
    el.querySelector("h3").textContent = (o.sandbox ? "Rehearsal · " : "") + (o.title || "Updating");
    const sub = el.querySelector(".rlSub"); if (o.sub || o.sandbox) sub.textContent = o.sub || "Sandbox: what it would do. Nothing is written."; else sub.remove();
    const list = el.querySelector("ol"), bar = el.querySelector(".rlBar"), fill = bar.querySelector("i"), sum = el.querySelector(".rlSum");
    const lines = new Map(); let done = false;
    const paintBar = () => { const all = [...lines.values()], n = all.length, f = all.filter(x => FINAL.has(x.state)).length, pct = done ? 100 : n ? Math.round(90 * f / n) : 5; fill.style.width = pct + "%"; bar.setAttribute("aria-valuenow", String(pct)); };
    // (a modal window, the order window or the designs window, makes everything outside it inert: the panel lives inside the topmost one while one is open, else in the top layer (a manual popover),
    //  so it is always seen and can be closed; it moves when a window opens or closes)
    let mo = null;
    const place = () => {
      if (!el.isConnected && !h.placed) return;
      const modal = [...doc.querySelectorAll("dialog[open]")].filter(d => { try { return d.matches(":modal"); } catch (_) { return false; } }), host = modal[modal.length - 1] || doc.body;
      if (el.parentNode !== host) { try { if (el.matches(":popover-open")) el.hidePopover(); } catch (_) { /* not a popover */ } host.appendChild(el); }
      if (host === doc.body) { try { if (el.hasAttribute("popover") && !el.matches(":popover-open")) el.showPopover(); else if (!el.hasAttribute("popover")) el.classList.add("rlOpen"); } catch (_) { el.classList.add("rlOpen"); } }
      else el.classList.add("rlOpen");
    };
    try { mo = new MutationObserver(ms => { if (ms.some(m => m.target !== el && m.target.tagName === "DIALOG")) place(); }); mo.observe(doc.body, { attributes: true, attributeFilter: ["open"], subtree: true }); } catch (_) { mo = null; }
    const close = () => { h.placed = false; if (mo) mo.disconnect(); try { if (el.matches(":popover-open")) el.hidePopover(); } catch (_) { /* not a popover */ } el.classList.remove("rlOpen"); el.remove(); };
    el.querySelector("[data-rl=close]").onclick = close;
    if (typeof el.showPopover === "function") el.setAttribute("popover", "manual");
    const h = { placed: true,
      el,
      line(lid, text, state, why) {
        lid = String(lid || ("l" + lines.size)); state = state || "work";
        let x = lines.get(lid);
        if (!x) { const li = doc.createElement("li"); li.innerHTML = `<span class="rlI" aria-hidden="true"></span><span class="rlT"></span>`; li.dataset.id = lid; list.appendChild(li); x = { id: lid, li }; lines.set(lid, x); }
        x.text = String(text || ""); x.state = state; x.why = why ? String(why) : "";
        x.li.dataset.s = state; const t = x.li.querySelector(".rlT"); t.textContent = x.text;
        if (x.why) { const w = doc.createElement("span"); w.className = "rlWhy"; w.textContent = x.why; t.appendChild(w); }
        x.li.scrollIntoView && list.scrollHeight > list.clientHeight && x.li.scrollIntoView({ block: "nearest" });
        paintBar(); return h;
      },
      finish(summary, tone) { done = true; sum.textContent = String(summary || "Done"); el.dataset.tone = tone || ([...lines.values()].some(x => x.state === "failed") ? "bad" : [...lines.values()].some(x => x.state === "warn") ? "warn" : "ok"); el.dataset.done = "1"; paintBar(); return h; },
      close, finished: () => done,
      lines: () => [...lines.values()].map(x => ({ id: x.id, text: x.text, state: x.state, why: x.why })),
      shown: () => el.isConnected
    };
    doc.body.appendChild(el); place();
    return h;
  }
  root.CNRelinkFlow = { open };
})(typeof window !== "undefined" ? window : null);
