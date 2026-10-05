/*  station-seals.js — an order's own seals in a station's pop-up (Paul, 5 Oct 2026: "The 'Complete' orders/pieces are missing
 *  their associated stamp/seal. Please review all modals and fix."). The Design Station's archived-order pop-up says
 *  "Completed {day} by {who}"; this puts the order's seals beside those words: ONE small seal (the press that completed it,
 *  the green ORDER COMPLETE or the blue QR LABEL PRINTED) and "+N" for the others, drawn by the very same Seal component
 *  as the Charm Sorter's (charm-nest-motion.js: Seal.compact, Seal.zoom), so it looks, zooms and fits the same way. A
 *  seal is permanent and has no tooltip or caption; it grows in place when rested on (500 ms) or clicked.
 *
 *    StationSeals.show(host, orderId, { size })  → Promise<boolean>
 *        host: the element the seal goes in (hidden until there is one); orderId: the receipt number. ONE read when
 *        it is called: the order's permanent timeline (OrderTimeline.get, order-timeline.js, which the page already
 *        loads: the server's timelineGet answers the recorded events and the ones it derives from the order's records,
 *        a Complete Order press and a QR label print each with its line, who and when). No Etsy call, no polling.
 *        The seal script is fetched ONLY when that timeline holds a seal to draw, once per page: an order with
 *        nothing recorded loads nothing and shows nothing (a seal is never made up: no person, no time, no seal).
 *    StationSeals.clear(host)  → the host emptied and hidden (a pop-up opening on another order)
 *
 *  Never breaks the page: every step is guarded, a failed timeline read or script load leaves the pop-up exactly as it
 *  was. Motion (the rest of charm-nest-motion.js) is NOT put on this page: the file would wrap every dialog's
 *  showModal/close with the sorter's grow-in and fly-back, so the dialog prototype is marked wrapped first and the file's
 *  own guard leaves the dialogs alone; only the Seal is wanted here. */
(function () {
  "use strict";
  if (window.StationSeals) return;
  const SRC = (() => { try { return new URL("charm-nest-motion.js?v=20261005-station-seals1", document.currentScript && document.currentScript.src ? document.currentScript.src : location.href).href; } catch (_) { return "charm-nest-motion.js"; } })();
  const READ_MS = 8000;
  /* The seal's own rules, the same lines the Charm Sorter's page carries (charm-nest-1.html): the size, the tilt, the
     overlap, nothing for pressing or stamping, which a pop-up that only shows what was recorded never does. */
  const CSS = [
    ":root{--seal-base:50px}",
    ".sealRow{display:inline-flex;align-items:center;flex-wrap:nowrap;gap:4px;pointer-events:none;margin:calc(var(--seal-fit,var(--seal-base)) * -.30) 0 calc(var(--seal-fit,var(--seal-base)) * -.18) 22px;min-width:0;max-width:100%;flex:0 1 auto;overflow:visible}",
    ".sealRow .seal+.seal{margin-left:0}",
    ".sealRow.mini{margin:calc(var(--seal-fit,var(--seal-base)) * -.28) 0 calc(var(--seal-fit,var(--seal-base)) * -.28) -12px}",
    ".seal{position:relative;display:inline-block;flex:0 0 var(--seal-fit,var(--seal-base));width:var(--seal-fit,var(--seal-base));height:var(--seal-fit,var(--seal-base));border-radius:50%;transform:rotate(var(--rot,-8deg));mix-blend-mode:multiply;opacity:.94;pointer-events:auto;transition:filter .9s ease-out,opacity .2s}",
    ".seal svg{display:block;width:100%;height:100%;overflow:visible;pointer-events:none}",
    "@media(prefers-reduced-motion:reduce){.seal{transition:none}}"
  ];
  const warn = (...a) => { try { console.warn("[StationSeals]", ...a); } catch (_) {} };
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const withTimeout = (p, ms) => new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout")), ms);
    Promise.resolve(p).then(v => { clearTimeout(t); res(v); }, e => { clearTimeout(t); rej(e); });
  });

  function css() {
    try {
      if (document.getElementById("stationSealCss")) return;
      const st = document.createElement("style"); st.id = "stationSealCss"; st.textContent = CSS.join("\n");
      (document.head || document.documentElement).appendChild(st);
    } catch (_) {}
  }
  /** The lines of the order that carry a seal event (a Complete Order press, a QR label print): the pieces to ask the Seal about. */
  function lineKeys(events) {
    const keys = [];
    for (const e of Array.isArray(events) ? events : []) {
      if (!e || (e.type !== "sealCompleted" && e.type !== "sealPrinted") || !(+e.at > 0)) continue;
      const k = String(e.lineKey || ""); if (k && !keys.includes(k)) keys.push(k);
    }
    return keys;
  }
  let loading = null;
  /** The Seal component, loaded once when first needed. → Promise<boolean>; false (never a throw) when it cannot be had. */
  function sealReady() {
    const ok = () => !!(window.Seal && typeof window.Seal.compact === "function");
    if (ok()) return Promise.resolve(true);
    if (loading) return loading;
    try {
      // (charm-nest-motion.js wraps dialogs unless the prototype says it was already wrapped: not on a station page)
      const P = window.HTMLDialogElement && window.HTMLDialogElement.prototype;
      if (P && !P._mdWrapped) P._mdWrapped = true;
    } catch (_) {}
    loading = new Promise(resolve => {
      try {
        const s = document.createElement("script"); s.src = SRC; s.async = true;
        s.onload = () => { loading = null; resolve(ok()); };
        s.onerror = () => { loading = null; try { s.remove(); } catch (_) {} warn("the seal script did not load"); resolve(false); };
        (document.head || document.documentElement).appendChild(s);
      } catch (e) { loading = null; warn("the seal script was not added", e); resolve(false); }
    });
    return loading;
  }
  function hide(host) { try { if (host) { host.innerHTML = ""; host.style.display = "none"; } } catch (_) {} }
  function clear(host) { try { if (host) host._ssTok = (host._ssTok || 0) + 1; hide(host); } catch (_) {} }

  async function show(host, orderId, opts) {
    try {
      if (!host) return false;
      const tok = host._ssTok = (host._ssTok || 0) + 1, id = digits(orderId);
      hide(host);
      const O = window.OrderTimeline;
      if (!id || !O || typeof O.get !== "function") return false;
      const tl = await withTimeout(O.get(id), READ_MS);
      if (host._ssTok !== tok || !host.isConnected) return false;
      const keys = lineKeys(tl && tl.events);
      if (!keys.length) return false;                       // nothing recorded: no seal, and nothing is loaded
      if (!(await sealReady()) || host._ssTok !== tok || !host.isConnected) return false;
      const html = window.Seal.compact(keys.map(k => ({ rec: null, lineKey: k, events: tl.events })), { size: Math.max(16, +(opts && opts.size) || 22) });
      if (!html) return false;
      css();
      host.innerHTML = html;
      host.style.display = "inline-flex"; host.style.alignItems = "center"; host.style.minWidth = "0";
      return true;
    } catch (e) { warn("no seal drawn", e && e.message || e); return false; }
  }
  window.StationSeals = { show, clear, lineKeys };
})();
