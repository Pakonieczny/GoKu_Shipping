/* charm-nest-refit.js — REFIT (Paul, 25 Nov 2026: "the user should be informed that the sheet is nesting again to fit the cheetah charm, and this needs to be clearly communicated to
   the user whenever looking at that sheet … All modals that show a sheet need to inform the user whenever that sheet is in the process of being adjusted to fit in existing charm. This
   must be visible and easy to understand at a glance").

   The cloud writes refit { at, jobId, pieces: [{ poolId, sku, order }] } on a sheet's record when it nests a sheet already laid out (released, in a set, approved) to fit a piece coming
   back to it (netlify/functions/_charmNestNest.js start). While that nest runs, and until the sheet's new files are written (previewAt past refit.at), every view of the sheet shows one
   plain banner: "Re-nesting to fit CHEETAH_8475 (order 4247431826): charms on this sheet may shift; none is taken off". Nothing is kept on the computer: what is shown is read from the
   records the page already reads (the Library's rows, the sheet window's record, the order window's pieces, the change feed's records), as they come.

   window.CNRefit: see(rec) · of(recOrId) → null | { phase: "nesting" | "files", pieces, at } · html(recOrId, { compact }) → "" | banner · card(el, sheet) · decorate() */
(function (root) {
  "use strict";
  const doc = root.document;
  const LIVE = ["queued", "running", "starting"], ENDED = ["error", "stopped", "stale"], STALE_MS = 45 * 60000;
  const num = v => (Number.isFinite(+v) ? +v : 0);
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const KNOWN = new Map();   // sheet id → { refit, nestJob, previewAt, seenAt } as last read (in memory only, while the page is open)
  const idOf = r => (r && (r.id || r.sheetId)) || "";
  const pick = r => { if (!r || typeof r !== "object") return null; const src = r.refit ? r : r.recalled && r.recalled.refit ? r.recalled : r.answer && r.answer.refit ? r.answer : r; return src; };

  /** A record (or a page holding one) is read: what it says of a refit is remembered for its sheet. */
  function see(rec) {
    try {
      const src = pick(rec), id = idOf(rec) || idOf(src); if (!src || !id) return;
      const was = KNOWN.get(id), at = num(src.updatedAt && src.updatedAt.toMillis ? src.updatedAt.toMillis() : src.updatedAt);
      if (was && at && was.updatedAt && at < was.updatedAt) return;   // (an older read of the record than one seen already)
      const has = src.refit !== undefined || src.nestJob !== undefined || src.previewAt !== undefined;
      if (!has && !was) return;
      KNOWN.set(id, { refit: src.refit !== undefined ? src.refit : was && was.refit, nestJob: src.nestJob !== undefined ? src.nestJob : was && was.nestJob, previewAt: src.previewAt !== undefined ? num(src.previewAt) : was ? was.previewAt : 0, updatedAt: at || (was && was.updatedAt) || 0 });
      soon();
    } catch (_) { /* a banner never stops a page */ }
  }
  /** What a sheet is going through now: null, or { phase, pieces, at }. */
  function of(x) {
    const id = typeof x === "string" ? x : idOf(x); if (x && typeof x === "object") see(x);
    const k = id ? KNOWN.get(id) : null, r = k && k.refit; if (!r || !num(r.at) || Date.now() - num(r.at) > STALE_MS) return null;
    const nj = k.nestJob && typeof k.nestJob === "object" ? k.nestJob : null, same = nj && String(nj.id || "") === String(r.jobId || "");
    if (same && LIVE.includes(String(nj.status || "queued"))) return { phase: "nesting", pieces: r.pieces || [], at: num(r.at) };
    if (same && ENDED.includes(String(nj.status || ""))) return null;
    if (num(k.previewAt) < num(r.at)) return { phase: "files", pieces: r.pieces || [], at: num(r.at) };   // (nested: its new files are being written)
    return null;
  }
  const words = pieces => { const list = (pieces || []).filter(Boolean), first = list.slice(0, 2).map(p => `${p.sku || "a charm"}${p.order ? ` (order ${p.order})` : ""}`).join(", "); return first + (list.length > 2 ? ` and ${list.length - 2} more` : ""); };
  /** The banner, or "". compact: one line (a card). */
  function html(x, o) {
    const s = of(x); if (!s) return "";
    const what = esc(words(s.pieces) || "a charm coming back to it");
    const head = s.phase === "nesting" ? `Re-nesting this sheet to fit ${what}` : `Re-nested to fit ${what}: writing its new files`;
    const tail = s.phase === "nesting" ? "Charms on this sheet may shift to make room; none is taken off. If it does not fit, it goes to the next draft of its metal." : "Its preview, files and labels are being updated.";
    if (o && o.compact) return `<div class="cnRefit compact" role="status" title="${esc(tail)}"><i class="cnRefitSpin" aria-hidden="true"></i><b>${head}</b></div>`;
    return `<div class="cnRefit" role="status"><i class="cnRefitSpin" aria-hidden="true"></i><div><b>${head}</b><span>${esc(tail)}</span></div></div>`;
  }
  const CSS = ".cnRefit{display:flex;gap:10px;align-items:flex-start;margin:8px 0;padding:10px 12px;border:1.5px solid #d97706;border-radius:10px;background:#fff7e6;color:#7c3a06;font:13px/1.4 var(--sans,system-ui);box-shadow:0 2px 8px rgba(217,119,6,.15)}"
    + ".cnRefit b{display:block;font-weight:700;color:#7c2d03}.cnRefit span{display:block;color:#8a4b12;font-size:12px;margin-top:2px}"
    + ".cnRefit.compact{margin:6px 0;padding:6px 9px;font-size:12px;align-items:center}.cnRefit.compact b{display:inline}"
    + ".cnRefitSpin{flex:none;width:14px;height:14px;margin-top:2px;border:2px solid rgba(217,119,6,.3);border-top-color:#d97706;border-radius:50%;animation:cnRefitSpin .8s linear infinite}"
    + ".cnRefit.compact .cnRefitSpin{margin-top:0}"
    + ".cnRefitFloat{position:absolute;top:12px;left:50%;transform:translateX(-50%);z-index:40;max-width:min(720px,90%);pointer-events:none}.cnRefitFloat .cnRefit{margin:0}"
    + "@keyframes cnRefitSpin{to{transform:rotate(360deg)}}@media(prefers-reduced-motion:reduce){.cnRefitSpin{animation:none}}";
  function css() { try { if (!doc || doc.getElementById("cnRefitCss")) return; const s = doc.createElement("style"); s.id = "cnRefitCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) {} }
  /** A slot of its own beside `anchor` (before it, or inside it as its first child), holding the banner of sheet `id`, or emptied. */
  function slot(anchor, id, how, compact) {
    if (!anchor || !id) return;
    const sel = `[data-cn-refit-for="${CSS_ESC(id)}"]`;
    let box = how === "inside" ? anchor.querySelector(`:scope > ${sel}`) : anchor.previousElementSibling && anchor.previousElementSibling.matches(sel) ? anchor.previousElementSibling : null;
    const h = html(id, { compact });
    if (!h) { if (box) box.remove(); return; }
    if (!box) { box = doc.createElement("div"); box.dataset.cnRefitFor = id; if (how === "inside") anchor.insertBefore(box, anchor.firstChild); else anchor.parentNode.insertBefore(box, anchor); }
    if (box.innerHTML !== h) box.innerHTML = h;
    return box;
  }
  const CSS_ESC = s => (root.CSS && root.CSS.escape ? root.CSS.escape(String(s)) : String(s).replace(/["\\]/g, "\\$&"));
  /** Every view of a sheet that is on screen: the Library's and the sets' pictures (img[data-sheet-preview]), the sheet window. */
  function decorate() {
    if (!doc) return; css();
    try { for (const img of doc.querySelectorAll("img[data-sheet-preview]")) slot(img, img.dataset.sheetPreview, "before", true); } catch (_) {}
    try {
      const SW = root.SheetWin, dlg = SW && SW.isOpen && SW.isOpen() ? doc.querySelector(".swStage") : null;
      if (SW && SW.isOpen && SW.isOpen() && dlg) {
        const id = SW.current && SW.current();
        if (getComputedStyle(dlg).position === "static") dlg.style.position = "relative";
        let fl = dlg.querySelector(":scope > .cnRefitFloat"); const h = id ? html(id) : "";
        if (!h) { if (fl) fl.remove(); } else { if (!fl) { fl = doc.createElement("div"); fl.className = "cnRefitFloat"; dlg.appendChild(fl); } if (fl.innerHTML !== h) fl.innerHTML = h; }
      }
    } catch (_) {}
  }
  /** A Nest tab card (charm-nest-1.html renderCard): its banner as the card's first line. */
  function card(el, sh) { try { if (!el || !sh) return; css(); const rec = sh.recalled || sh.answer || null; if (rec) see(Object.assign({ id: sh.sheetId }, rec)); slot(el, sh.sheetId, "inside", true); } catch (_) {} }
  let t = 0; function soon() { if (t || !doc) return; t = setTimeout(() => { t = 0; decorate(); }, 120); }
  // (the phase moves with time too: a nest that ends, files written: looked at again every 4 s while any sheet is being adjusted)
  if (doc) setInterval(() => { if ([...KNOWN.keys()].some(id => of(id))) decorate(); else if (doc.querySelector("[data-cn-refit-for], .cnRefitFloat")) decorate(); }, 4000);
  root.CNRefit = { see, of, html, card, decorate };
})(typeof window !== "undefined" ? window : this);
