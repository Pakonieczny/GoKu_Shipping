/* charm-nest-seal-travel.js — every seal an ORDER has collected, wherever the order ends up (window.CNSealTravel)
 * (SEALTRAVEL, Paul 20 Oct 2026: "Depending on which button is pressed, the order may go to different tabs or folders, however all the particular seals must always follow this order. For example, Print QR label
 *  and Cancel Order can be part of the same order, so both those seals would have to travel with a given order until that order's eventual conclusion. All of these seals must be visible in all places where a given
 *  button is relevant or information about a given button's status is relevant.")
 *
 * ONE reader and ONE painter, by kind, so a new kind of seal shows everywhere with no more work:
 *
 *   CNSealTravel.of(rid, rows?, extra?, opts?) -> [records], oldest first; THE reader of an order's seals, the order window's too (winSealsOf). opts.events: the order's timeline events when the caller holds them. A record is { kind, how, at, by, id, label?, n?, src? } (src "record": from the custom records, which a line's own seals row already draws). Read from what the page already holds (no read, no timer, no listener):
 *                                          · the order's custom records (CustomPrint.sealsOfOrder: each QR print, an older Complete Order press, a send's sheet stamps), kinds "print" / "complete" / their own;
 *                                          · its Release Hold (Orders.releaseStampOf over its lines, so a hold put on again hides it as before, RELNOSEAL; the cloud's record when no line is in the pull);
 *                                          · every other seal record kept on the order's Review document ({rid}_review in the custom-order records: the same record and shape as the release one,
 *                                            { at, by, how, id }, under its own field — "cancel", "sheet", … — or listed under `seals`), which the maps' list and the changes feed bring to every computer and back after a reload;
 *                                          · its ORDER HELD seal (kind "hold"): the Hold Order press, the Review document's own mark (review.markedAt / markedBy), and the timeline's note of it;
 *                                          · extra: other objects that carry seal records under `seal` / `seals` (a Cancelled list's record).
 *   CNSealTravel.html(records, opts?)     -> the strip: each record drawn with its kind's label and ink (KINDS); print / complete exactly as Seal.row draws them (the button seals), release exactly as the Orders card
 *                                          draws it (Orders.releaseSealHtml: 72 px with its words), every other kind as the same worded seal in its own ink; a kind not in KINDS falls back to a neutral ink with its stored label.
 *                                          opts: { except: [kinds not drawn here, because the surface already draws them], size: smaller print / complete seals (Seal.row's mini row), wordSize: a smaller face for the worded seals (a narrow list) }. "" for none.
 *   CNSealTravel.sig(records)             -> the text that changes exactly when the strip does (a list paints a row again only then)
 *   CNSealTravel.KINDS                    -> { kind: { label, ink, family, icon, action } }
 */
(function (root) {
  "use strict";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const NEUTRAL = "#5f625c";
  /* (SEALTRAVEL, Paul 20 Oct 2026: one table, by kind: what the seal says and its ink. family/icon pick the outline and symbol of Seal.face (charm-nest-motion.js) for a kind Seal does not draw by itself) */
  const KINDS = {
    print:    { label: "QR Label Printed", ink: "#65737f", family: "prepared",   icon: "qr",        action: "QR LABEL PRINTED" },
    complete: { label: "Order Complete",   ink: "#77518a", family: "fulfilment", icon: "complete",  action: "ORDER COMPLETE" },
    release:  { label: "Release Hold",     ink: "#a2591c", family: "release",    icon: "release",   action: "RELEASE HOLD" },
    cancel:   { label: "Order Cancelled",  ink: "#982f48", family: "cancelled",  icon: "cancelled", action: "ORDER CANCELLED" },
    sheet:    { label: "Sent to Sheet",    ink: "#65737f", family: "prepared",   icon: "prepared",  action: "SENT TO SHEET" },
    // (HOLDORDER / SEALTRAVEL, Paul 20 Oct 2026: the Hold Order press (was Send to Review): ORDER HELD, in the family of a hold, as the timeline draws it (charm-nest-timeline-ui.js holdOrderNote))
    hold:     { label: "Order Held",       ink: "#9f483d", family: "exceptions", icon: "exceptions", action: "ORDER HELD" }
  };
  const ALIAS = { button: "complete", releaseHold: "release", released: "release", cancelled: "cancel", cancelOrder: "cancel", orderCancelled: "cancel", sendToSheet: "sheet", sentToSheet: "sheet", holdOrder: "hold", reviewMark: "hold", sendToReview: "hold", sentToReview: "hold", onHold: "hold", orderComplete: "complete", qr: "print", labelPrinted: "print" };
  const kindOf = (how, field) => { const k = String(how || field || "").trim(); return ALIAS[k] || k || "seal"; };
  const spec = kind => KINDS[kind] || null;
  const words = s => String(s || "").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").replace(/\b\w/g, c => c.toUpperCase()).trim();
  /** What Seal itself says of a kind (Seal.labelOf(how), SEALCANCEL), "" when Seal does not know it (its fallback, QR LABEL PRINTED). */
  const sealWords = how => { try { const S = root.Seal, w = S && S.labelOf ? S.labelOf(how) : ""; return w && (w !== "QR LABEL PRINTED" || how === "print") ? w : ""; } catch (_) { return ""; } };
  const labelOf = r => (spec(r.kind) && spec(r.kind).label) || String(r.label || "").trim() || words(sealWords(r.how).toLowerCase()) || words(r.kind) || "Seal";
  /** A kind's ink: the table's, else Seal.INK[how] (SEALCANCEL), else the neutral ink. */
  const inkOf = r => { const k = spec(r.kind); if (k) return k.ink; try { const i = root.Seal && Seal.INK && Seal.INK[r.how]; if (i) return i; } catch (_) {} return NEUTRAL; };

  /** One seal record from an object kept for it ({ at, by, how, id, label? }), or null. */
  function recOf(v, field) {
    if (!v || typeof v !== "object" || !(+v.at > 0) || +v.undoneAt > 0 || !(v.by != null || v.how || v.kind || v.id)) return null;   // (a seal undone, e.g. a cancel taken back with Restore, SEALCANCEL: no longer shown)
    const kind = kindOf(v.kind || v.how, field);
    const r = { kind, how: String(v.how || field || kind), at: +v.at, by: String(v.by || ""), id: String(v.id || "") };
    if (v.label) r.label = String(v.label).slice(0, 60);
    return r;
  }
  /** The seal records an object carries: under `seal` / `seals` (an array, or a map by kind), and with all = true under any field of its own (the Review document's release, cancel, sheet, …). */
  function gather(obj, all) {
    const out = []; if (!obj || typeof obj !== "object") return out;
    const take = (v, f) => { const r = recOf(v, f); if (r) out.push(r); };
    const list = obj.seals; if (Array.isArray(list)) list.forEach(v => take(v, "")); else if (list && typeof list === "object") for (const [f, v] of Object.entries(list)) take(v, f);
    if (obj.seal) take(obj.seal, "");
    if (all) for (const [f, v] of Object.entries(obj)) if (!["review", "seals", "seal", "stamps"].includes(f) && v && typeof v === "object" && !Array.isArray(v)) take(v, f);
    return out;
  }
  const reviewDoc = rid => { try { const m = root.B && root.B.maps && root.B.maps.reviewMarks; return (m && m[rid + "_review"]) || null; } catch (_) { return null; } };
  const rowsOfAll = rid => { try { return (root.Orders && root.Orders.rows ? root.Orders.rows() : []).filter(r => r && r.order && String(r.order.receiptId) === rid); } catch (_) { return []; } };

  const SKIP = new Set(["engraveApproved", "engravePlain", "monogramApproved"]);   // (the engraving and monogram approvals: drawn on their own pieces' cells and cards, beside their own Approve)
  /* (SEALTRAVEL, Paul 20 Oct 2026, with SEALWIN: ONE reader for the cards and the order window, so they can never disagree: the window's winSealsOf asks this, handing in what only it holds, the order's timeline events
     and its cancel record) */
  function of(rid, rows, extra, opts) {
    rid = String(rid == null ? "" : rid).replace(/\D/g, ""); if (!rid) return [];
    opts = opts || {};
    const all = Array.isArray(rows) ? rows.filter(Boolean) : rowsOfAll(rid), live = all.filter(r => r.state !== "gone");
    const out = [], events = Array.isArray(opts.events) ? opts.events : [];
    const add = (r, src) => { if (!r || SKIP.has(r.kind) || SKIP.has(r.how)) return; if (src) r.src = src; out.push(r); };
    // the pieces' records: each QR print (its number), an older Complete Order press, a send's sheet stamps (a line's own seals row already draws these: src "record")
    try {
      const CP = root.CustomPrint, S = root.Seal;
      if (CP && S) {
        const rec = CP.cardRecord ? CP.cardRecord(all.map(r => CP.recordOfRow ? CP.recordOfRow(r) : null).concat([CP.sealsOfOrder ? CP.sealsOfOrder(rid) : null])) : CP.sealsOfOrder(rid);
        for (const st of rec ? (S.ofPiece ? S.ofPiece(rec) : S.list(rec)) : []) { const k = kindOf(st.how); add(Object.assign({}, st, { kind: k === "seal" ? "print" : k, how: st.how || "print", at: +st.at || 0, by: String(st.by || "") }), "record"); }
      }
    } catch (_) {}
    // Release Hold: the Orders card's own reading (Orders.releaseStampOf: none while the order is held now, RELNOSEAL); else the order's Review document (every computer has it), and every release its timeline keeps
    const RM = root.CharmNestReviewMark;
    let rel = null; try { rel = root.Orders && root.Orders.releaseStampOf ? root.Orders.releaseStampOf(live.length ? live : all) : null; } catch (_) {}
    const heldNow = !rel && live.some(r => r.hold);
    if (rel) add({ kind: "release", how: "releaseHold", at: +rel.at, by: String(rel.by || ""), id: String(rel.id || "") });
    if (!heldNow) for (const e of events) if (e && e.type === "released" && e.data && e.data.seal === "releaseHold") add(recOf({ at: +e.data.sealAt || e.at, by: e.by, id: e.data.sealId || e.id, how: "releaseHold" }, ""));
    // the Review document's own reader (CharmNestReviewMark.sealsOf, SEALCANCEL: Release Hold and ORDER CANCELLED, an undone one left out); then every seal-shaped field it does not know yet, on the order's own
    // records in the maps (the release record's pattern: release, cancel, sheet ... and kinds still to come)
    try { for (const v of (RM && RM.sealsOf ? RM.sealsOf(rid) : []) || []) { const r = recOf(v, ""); if (r && !(r.kind === "release" && heldNow)) add(r); } } catch (_) {}
    try {
      for (const [k, doc] of Object.entries((root.B && root.B.maps && root.B.maps.reviewMarks) || {})) {
        if (!doc || String(doc.receiptId || String(k).split("_")[0]).replace(/\D/g, "") !== rid) continue;
        for (const r of gather(doc, true)) if (!(r.kind === "release" && heldNow)) add(r);
      }
    } catch (_) {}
    // ORDER HELD (HOLDORDER, Paul 20 Oct 2026): the Hold Order press (was Send to Review) is the order's Review mark (markedAt / markedBy, kept once resolved too) and the timeline's note of it (data.reviewMark
    // with no `how`, which charm-nest-timeline-ui.js draws as ORDER HELD): the same press from both is one seal
    try { const i = RM && RM.info ? RM.info(rid) : null; if (i && +i.at > 0) add({ kind: "hold", how: "holdOrder", at: +i.at, by: String(i.by || ""), id: `rvmark.${rid}.${+i.at}` }); } catch (_) {}
    for (const e of events) if (e && e.type === "note" && e.data && e.data.reviewMark && !e.data.how && +e.at > 0) add({ kind: "hold", how: "holdOrder", at: +e.at, by: String(e.data.by || e.by || ""), id: String(e.id || "") });
    for (const o of [].concat(extra || [])) for (const r of gather(o, false)) add(r);
    // each seal once (the same kind within 10 s, by the same person; a print by its number), oldest first
    const once = [];
    for (const r of out.sort((a, b) => a.at - b.at)) if (!once.some(o => o.kind === r.kind && Math.abs(o.at - r.at) < 10000 && (r.kind === "hold" || String(o.by) === String(r.by)) && (o.n || 0) === (r.n || 0))) once.push(r);
    return once;
  }

  const whenOf = t => new Date(+t).toLocaleString("en-US", { timeZone: "America/Toronto", month: "short", day: "numeric", year: "numeric" }) + " · " + new Date(+t).toLocaleTimeString("en-US", { timeZone: "America/Toronto", hour: "numeric", minute: "2-digit" });
  const ROW_KINDS = new Set(["print", "complete"]);
  /** Whether Seal draws this kind by itself (its own words and ink, e.g. how "sheet"): anything but its fallback, QR LABEL PRINTED. */
  const sealKnows = r => { try { return !!root.Seal && Seal.modelOf({ how: r.how, at: r.at }).action !== "QR LABEL PRINTED"; } catch (_) { return false; } };
  /** The ink face of one record: Seal's own when it knows the kind, else Seal.face with the kind's outline and words, in the kind's ink (neutral for a kind not in KINDS). */
  function faceOf(r, size) {
    const S = root.Seal, k = spec(r.kind), ink = inkOf(r), label = labelOf(r), title = `${label}${r.by ? " by " + r.by : " · operator not recorded"} · ${whenOf(r.at)}`;
    const sz = +size || S.BASE_SIZE;
    if (sealKnows(r)) return S.html({ how: r.how, at: r.at, by: r.by, id: r.id }, sz, "stFace").replace(/aria-label="[^"]*"/, `aria-label="${esc(title)}"`);
    const fam = k ? k.family : "received", famInk = (S.FAMILY && S.FAMILY[fam] && S.FAMILY[fam].ink) || "";
    let svg = S.face({ family: fam, action: k ? k.action : label.toUpperCase().slice(0, 18), icon: k ? k.icon : "qr", at: r.at, by: r.by });
    if (famInk && famInk.toLowerCase() !== ink.toLowerCase()) svg = svg.split(famInk).join(ink);
    return `<span class="seal seal-travel stFace" style="--rot:-6deg;--sz:${sz}px" data-at="${+r.at || 0}" tabindex="0" role="img" aria-label="${esc(title)}">${svg}</span>`;
  }
  /** One worded seal (the Release Hold seal's layout): the face, the kind's label, who and when, in the kind's ink. */
  function worded(r, size) {
    const O = root.Orders;
    if (r.kind === "release" && O && O.releaseSealHtml && !size) return O.releaseSealHtml({ how: "releaseHold", at: r.at, by: r.by, id: r.id });   // (the existing look, exactly)
    const ink = inkOf(r), label = labelOf(r), by = r.by || "someone", when = whenOf(r.at);
    return `<span class="sealTravelSeal" data-testid="travel-seal" data-seal-kind="${esc(r.kind)}" data-seal-at="${+r.at}" style="--stInk:${ink};border-color:${ink}61;background:${ink}12${size ? `;--seal-fit:${+size}px` : ""}" title="${esc(`${label} by ${by} · ${when}`)}">${faceOf(r, size)}<span class="stWords"><b>${esc(label)}</b><span class="stBy">by ${esc(by)}</span><span class="stAt">${esc(when)}</span></span></span>`;
  }
  function html(records, opts) {
    opts = opts || {}; const S = root.Seal; if (!S) return "";
    const skip = new Set([].concat(opts.except || [])), list = (records || []).filter(r => r && !skip.has(r.kind)); if (!list.length) return "";
    const parts = []; let run = [];
    const flush = () => { if (!run.length) return; parts.push(S.row({ stamps: run, prints: run.reduce((m, x) => Math.max(m, x.n || 0), 0) }, opts.size ? { size: opts.size } : {})); run = []; };
    for (const r of list) {
      if (ROW_KINDS.has(r.kind)) { run.push(Object.assign({}, r, { how: r.kind === "complete" ? "button" : "print" })); continue; }
      flush(); parts.push(worded(r, opts.wordSize));
    }
    flush();
    return `<span class="sealTravel" data-testid="seal-travel" data-seal-kinds="${esc([...new Set(list.map(r => r.kind))].join(" "))}">${parts.join("")}</span>`;
  }
  const sig = records => (records || []).map(r => r.kind + ":" + r.at + ":" + r.by + ":" + (r.n || 0) + ":" + (r.label || "")).join("|");

  // (SEALTRAVEL: the strip's own few rules; the worded seal has the Release Hold seal's layout (.relSeal), coloured by its kind, under its own class so nothing that looks for the Release Hold seal takes it for one)
  try {
    const st = root.document.createElement("style"); st.id = "sealTravelCss";
    st.textContent = ".sealTravel{display:flex;flex-wrap:wrap;align-items:center;gap:6px 10px;min-width:0;max-width:100%}.sealTravelSlot{margin:6px 0 0;flex:1 0 100%;min-width:0}.sealTravelSeal{--stInk:#5f625c;display:inline-flex;align-items:center;gap:7px;flex:0 1 auto;max-width:100%;min-width:0;margin:0;padding:4px 8px 4px 4px;border:1px solid;border-radius:9px;color:var(--stInk);box-sizing:border-box}.sealTravelSeal>.seal{flex:0 0 var(--seal-fit,48px);width:var(--seal-fit,48px);height:var(--seal-fit,48px);opacity:1}.stWords{display:flex;flex-direction:column;gap:0;min-width:0;line-height:1.25;font:8px var(--sans)}.stWords b{font-size:9.5px;font-weight:800;letter-spacing:.02em;color:var(--stInk);overflow-wrap:anywhere}.stWords .stBy{font-weight:600;color:var(--ink,#222);overflow-wrap:anywhere}.stWords .stAt{color:var(--ink70,#555)}";
    if (!root.document.getElementById(st.id)) root.document.head.appendChild(st);
  } catch (_) {}

  root.CNSealTravel = { of, html, sig, KINDS, gather };
})(typeof window !== "undefined" ? window : globalThis);
