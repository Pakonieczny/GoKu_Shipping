/* qr-piece-label.js — the QR sticker of ONE piece of a multi-piece order (Paul, 14 Oct 2026).
 *
 * The rule: the Sorting Station prints ONE sticker per order, whatever the order holds (pairs, mismatched pairs and
 * multi-piece orders included). The only exception is a sticker a person prints BY HAND at the Design Station (the Charm
 * Sorter's Print QR label) for one piece of an order that has more pieces, for example an extra chain that never goes on a
 * sheet. That sticker is the same 1 x 1 in label: its QR is still the MASTER ORDER number (scanning it opens the master
 * order, and the link is never cut), and the note area on the right, where an ordinary sticker prints Metal / Title /
 * Match, says in big type what the sticker is for. The note is typed (or edited) at print time; it is wrapped and set at
 * the LARGEST size that fits the area, so a long message shrinks and a short one stays big.
 * NO "ORDER <number>" in the note area (Paul, 18 Oct 2026: "Remove the 'ORDER 4201047416' since it's a duplicate from the
 * partial 'Print QR' process"): the order number is already printed under the QR, with the date, as on every sticker. The
 * note has the whole area, and a word is not broken across lines when a size in range sets every word on one line.
 *
 *   QrPieceLabel.fitNote(text, { boxW, boxH, maxPt, minPt })  -> { pt, lines, h, w, fits, truncated }
 *   QrPieceLabel.layout(note, orderNumber)                    -> { note: {...}, geo } the note placed on the 72 x 72 pt page (no ORDER block since 18 Oct 2026)
 *   QrPieceLabel.nodes(layout)                                -> the one pdfMake text node QR Printer.html puts where Metal / Title / Match go
 *   QrPieceLabel.isPiece(cardRows, orderRows)                 -> true when the order has a line that is not on the card (the card is a piece of it)
 *   QrPieceLabel.nameOf(rows)                                 -> the note offered first (the piece's own name)
 *   QrPieceLabel.ask({ rid, note })                           -> a small dialog: the note, a live preview of its size; resolves to the note or null
 *
 * Pure apart from ask(): the same file runs in QR Printer.html, in the Charm Sorter and in node (tests). Nothing is stored. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module && module.exports) module.exports = api;
  else if (root) root.QrPieceLabel = api;
})(typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : this, function (root) {
  "use strict";

  const NOTE_MAX = 60;                    // characters a person may type
  const BOX = { x: 33, y: 0.5, w: 37, bottom: 52 };   // the right-hand area of the 72 x 72 pt page, in pt: right of the QR (as the Metal / Title / Match block), 2 pt margin on the right, down to the order number's line on the left
  const LINE = 1.2;                       // the line pitch counted per pt of type (pdfMake's Roboto lays out at 1.17: the extra is margin)
  const SAFETY = 1.02;                    // widths are Helvetica Bold's (wider than Roboto Bold's, apart from the digits, 1 % wider) plus 2 %
  const NOTE_MAX_PT = 10, NOTE_MIN_PT = 4.5, STEP = 0.25;

  // Helvetica Bold advance widths per 1000 em for ASCII 32..126
  const HB = [278, 333, 474, 556, 556, 889, 722, 278, 333, 333, 389, 584, 278, 333, 278, 278, 556, 556, 556, 556, 556, 556, 556, 556, 556, 556, 333, 333, 584, 584, 584, 611,
    975, 722, 722, 722, 722, 667, 611, 778, 722, 278, 556, 722, 611, 833, 722, 778, 667, 778, 722, 667, 611, 722, 667, 944, 667, 667, 611, 333, 278, 333, 584, 556,
    333, 556, 611, 556, 611, 556, 333, 611, 611, 278, 278, 556, 278, 889, 611, 611, 611, 611, 389, 556, 333, 611, 556, 778, 556, 556, 500, 389, 280, 389, 584];
  /** The width in pt of `text` set in bold at `pt`: a table, so the same answer in every browser and in node, and never narrower than the real glyphs. */
  function width(text, pt) {
    let em = 0;
    for (const ch of String(text == null ? "" : text)) {
      const c = ch.codePointAt(0);
      em += (c >= 32 && c <= 126 ? HB[c - 32] : c >= 0xC0 && c <= 0x24F ? 722 : 1000) / 1000;
    }
    return em * pt * SAFETY;
  }

  /** A note as a person may type it: one line of at most NOTE_MAX characters, no control characters, single spaces. */
  function cleanNote(s, max) {
    return Array.from(String(s == null ? "" : s).replace(/[\u0000-\u001f\u007f]+/g, " ").replace(/\s+/g, " ").trim()).slice(0, max || NOTE_MAX).join("").trim();
  }

  /** Words wrapped to `boxW` at `pt` (a "\n" starts a new line); a word wider than a line is broken by characters. */
  function wrap(text, pt, boxW, measure) {
    const m = measure || width, out = [];
    for (const para of String(text == null ? "" : text).split("\n")) {
      let cur = "";
      for (let w of para.split(/\s+/).filter(Boolean)) {
        while (m(w, pt) > boxW && w.length) {
          if (cur) { out.push(cur); cur = ""; }
          const cs = Array.from(w); let k = 1;
          while (k < cs.length && m(cs.slice(0, k + 1).join(""), pt) <= boxW) k++;
          out.push(cs.slice(0, k).join("")); w = cs.slice(k).join("");
        }
        if (!w) continue;
        const next = cur ? cur + " " + w : w;
        if (!cur || m(next, pt) <= boxW) cur = next; else { out.push(cur); cur = w; }
      }
      if (cur) out.push(cur);
    }
    return out;
  }

  /** The largest type size (maxPt down to minPt in `step`s) at which the whole text, wrapped, fits the box; { pt, lines, h, w, fits, truncated }.
   *  The longer the text, the smaller the size (never larger), so a message always fits the label. Text that does not fit even at minPt is
   *  shortened with an ellipsis until it does (truncated: true); the box is never overflowed. */
  function fitNote(text, o) {
    const p = Object.assign({ boxW: BOX.w, boxH: 30, maxPt: NOTE_MAX_PT, minPt: NOTE_MIN_PT, step: STEP, lineH: LINE, measure: width }, o || {});
    const src = String(text == null ? "" : text).replace(/[ \t]+/g, " ").trim();
    const take = (t, pt) => {
      const lines = wrap(t, pt, p.boxW, p.measure), h = lines.length * pt * p.lineH;
      return { pt, lines, h, w: lines.reduce((m, l) => Math.max(m, p.measure(l, pt)), 0), ok: h <= p.boxH + 1e-9 && lines.every(l => p.measure(l, pt) <= p.boxW + 1e-9) };
    };
    const n = Math.floor((p.maxPt - p.minPt) / p.step + 1e-9);
    for (let i = 0; i <= n; i++) {
      const pt = Math.round((p.maxPt - i * p.step) * 100) / 100, r = take(src, pt);
      if (r.ok) return { pt, lines: r.lines, h: r.h, w: r.w, fits: true, truncated: false };
    }
    const cs = Array.from(src);
    for (let k = cs.length - 1; k > 0; k--) {
      const r = take(cs.slice(0, k).join("").trimEnd() + "…", p.minPt);
      if (r.ok) return { pt: p.minPt, lines: r.lines, h: r.h, w: r.w, fits: true, truncated: true };
    }
    return { pt: p.minPt, lines: [], h: 0, w: 0, fits: false, truncated: true };
  }

  /** The piece sticker's note on the 72 x 72 pt page: what the sticker is for, set as large as it fits, in the whole note area (top-left of
   *  the text block, in pt). No ORDER <number> block (Paul, 18 Oct 2026: a duplicate of the number printed under the QR); `orderNumber` stays
   *  in the signature for the callers and is not drawn here. The largest size tried is the one at which the longest word still fits a line
   *  (on the fitter's 0.25 pt steps), so "BRACELET CHAIN 6190" is set BRACELET / CHAIN / 6190, not BRACE / LET; a word too long for a line
   *  even at the smallest size is still broken by characters, as before. */
  function layout(note, orderNumber, geo) {
    const g = Object.assign({}, BOX, geo || {});
    const text = cleanNote(note).toUpperCase() || "PIECE";
    const noteH = g.bottom - g.y;
    const widest = text.split(" ").reduce((m, w) => Math.max(m, width(w, 1)), 0);
    const whole = Math.floor(g.w / widest / STEP + 1e-9) * STEP;   // the largest size (on the steps) at which every word fits a line
    const big = fitNote(text, { boxW: g.w, boxH: noteH, maxPt: whole >= NOTE_MIN_PT ? Math.min(NOTE_MAX_PT, whole) : NOTE_MAX_PT, minPt: NOTE_MIN_PT });
    return {
      note: { text, pt: big.pt, lines: big.lines, x: g.x, y: g.y, w: big.w, h: big.h, boxH: noteH, truncated: big.truncated },
      geo: g
    };
  }

  /** The pdfMake node of a layout, the note alone (QR Printer.html puts it where an ordinary sticker prints Metal / Title / Match). */
  function nodes(L) {
    const at = part => ({ text: part.lines.join("\n"), fontSize: part.pt, bold: true, lineHeight: 1, absolutePosition: { x: part.x, y: part.y } });
    return [at(L.note)];
  }

  /** The card is ONE PIECE of a bigger order when the order has a line that is not on the card: [{ key }] each. */
  function isPiece(cardRows, orderRows) {
    const card = (cardRows || []).filter(r => r && r.key != null).map(r => String(r.key));
    if (!card.length) return false;
    return (orderRows || []).some(r => r && r.key != null && !card.includes(String(r.key)));
  }

  /** The note offered first: the card's own name (a special line's word, "Chain only"; else its design, else its title), at most 4 words a line. */
  function nameOf(rows) {
    const names = [];
    for (const r of (rows || []).slice(0, 2)) {
      const sp = (r && r.spec) || {}, ln = (r && r.line) || {};
      const raw = String((sp.special && sp.special.label) || sp.designSku || ln.sku || ln.title || "").replace(/_+/g, " ").replace(/\s+/g, " ").trim();
      const nm = raw.split(" ").slice(0, 4).join(" ");
      if (nm && !names.includes(nm)) names.push(nm);
    }
    return cleanNote(names.join(" + "));
  }

  /* ── the small dialog ── */
  const SCALE = 3.4;                       // preview pixels per pt
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  function css(doc) {
    if (doc.getElementById("qrPieceCss")) return;
    const s = doc.createElement("style"); s.id = "qrPieceCss";
    s.textContent = "dialog.pieceDlg{width:min(380px,94vw)}.pieceDlg .dlgBody p{margin:0 0 12px;line-height:1.5;color:var(--ink70)}"
      + ".pieceDlg .pieceRow{display:flex;gap:14px;align-items:flex-start}.pieceDlg .pieceRow .field{flex:1 1 auto;min-width:0;margin-bottom:0}"
      + ".pieceDlg .piecePrev{position:relative;flex:0 0 auto;border:1px dashed var(--line);border-radius:6px;background:#fff;overflow:hidden}"
      + ".pieceDlg .piecePrev div{position:absolute;left:0;white-space:nowrap;font:700 10px/1 Roboto,'Helvetica Neue',Arial,sans-serif;color:#000}"
      + ".pieceDlg .pieceSize{margin-top:8px;font-size:12px;color:var(--ink45);min-height:16px}";
    (doc.head || doc.documentElement).appendChild(s);
  }
  /** Asks what the sticker of this piece is for. o: { rid, note }. Resolves to the note (cleaned, at most NOTE_MAX characters) or null (cancelled). */
  function ask(o) {
    const doc = root && root.document, opt = o || {};
    if (!doc || typeof doc.createElement !== "function" || !doc.body) return Promise.resolve(cleanNote(opt.note) || null);
    return new Promise(resolve => {
      css(doc);
      const d = doc.createElement("dialog"); d.className = "splitDlg pieceDlg"; d.setAttribute("aria-labelledby", "pieceT");
      d.innerHTML = '<div class="dlg"><div class="dlgHead"><h3 id="pieceT">Label for this piece</h3></div>'
        + '<div class="dlgBody"><p>Order ' + esc(opt.rid) + ' has more pieces. This sticker is for this piece only; scanning it opens the order.</p>'
        + '<div class="pieceRow"><div class="field"><label for="pieceNote">What is it for?</label><input id="pieceNote" type="text" maxlength="' + NOTE_MAX + '" autocomplete="off" spellcheck="false"></div>'
        + '<div class="piecePrev" aria-hidden="true"></div></div><div class="pieceSize" role="status" aria-live="polite"></div></div>'
        + '<div class="dlgFoot"><div class="left"><button type="button" class="btn ghost sm" data-k="cancel">Cancel</button></div><button type="button" class="btn gold sm" data-k="go">Print label</button></div></div>';
      doc.body.appendChild(d);
      const input = d.querySelector("#pieceNote"), prev = d.querySelector(".piecePrev"), size = d.querySelector(".pieceSize"), go = d.querySelector("[data-k=go]");
      input.value = cleanNote(opt.note);
      let done = false;
      const finish = v => { if (done) return; done = true; try { if (d.open) d.close(); } catch (_) { d.removeAttribute("open"); } d.remove(); resolve(v); };
      const paint = () => {
        const t = cleanNote(input.value), L = layout(t, opt.rid), g = L.geo;
        go.disabled = !t;
        prev.style.width = Math.round(g.w * SCALE) + "px"; prev.style.height = Math.round((g.bottom - g.y) * SCALE) + "px";
        prev.innerHTML = "";
        for (const part of [L.note]) part.lines.forEach((line, i) => {
          const n = doc.createElement("div"); n.textContent = line;
          n.style.fontSize = Math.round(part.pt * SCALE * 10) / 10 + "px"; n.style.top = Math.round((part.y - g.y + i * part.pt * LINE) * SCALE * 10) / 10 + "px";
          prev.appendChild(n);
        });
        size.textContent = t ? "Prints at " + L.note.pt + " pt" + (L.note.truncated ? " (shortened to fit)" : "") : "Type what the sticker is for";
      };
      input.addEventListener("input", paint);
      input.addEventListener("keydown", e => { if (e.key === "Enter") { e.preventDefault(); if (!go.disabled) go.click(); } });
      go.onclick = () => { const t = cleanNote(input.value); if (t) finish(t); };
      d.querySelector("[data-k=cancel]").onclick = () => finish(null);
      d.addEventListener("cancel", e => { e.preventDefault(); finish(null); });
      paint();
      try { d.showModal(); } catch (_) { d.setAttribute("open", ""); }
      try { input.focus(); input.select(); } catch (_) { /* a hidden tab */ }
    });
  }

  return { NOTE_MAX, BOX, LINE, width, cleanNote, wrap, fitNote, layout, nodes, isPiece, nameOf, ask };
});
