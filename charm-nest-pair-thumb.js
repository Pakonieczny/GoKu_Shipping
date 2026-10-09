/*  charm-nest-pair-thumb.js — the ONE picture of a mismatched pair design (Paul, 9 Oct: "both vectors need to be shown in the
 *  thumbnails side-by-side", each charm "identified as the left and right of the same listing and same order").
 *
 *  A mismatched pair design draws two different bodies under one SKU: a left earring and a right earring. Wherever the
 *  application shows a design picture, that picture shows BOTH bodies side by side at the true relative scale of the master
 *  drawing (one scale for the whole drawing, never each body fitted to its own box) with a small chip under each body:
 *  Left under the left body, Right under the right body ("L" / "R" when the picture is small or the bodies are narrow).
 *
 *  This file is the only place that decides how that looks. The picture makers (charm-nest-pdf.js thumbnail, the compute
 *  worker's front picture, the bridge's renderFront, scripts/index-master.cjs thumbnailPng for the stored PNG) each ask it
 *  for the picture and keep their own code for every other charm, so a single charm or a matching pair is drawn by exactly
 *  the code it was drawn by before: plan(charm) is null for anything that is not a mismatched pair.
 *
 *  Which bodies a charm has, which side each is and whether the design is a mismatched pair come from CharmNestPair
 *  (charm-nest-pair.js, PAIRMASTER): bodiesOf, isMismatched, sideOf, sideLabel. This file never re-detects them.
 *
 *    plan(charm)                         null, or { bodies:[{ index, side, label, short, bbox, outline, members }] } left to right
 *    layout(plan, bbox, {size,padPt})    the picture's pixel layout: { s, W, H, H0, band, fontPx, chipH, chips:[...] }
 *    canvasFor(P, charm, opts)           the finished canvas (null when not a pair): opts { size, padPt, bg, makeCanvas, highlight, body }
 *                                        highlight "L" | "R": both bodies, the other washed out. body 0 | 1: that ear alone, at its scale in the pair
 *    paintTags(ctx, plan, tx, k, opts)   only the chips, on a canvas the caller already owns (a placed charm's drawing)
 *    svgPicture(plan, opts)              the same picture as an SVG string for the stored PNG (Resvg), opts { bbox, padPt, size, bg, inner }
 *    chipHtml(side, {short, px})         the same chip as markup, for pages that show the two ears as two pictures or rows
 *    chipsText(plan, size)               ["Left","Right"] or ["L","R"]: what the chips say at this picture size
 *    use(pairApi)                        tests only: stand in for CharmNestPair
 */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.CharmNestPairThumb = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  let injected = null;
  const pairApi = () => {
    if (injected) return injected;
    if (root && root.CharmNestPair) return root.CharmNestPair;
    if (typeof module === "object" && module.exports && typeof require === "function") { try { return require("./charm-nest-pair.js"); } catch (_) {} }
    return null;
  };

  /* ── the look, in one place ── */
  const SHORT_BELOW_PX = 110;                 // under this long side the chips say L / R
  const INK = "#2a2724", INK_TEXT = "#fffaf0", FONT = '"Source Sans 3","Segoe UI",system-ui,-apple-system,Helvetica,Arial,sans-serif';
  const FONT_FRAC = 0.07, MIN_FONT_PX = 8;     // chip text is 7 percent of the picture's long side: the same look at 168 px and at 2048 px
  const WASH_ALPHA = 0.72;                    // the body that is not being pointed at, when `highlight` is given
  // average advance widths in em of Source Sans 3 Semibold: the canvas and the SVG both lay the chips out from these, so they agree
  const ADV = { L: .5, e: .5, f: .31, t: .35, R: .6, i: .27, g: .52, h: .55 };
  const textEm = s => { let w = 0; for (const ch of String(s)) w += ADV[ch] != null ? ADV[ch] : .55; return w; };

  const num = Number.isFinite;
  const okBox = b => Array.isArray(b) && b.length === 4 && b.every(num);

  /** null for every charm that is not a mismatched pair (the caller then runs its old code, unchanged). */
  const memo = typeof WeakMap === "function" ? new WeakMap() : null;
  function plan(charm) {
    if (!charm || !okBox(charm.bbox)) return null;
    const P = pairApi(); if (!P || typeof P.isMismatched !== "function" || typeof P.bodiesOf !== "function") return null;
    const sig = (charm.members ? charm.members.length : 0) + "|" + charm.bbox.join(",");
    if (memo) { const hit = memo.get(charm); if (hit && hit.sig === sig) return hit.plan; }
    let out = null;
    try {
      if (P.isMismatched(charm)) {
        const bodies = P.bodiesOf(charm);
        if (Array.isArray(bodies) && bodies.length === 2 && bodies.every(b => b && okBox(b.bbox))) {
          const sorted = bodies.slice().sort((a, b) => (a.bbox[0] + a.bbox[2]) - (b.bbox[0] + b.bbox[2]));
          out = { bodies: sorted.map((b, i) => {
            const side = typeof P.sideOf === "function" ? P.sideOf(i, 2) : (i === 0 ? "L" : "R");
            const label = (typeof P.sideLabel === "function" ? P.sideLabel(side) : "") || (i === 0 ? "Left" : "Right");
            return { index: b.index != null ? b.index : i, side, label, short: label.charAt(0).toUpperCase(), bbox: b.bbox.slice(), outline: b.outline || null, members: b.members || null };
          }) };
        }
      }
    } catch (_) { out = null; }
    if (memo) memo.set(charm, { sig, plan: out });
    return out;
  }

  /** The chips' words at this picture size: the long form unless the picture is small or the two chips would touch. */
  function chipsText(pl, size, chipBoxes) {
    const long = pl.bodies.map(b => b.label), short = pl.bodies.map(b => b.short);
    if (size < SHORT_BELOW_PX) return short;
    if (chipBoxes && chipBoxes.touching) return short;
    return long;
  }

  /** The picture's pixel layout. size = the long side the maker was asked for. The whole picture, band included, stays inside size x size:
      the drawing gets one scale s, and the chips go in a band under it. */
  function layout(pl, bbox, o) {
    o = o || {}; const size = +o.size || 168, pad = o.padPt != null ? +o.padPt : 2;
    const fontPx = Math.max(MIN_FONT_PX, size * FONT_FRAC), chipH = Math.round(fontPx * 1.55 * 10) / 10, gap = Math.max(2, fontPx * .45);
    const band = Math.ceil(chipH + gap * 2);
    const w = bbox[2] - bbox[0] + 2 * pad, h = bbox[3] - bbox[1] + 2 * pad;
    const s = Math.min(size / w, Math.max(8, size - band) / h);
    const W = Math.max(8, Math.round(w * s)), H0 = Math.max(8, Math.round(h * s));
    const place = words => {
      const padX = fontPx * .55, items = pl.bodies.map((b, i) => {
        const cx = ((b.bbox[0] + b.bbox[2]) / 2 - bbox[0] + pad) * s, cw = textEm(words[i]) * fontPx + 2 * padX;
        return { text: words[i], side: b.side, cx, w: cw, x: Math.min(Math.max(cx - cw / 2, 1), Math.max(1, W - cw - 1)) };
      });
      items.touching = items.length === 2 && items[0].x + items[0].w + gap > items[1].x;
      return items;
    };
    let chips = place(chipsText(pl, size));
    if (chips.touching && size >= SHORT_BELOW_PX) chips = place(chipsText(pl, size, chips));
    return { s, W, H: H0 + band, H0, band, fontPx, chipH, gap, chips, baseline: H0 + gap + chipH / 2 + fontPx * .35, y: H0 + gap, pad, size };
  }

  function pill(ctx, x, y, w, h, r) {
    ctx.beginPath(); ctx.moveTo(x + r, y); ctx.lineTo(x + w - r, y); ctx.arc(x + w - r, y + r, r, -Math.PI / 2, Math.PI / 2); ctx.lineTo(x + r, y + h); ctx.arc(x + r, y + r, r, Math.PI / 2, Math.PI * 1.5); ctx.closePath();
  }
  /** Draw the chips from a layout (px). `dim` = the side that is washed out (its chip is faded too). */
  function paintChips(ctx, L, dim) {
    ctx.save(); ctx.font = `600 ${L.fontPx}px ${FONT}`; ctx.textAlign = "center"; ctx.textBaseline = "alphabetic";
    for (const c of L.chips) {
      ctx.globalAlpha = dim && c.side === dim ? .45 : 1;
      ctx.fillStyle = INK; pill(ctx, c.x, L.y, c.w, L.chipH, L.chipH / 2); ctx.fill();
      ctx.fillStyle = INK_TEXT; ctx.fillText(c.text, c.x + c.w / 2, L.baseline);
    }
    ctx.restore();
  }

  /** Wash out one body (highlight "L" washes the right one). Only when the two bodies do not overlap sideways: a wash over a body that
      reaches into the other would hide part of the one being pointed at. */
  function washRect(pl, bbox, L, highlight) {
    if (highlight !== "L" && highlight !== "R") return null;
    if (pl.bodies.length !== 2) return null;
    const [a, b] = pl.bodies; if (a.bbox[2] > b.bbox[0]) return null;
    const other = highlight === "L" ? b : a, k = L.s;
    return { x: (other.bbox[0] - bbox[0] + L.pad) * k - 1, w: (other.bbox[2] - other.bbox[0]) * k + 2, side: other.side };
  }

  /** The finished canvas of a mismatched pair, or null for any other charm. `P` is CharmNestPDF (its drawCharm draws the charm exactly as every other picture does). */
  function canvasFor(P, charm, o) {
    o = o || {}; const pl = plan(charm); if (!pl) return null;
    if (!P || typeof P.drawCharm !== "function" || typeof o.makeCanvas !== "function") return null;
    if (o.body === 0 || o.body === 1) return bodyCanvas(P, charm, pl, o);
    const b = charm.bbox, L = layout(pl, b, o), cv = o.makeCanvas(L.W, L.H), ctx = cv.getContext("2d");
    ctx.fillStyle = o.bg || "#fff"; ctx.fillRect(0, 0, L.W, L.H);
    P.drawCharm(ctx, charm, (x, y) => [(x - b[0] + L.pad) * L.s, (b[3] + L.pad - y) * L.s], L.s);
    const wash = washRect(pl, b, L, o.highlight);
    if (wash) { ctx.save(); ctx.globalAlpha = WASH_ALPHA; ctx.fillStyle = o.bg || "#fff"; ctx.fillRect(wash.x, 0, wash.w, L.H0); ctx.restore(); }
    paintChips(ctx, L, wash ? wash.side : null);
    return cv;
  }

  /** ONE ear alone (opts.body 0 = Left, 1 = Right): that body at the scale it has in the pair's own picture of this size (so a left and a right picture
      shown beside each other keep the true relative size of the two bodies), with its one chip under it. */
  function bodyCanvas(P, charm, pl, o) {
    const one = pl.bodies[o.body]; if (!one || !okBox(one.bbox)) return null;
    const lp = layout(pl, charm.bbox, o), pad = lp.pad, k = lp.s, bb = one.bbox;
    const words = chipsText({ bodies: [one] }, lp.size), chipW = textEm(words[0]) * lp.fontPx + 2 * lp.fontPx * .55;
    const bw = (bb[2] - bb[0] + 2 * pad) * k, W = Math.max(8, Math.round(Math.max(bw, chipW + 2))), H0 = Math.max(8, Math.round((bb[3] - bb[1] + 2 * pad) * k)), offX = (W - bw) / 2;
    const L = { s: k, W, H: H0 + lp.band, H0, band: lp.band, fontPx: lp.fontPx, chipH: lp.chipH, gap: lp.gap, pad, size: lp.size, y: H0 + lp.gap, baseline: H0 + lp.gap + lp.chipH / 2 + lp.fontPx * .35,
      chips: [{ text: words[0], side: one.side, w: chipW, x: (W - chipW) / 2 }] };
    const one1 = Object.assign({}, charm, { outline: one.outline || charm.outline, members: one.members && one.members.length ? one.members : charm.members, bbox: bb.slice() });
    const cv = o.makeCanvas(L.W, L.H), ctx = cv.getContext("2d");
    ctx.fillStyle = o.bg || "#fff"; ctx.fillRect(0, 0, L.W, L.H);
    P.drawCharm(ctx, one1, (x, y) => [(x - bb[0] + pad) * k + offX, (bb[3] + pad - y) * k], k);
    paintChips(ctx, L, null);
    return cv;
  }

  /** Only the chips, for a canvas that already holds a charm drawn with transform tx (points to px) and scale k. They sit just under each body
      (or at opts.y). A placed charm's panel has no band, so a body near the bottom edge gets its chip clamped inside the canvas. */
  function paintTags(ctx, pl, tx, k, o) {
    o = o || {}; if (!pl || !ctx || !ctx.canvas) return false;
    const cw = ctx.canvas.width, ch = ctx.canvas.height, size = Math.max(cw, ch);
    const fontPx = o.fontPx || Math.max(MIN_FONT_PX, Math.min(cw, ch) * FONT_FRAC), chipH = Math.round(fontPx * 1.55 * 10) / 10, gap = Math.max(2, fontPx * .45), padX = fontPx * .55;
    const words = chipsText(pl, size);
    let bottom = 0; for (const b of pl.bodies) bottom = Math.max(bottom, tx(0, b.bbox[1])[1]);
    const y = o.y != null ? o.y : Math.min(bottom + gap, ch - chipH - 1);
    const chips = pl.bodies.map((b, i) => { const cx = tx((b.bbox[0] + b.bbox[2]) / 2, 0)[0], w = textEm(words[i]) * fontPx + 2 * padX; return { text: words[i], side: b.side, w, x: Math.min(Math.max(cx - w / 2, 1), Math.max(1, cw - w - 1)) }; });
    paintChips(ctx, { chips, y, chipH, fontPx, baseline: y + chipH / 2 + fontPx * .35 }, null);
    return true;
  }

  /** The same chip as a piece of page markup, for places that show the two ears as TWO pictures or rows (a Left row and a Right row, a station tile):
      side "L" | "R" (or a body index 0 | 1), opts.short for L / R. Inline style only, so it needs no page CSS and looks the same on every page. */
  function chipHtml(side, o) {
    o = o || {}; const sd = side === 0 ? "L" : side === 1 ? "R" : side; if (sd !== "L" && sd !== "R") return "";
    const label = sd === "L" ? "Left" : "Right", text = o.short ? sd : label, px = +o.px || 11;
    return `<span class="pairChip" data-side="${sd}" title="${label} ear" style="display:inline-block;vertical-align:middle;padding:0 ${f2(px * .55)}px;border-radius:999px;background:${INK};color:${INK_TEXT};font:600 ${f2(px)}px/${f2(px * 1.55)}px ${FONT.replace(/"/g, "'")};white-space:nowrap">${text}</span>`;
  }

  const esc = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const f2 = v => (Math.round(v * 100) / 100).toString();
  /** The stored PNG's picture as SVG: o.inner is the caller's already-built body markup in POINT units with y UP (the caller's own
      `<g transform="scale(1 -1)">` content); this wraps it with the pair layout (one scale, a band, the chips). Font: Source Sans 3. */
  function svgPicture(pl, o) {
    const b = o.bbox, L = layout(pl, b, o), tx = (L.pad - b[0]) * L.s, ty = (b[3] + L.pad) * L.s;
    const chips = L.chips.map(c => `<rect x="${f2(c.x)}" y="${f2(L.y)}" width="${f2(c.w)}" height="${f2(L.chipH)}" rx="${f2(L.chipH / 2)}" ry="${f2(L.chipH / 2)}" fill="${INK}"/>` +
      `<text x="${f2(c.x + c.w / 2)}" y="${f2(L.baseline)}" text-anchor="middle" font-family="Source Sans 3 Semibold, Source Sans 3, sans-serif" font-weight="600" font-size="${f2(L.fontPx)}" fill="${INK_TEXT}">${esc(c.text)}</text>`).join("");
    return { layout: L, svg: `<svg xmlns="http://www.w3.org/2000/svg" width="${L.W}" height="${L.H}" viewBox="0 0 ${L.W} ${L.H}"><rect width="100%" height="100%" fill="${o.bg || "#fff"}"/>` +
      `<g transform="translate(${f2(tx)} ${f2(ty)}) scale(${f2(L.s)} ${f2(-L.s)})">${o.inner || ""}</g>${chips}</svg>` };
  }

  return { plan, layout, canvasFor, paintTags, svgPicture, chipHtml, chipsText: (pl, size) => chipsText(pl, size), use: p => { injected = p; memoClear(); }, _const: { SHORT_BELOW_PX, FONT_FRAC, INK } };
  function memoClear() { /* the WeakMap cannot be cleared; a stand-in change is rare (tests) and plan() re-checks the signature per charm object */ }
});
