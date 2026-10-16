/*  charm-nest-monogram-fit.js — the piece-size policy of the MONOGRAM engine (MONOX E4, Paul, 16 Oct 2026).
 *  The engine draws a monogram at the size of the font file (about 14.8 mm tall) and widens every letter, so a finished piece is wider than it is tall (18 to 24 mm after the stronger widening). A shop sells a charm
 *  in a few sizes (SIZEFIT: "12MM", "14MM" ... charm-nest-orders.js sizeMmOf), so the whole finished piece must be able to be brought to a size WITHOUT touching the layout the engine judged:
 *
 *    opts.fit = { maxWidthMm, maxHeightMm, grow?, minScale? }     read by CharmNestMonogram.build / autoFit (a hook of three lines: the engine builds as always, then this module scales the finished piece)
 *       scale = min(maxWidthMm / wMm, maxHeightMm / hMm), never above 1 (the piece only shrinks to fit; grow: true lets it also fill the envelope); one scale for both axes (proportions and the judged layout stay exactly as they were)
 *    fitScale(wMm, hMm, fit)            → { scale, limiting: 'width' | 'height' | 'none', ... }   the number alone
 *    applyFit(result, fit)              → the same result, scaled in place: rings, bboxPt, wMm, hMm, placed[], the contact marks of the card; report.fit says what was done (see below)
 *    fitFromSize(token)                 → { maxWidthMm } | null     a SIZEFIT token ("18MM", "1.8CM") as the envelope of a monogram (the width is the size: a monogram is wider than it is tall)
 *    ENVELOPES                          → [16, 18, 20]   the maximum widths offered to Paul (mm)
 *
 *  report.fit = { applied, scale, limiting, maxWidthMm, maxHeightMm, designMm: [w, h], sizeMm: [w, h], strokeMm: { design, final }, warnings[] }.  report.metrics (violations, ratios, visible widths, crowding) are the
 *  layout as judged at the design size and are NOT recomputed: a uniform scale changes no ratio and no proportion; the limits in millimetres (visible width 3.8 mm, crowding 3.2 mm) were calibrated at the design size.
 *  Pure, no page, no network. Works as window.CharmNestMonogramFit and through require().  */
(function (root) {
  'use strict';
  const ENVELOPES = [16, 18, 20];
  const POLICY = { WARN_SCALE: 0.78, MIN_SCALE: 0.5, MIN_MEAN_STROKE_MM: 0.4 };
  const finite = v => typeof v === 'number' && Number.isFinite(v);
  const r3 = v => Math.round(v * 1000) / 1000;
  const r4 = v => Math.round(v * 1e4) / 1e4;

  /** The envelope as given by the caller, cleaned: { maxWidthMm, maxHeightMm, grow, minScale } or null when there is none. */
  function cleanFit(fit) {
    if (!fit || typeof fit !== 'object') return null;
    const pos = v => (finite(v) && v > 0.5 && v < 500 ? v : null), w = pos(fit.maxWidthMm), h = pos(fit.maxHeightMm);
    if (w == null && h == null) return null;
    return { maxWidthMm: w, maxHeightMm: h, grow: fit.grow === true, minScale: finite(fit.minScale) ? Math.min(1, Math.max(0.1, fit.minScale)) : POLICY.MIN_SCALE };
  }

  /** The scale that brings a piece of wMm x hMm into the envelope: one number for both axes. */
  function fitScale(wMm, hMm, fit) {
    const f = cleanFit(fit);
    if (!f || !(wMm > 0) || !(hMm > 0)) return { scale: 1, limiting: 'none', fit: f };
    const sw = f.maxWidthMm == null ? Infinity : f.maxWidthMm / wMm, sh = f.maxHeightMm == null ? Infinity : f.maxHeightMm / hMm;
    let s = Math.min(sw, sh), limiting = sw <= sh ? 'width' : 'height';
    if (!f.grow && s >= 1) { s = 1; limiting = 'none'; }
    return { scale: s, limiting, fit: f };
  }

  /** A SIZEFIT token ("18MM", "1.8CM", as charm-nest-orders.js sizeMmOf reads it) as the envelope of a monogram: the size is its maximum width; null for anything else. */
  function fitFromSize(token, extra) {
    const m = /^(\d{1,3}(?:\.\d{1,2})?)\s*(MM|CM)$/i.exec(String(token == null ? '' : token).trim()); if (!m) return null;
    const mm = +m[1] * (m[2].toUpperCase() === 'CM' ? 10 : 1);
    return mm >= 5 && mm <= 80 ? Object.assign({ maxWidthMm: mm }, extra && typeof extra === 'object' ? extra : {}) : null;
  }

  /** Scale a finished build() result to the envelope, in place (the quick-look closure the engine keeps on the result stays valid: it works at the design size). Returns the result. */
  function applyFit(result, fit) {
    if (!result || !result.ok || !Array.isArray(result.rings) || !Array.isArray(result.bboxPt)) return result;
    const f = cleanFit(fit); if (!f) return result;
    const rep = result.report || (result.report = {}), w0 = result.wMm, h0 = result.hMm, sc = fitScale(w0, h0, f);
    const strokes = ['L', 'M', 'R'].map(k => rep.prep && rep.prep[k] && finite(rep.prep[k].strokeAfterMm) ? rep.prep[k].strokeAfterMm : null).filter(v => v != null);
    const designStroke = strokes.length ? Math.min(...strokes) : null;
    const info = { applied: false, scale: 1, limiting: sc.limiting, maxWidthMm: f.maxWidthMm, maxHeightMm: f.maxHeightMm, grow: f.grow, designMm: [r3(w0), r3(h0)], sizeMm: [r3(w0), r3(h0)], strokeMm: { design: designStroke, final: designStroke }, warnings: [] };
    rep.fit = info;
    const k = sc.scale;
    if (!(k > 0) || Math.abs(k - 1) < 1e-6) return result;
    for (const r of result.rings) r.pts = r.pts.map(p => [r4(p[0] * k), r4(p[1] * k)]);
    result.bboxPt = result.bboxPt.map(v => r4(v * k)); result.wMm = r4(w0 * k); result.hMm = r4(h0 * k);
    for (const p of result.placed || []) { p.bboxPt = p.bboxPt.map(v => r4(v * k)); p.scale = r4(p.scale * k); p.scaleX = r4(p.scaleX * k); if (p.trimMm) p.trimMm = r3(p.trimMm * k); }
    // what the card paints over the piece (the contact marks) and what is a length or an area of the piece
    const mt = rep.metrics;
    if (mt && Array.isArray(mt.contacts)) for (const c of mt.contacts) { for (const key of ['xPt', 'yPt', 'yMm', 'wMm', 'lenMm']) if (finite(c[key])) c[key] = r4(c[key] * k); }
    if (rep.joinMm && typeof rep.joinMm === 'object') for (const key of Object.keys(rep.joinMm)) if (finite(rep.joinMm[key])) rep.joinMm[key] = r3(rep.joinMm[key] * k);
    if (rep.contacts && typeof rep.contacts === 'object') for (const key of ['leftMiddle', 'middleRight']) if (Array.isArray(rep.contacts[key])) rep.contacts[key] = rep.contacts[key].map(a => (finite(a) ? r3(a * k * k) : a));
    if (Array.isArray(rep.altered)) for (const a of rep.altered) if (finite(a.mm)) a.mm = r3(a.mm * k);
    info.applied = true; info.scale = r4(k); info.sizeMm = [r3(result.wMm), r3(result.hMm)];
    if (designStroke != null) info.strokeMm.final = r3(designStroke * k);
    if (k < f.minScale - 1e-9) info.warnings.push(`the piece is scaled to ${Math.round(k * 100)} % to fit ${f.maxWidthMm != null ? f.maxWidthMm + ' mm wide' : f.maxHeightMm + ' mm tall'}: that is more than the ${Math.round((1 - f.minScale) * 100)} % a monogram can shrink and stay legible`);
    else if (k < POLICY.WARN_SCALE) info.warnings.push(`the piece is scaled to ${Math.round(k * 100)} % to fit ${f.maxWidthMm != null ? f.maxWidthMm + ' mm wide' : f.maxHeightMm + ' mm tall'} (${r3(result.wMm)} x ${r3(result.hMm)} mm)`);
    if (info.strokeMm.final != null && info.strokeMm.final < POLICY.MIN_MEAN_STROKE_MM) info.warnings.push(`the thinnest letter's mean line is ${info.strokeMm.final} mm at this size (under ${POLICY.MIN_MEAN_STROKE_MM} mm it may not cut cleanly)`);
    if (Array.isArray(rep.notes)) rep.notes.push(`scaled to ${Math.round(k * 100)} % to fit ${f.maxWidthMm != null ? f.maxWidthMm + ' mm wide' : f.maxHeightMm + ' mm tall'} (drawn ${r3(w0)} x ${r3(h0)} mm, now ${r3(result.wMm)} x ${r3(result.hMm)} mm)`);
    if (Array.isArray(rep.warnings)) for (const w of info.warnings) rep.warnings.push(w);
    return result;
  }

  const api = { applyFit, fitScale, fitFromSize, cleanFit, ENVELOPES, POLICY };
  root.CharmNestMonogramFit = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
