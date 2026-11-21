/*  charm-nest-back-file.js — ONE piece's engraving file (its BACK, or its FRONT), the parts the page and the cloud both use.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Cloud plan v2, build step 11 (Paul, 20 Nov 2026: no production work runs on any computer, the cloud does it all). An approved placement became its file only
 *  inside charm-nest-bridge.js (Engrave: writeBacksOnce, verifyBackFile, maskKey, restoreWritten), so only a page could write it. The pure parts are here now, and
 *  the page's verifyBackFile and maskKey call them; the cloud's back-file tasks (netlify/functions/_charmEngraveFiles.js) call the very same functions, so a file
 *  the cloud writes is the file a page writes: the same spec for charm-nest-pdf.js buildBackFile, the same re-read check, the same record for backPut.
 *
 *  Loads two ways, one source:  window.CharmNestBackFile (the sorter page, before charm-nest-bridge.js)
 *                               module.exports          (node: require("../../charm-nest-back-file.js"))
 *  Nothing here reaches the network, a page global, Firestore or Etsy: what it needs is handed in (P = CharmNestPDF, G = CharmNestGeom).
 *
 *    maskKey(mask)                         the identity of an engravable area (size, frame, a hash of its bits): a placement kept as numbers is rebuilt only on the same area
 *    backArtOf(view, charm, G)             the parts a person added to a BACK in the Charm Editor (charm.artBack), turned into the view's frame (view.M, then view.R)
 *    relGlyphs(fit, view)                  the fit's letter outlines about the view's centre: the frame buildBackFile writes the ENGRAVE layer in
 *    fileViewOf(face, backFileView)        what the file says of itself: a front "asSeenFromFront", a back the shop's setting (asSeenFromBack | frontCoordinates)
 *    backSpec(o)                           the spec of charm-nest-pdf.js buildBackFile for one piece (what writeBacksOnce hands it)
 *    verifyBackFile(bytes, o)              the written file read again: its cut geometry against the verified view, its words inside the eroded area (7.4)
 *    rebuild(place, o)                     an approved placement made again EXACTLY (restoreWritten's rebuild): the words where the person approved them, or engravingInvalid
 *    backRecord(o)                         the Charm_Pool_Back record of one piece's approval, as writeBacksOnce sends it to backPut
 *    backName(fileBase, face, poolId, at)  "<fileBase>_<face>_<poolId>_<approvedAt>", the two files' name
 *    previewOps(o) / svgOfOps(p) / previewSvg(o) / drawPreview(ctx, p)
 *                                          the approval's picture (renderBack(job, 500, { grid: false, hatch: false })): white, the cut lines in black, the added parts and
 *                                          the words in the back's blue (a front's words in #111); its paint, as SVG (the cloud renders it to PNG with resvg), or drawn on a 2D context
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestBackFile = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  const VERSION = 1, PT = 72 / 25.4, MM = 25.4 / 72;
  const BACK_INK_CSS = "#1a5fc4", FRONT_INK_CSS = "#111";   // (BLUEBACK: CharmNestBacks.INK, the words of a back on every picture; a front's words keep #111)
  const isFront = x => !!x && (x.face === "front" || (typeof x.key === "string" && x.key.endsWith("~front")));
  const faceWord = x => (typeof x === "string" ? (x === "front" ? "front" : "back") : isFront(x) ? "front" : "back");

  /** The identity of an engravable area (bridge restoreWritten): its size, its frame and a hash of its bits. */
  const maskKey = m => { let h = 2166136261; for (let i = 0; i < m.bits.length; i++) h = Math.imul(h ^ m.bits[i], 16777619) >>> 0; return [m.w, m.h, m.res, m.ox, m.oy, h].join(":"); };

  /** CHARM EDIT · BACK (27 Oct 2026): the parts a person added to this BACK (the charm's artBack, in its own points as seen from the front), turned into the view's frame by the
   *  same two steps the file turns them (view.M, then view.R). A front, or a back with nothing added: []. */
  function backArtOf(view, charm, G) {
    if (!view || view.face === "front") return [];
    const list = charm && Array.isArray(charm.artBack) && charm.artBack.length ? charm.artBack : null; if (!list) return [];
    return list.filter(s => s && s.kind === "path" && Array.isArray(s.subpaths) && (s.fill || s.stroke)).map(s => { try { return G.transformSeg(G.transformSeg(s, view.M), view.R); } catch (_) { return null; } }).filter(Boolean);
  }

  /** The fit's letter outlines about the view's centre (writeBacksOnce `rel`). */
  function relGlyphs(fit, view) {
    return ((fit && fit.glyphs) || []).map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== "Z") { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === "C" || c.type === "Q") { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === "C") { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
  }

  /** What the file says of itself (bridge fileViewOf): a front is always as seen from the front; a back is as the shop's setting asks. */
  const fileViewOf = (face, backFileView) => (face === "front" ? "asSeenFromFront" : backFileView || "asSeenFromBack");

  /** The two files' name (writeBacksOnce `name`, CharmNestExport.faceFiles). */
  const backName = (fileBase, face, poolId, at) => `${fileBase}_${faceWord(face)}_${poolId}_${at}`;

  /** The spec charm-nest-pdf.js buildBackFile takes for one piece (writeBacksOnce, prepareApproval).
   *  o = { charm, parsed, view, fit, face, backFileView, title, meta } (meta: { poolId, order, sku, copy, text, font, fontKey, lineGap, approvedBy, approvedAt }) */
  function backSpec(o) {
    const front = o.face === "front", charm = o.charm, view = o.view, fit = o.fit, m = o.meta || {};
    return { charm, mirrored: !!charm.mirrored, parsed: o.parsed, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 * PT, glyphs: relGlyphs(fit, view),
      ...(front ? { face: "front" } : {}), ...(!front && Array.isArray(charm.artBack) && charm.artBack.length ? { artBack: charm.artBack } : {}),
      view: fileViewOf(o.face, o.backFileView), title: o.title,
      meta: { poolId: m.poolId, ...(front ? { face: "front" } : {}), order: m.order, sku: m.sku, copy: m.copy, text: m.text, font: m.font || "Source Sans 3", fontKey: m.fontKey, weight: fit.weight, sizePt: fit.size, capMm: fit.capMm, lineGap: m.lineGap, angle: fit.angle, approvedBy: m.approvedBy, approvedAt: m.approvedAt, upAngle: view.upAngle, flipChecks: view.checks } };
  }

  /** Parse the written back file and compare its cut geometry with the verified view (mirrored back for the front-coordinates variant), then its words with the eroded area.
   *  o = { P, G, view, mask, fit, face, backFileView, added (backArtOf of the view: the parts added to a back, never read as words) } → { ok, why, pixelDiff, textPaths, ink?, artOnly? }
   *  (the page's Engrave.verifyBackFile, line for line: job.view → o.view, job.mask → o.mask, job.fit → o.fit, job.face → o.face, the setting → o.backFileView) */
  async function verifyBackFile(bytes, o) {
    const P = o.P, G = o.G, view = o.view;
    try {
      const parsed = await P.parseSource(bytes, "back.ai");
      // A back file has one explicit cut-reference layer. Do not mistake a
      // rectangular charm containing holes for an artboard around many charms.
      const references = parsed.segments.concat(parsed.nested).filter(m => m.kind === "path" && m.closed && (m.stroke || m.fill) && /CUT OUTLINE/.test(m.layer || ""));
      const larger = (a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a;
      const g = references.length ? null : await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 4 });
      if (!references.length && !g.charms.length) return { ok: false, why: "no cut outline found in the written file" };
      const c = references.length ? { outline: references.reduce(larger), members: references } : g.charms.reduce(larger);
      const cut = c.members.filter(m => m === c.outline || G.isCutLine(m));
      const bbW = cut.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const bbV = view.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const res = 6; const fw = G.makeFrame([0, 0, bbW[2] - bbW[0], bbW[3] - bbW[1]], res, (bbW[2] - bbW[0]) / 2, 1), fv = G.makeFrame([0, 0, bbV[2] - bbV[0], bbV[3] - bbV[1]], res, (bbV[2] - bbV[0]) / 2, 1);
      if (Math.abs(fw.w - fv.w) > 2 || Math.abs(fw.h - fv.h) > 2) return { ok: false, why: `written extent ${fw.w}×${fw.h} px differs from the verified back ${fv.w}×${fv.h} px` };
      // Reuse the preview's pixel grid after translating both files to their cut bounds.
      // A new bbox-centred grid shifts the clearance pixels on asymmetric rotated charms.
      const originalFrame = view.mask;
      const frame = originalFrame ? { w: originalFrame.w, h: originalFrame.h, res: originalFrame.res, ox: originalFrame.ox - bbV[0], oy: originalFrame.oy - bbV[1], cx: originalFrame.cx - bbV[0], cy: originalFrame.cy - bbV[1] } : fv;
      const front = o.face !== "front" && (o.backFileView || "asSeenFromBack") === "frontCoordinates";   // (the FRONT file is the charm as it is: nothing to turn back)
      const normalized = G.translate(-bbW[0], -bbW[1]);
      const writtenToView = front ? G.mul(normalized, G.mirrorX((bbW[2] - bbW[0]) / 2)) : normalized;
      const wc = cut.map(m => G.transformSeg(m, writtenToView)), vc = view.members.map(m => G.transformSeg(m, G.translate(-bbV[0], -bbV[1])));
      const outlineIndex = view.outline ? view.members.indexOf(view.outline) : view.members.reduce((best, m, i, all) => (m.bbox[2] - m.bbox[0]) * (m.bbox[3] - m.bbox[1]) > (all[best].bbox[2] - all[best].bbox[0]) * (all[best].bbox[3] - all[best].bbox[1]) ? i : best, 0);
      const W = G.materialMask(wc, wc[cut.indexOf(c.outline)], frame), V = G.materialMask(vc, vc[outlineIndex], frame);
      const diff = G.diffFraction(W, V);
      // CHARM EDIT · BACK (27 Oct 2026): the parts a person added to the back are written in the same ENGRAVE layer as the words. They were checked on the metal when they were saved
      // (charm-nest-art-edit.js) and the words keep off them (engraveMask avoid); the clearance off the cut edges checked here is the words' own, so an added part (known by its box, turned into
      // this frame as the view turns it: backArtOf) is not read as a word. A file with nothing added is read exactly as before.
      const addedBoxes = o.face !== "front" ? (o.added || []).filter(s => s.fill).map(s => G.transformSeg(s, G.translate(-bbV[0], -bbV[1])).bbox) : [];
      const addedPart = s => { if (!addedBoxes.length) return false; const b = G.transformSeg(s, writtenToView).bbox; return !!b && addedBoxes.some(a => a.every((v, i) => Math.abs(v - b[i]) < .1)); };
      const textPaths = parsed.segments.concat(parsed.nested).filter(s => s.kind === "path" && s.fill && !s.stroke && !/CUT OUTLINE/.test(s.layer || "") && !addedPart(s)), textOk = textPaths.length >= 1;
      // Preserve the written Béziers so verifyInk uses the same curved outlines
      // and sampling as the preview. Flattening them first changed valid fits.
      const textCmds = textPaths.flatMap(s => G.transformSeg(s, writtenToView).subpaths.flatMap(sub => sub.map(([op, p, p1, p2]) => {
        if (op === "m" || op === "l") return { type: op === "m" ? "M" : "L", x: p[0], y: p[1] };
        if (op === "c") return { type: "C", x1: p[0], y1: p[1], x2: p1[0], y2: p1[1], x: p2[0], y: p2[1] };
        if (op === "h") return { type: "Z" };
        throw new Error("Unsupported engraving path in the back file");
      })));
      // EDITOR WORDS (27 Oct 2026): a face approved with no words (its engraving is the charm editor's parts) is right when its file carries no words at all
      if (o.fit && o.fit.artOnly) return { ok: diff <= .01 && !textPaths.length, why: diff > .01 ? `cut geometry differs by ${(diff * 100).toFixed(2)}%` : textPaths.length ? "words in a file that carries none" : null, pixelDiff: diff, textPaths: false, artOnly: true };
      const ink = textOk && G.verifyInk(textCmds, G.engraveMask({ mask: W }, { marginMm: (o.mask && o.mask.marginMm) || 0 }));
      return { ok: diff <= .01 && textOk && ink.ok, why: diff > .01 ? `cut geometry differs by ${(diff * 100).toFixed(2)}%` : !textOk ? "no engraving paths in the file" : !ink.ok ? "engraving intersects a cut-out or cut-edge clearance" : null, pixelDiff: diff, textPaths: textOk, ink };
    } catch (e) { return { ok: false, why: e.message }; }
  }

  /** The middle of an engravable area (the bridge's maskCentroid): where a face with no words (the Charm Editor's parts alone) is said to sit. */
  function maskCentroid(m) {
    let sx = 0, sy = 0, n = 0;
    for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) { sx += x; sy += y; n++; }
    if (!n) return [m.cx, m.cy];
    return [m.ox + (sx / n + 0.5) / m.res, m.oy + (sy / n + 0.5) / m.res];
  }

  /** An approved placement made again EXACTLY (restoreWritten's rebuild, for a placement that is kept as numbers): the words, size, turn and place the person approved, on the charm as it is
   *  now. Nothing is moved or shrunk to make it fit (that is a new placement, which a person approves again): a charm whose face changed, or words that no longer sit on the metal, throw
   *  engravingInvalid with the words a person reads.
   *  place = { centre, angle, size, lines, weight, lineGap?, fittedMax?, upAngle, cx?, cy?, artOnly? }
   *  o = { G, charm, face, font(weight) → the font to set it in, marginMm, lineGap (when the place has none), minCapMm, minStrokeMm, minGapMm, maskKey? (the area it was approved on), keepArtClear?,
 *        tolPt? (how far the view's centre may be from the one the words were placed on: 1e-6 pt unless said) }
   *  → { view, mask, fit, verify: { geometry } } */
  function rebuild(place, o) {
    const G = o.G, charm = o.charm, front = o.face === "front", word = front ? "front" : "back";
    const again = why => Object.assign(new Error(why), { engravingInvalid: true });
    if (!place || (!place.artOnly && !(place.size > 0 && Array.isArray(place.centre) && Array.isArray(place.lines) && place.lines.length))) throw again("This engraving's placement is not kept — fit the words again");
    if (!charm || !charm.outline) throw Object.assign(new Error("The charm's drawing is not here yet"), { engravingPending: true });
    let view;
    try { view = G.backView(charm, Object.assign({ res: 6, holeOnly: true }, Number.isFinite(+place.upAngle) && place.upAngle !== null && place.upAngle !== "" ? { upAngle: +place.upAngle } : {}, front ? { face: "front" } : {})); }
    catch (e) { throw again(`This charm's ${word} could not be read again (${e.message}) — fit the words again`); }
    const tol = +o.tolPt > 0 ? +o.tolPt : 1e-6;   // (the cloud allows the shop's 0.06 mm between its reading of a drawing and Chrome's; a page reads its own)
    if (Number.isFinite(+place.cx) && Number.isFinite(+place.cy) && place.cx !== null && (Math.abs(view.cx - +place.cx) > tol || Math.abs(view.cy - +place.cy) > tol)) throw again(`This charm's ${word} changed since the words were placed — fit the words again`);
    const mask = G.engraveMask(view, Object.assign({ marginMm: o.marginMm, keepOut: (front ? charm.frontKeepOut : charm.backKeepOut) || [] }, !front && Array.isArray(charm.artBack) && charm.artBack.length ? { avoid: charm.artBack } : {}, o.keepArtClear ? { keepArtClear: true } : {}));
    if (o.maskKey && maskKey(mask) !== o.maskKey) throw again(`This charm's ${word} changed since the words were approved — fit the words again`);
    if (place.artOnly) {
      const fit = { ok: true, artOnly: true, lines: [], glyphs: [], cmds: [], centre: maskCentroid(mask), angle: 0, size: 0, fittedMax: 0, capMm: 0, weight: "Regular", small: false, thin: false, metrics: null, rect: null };
      return { view, mask, fit, verify: { geometry: { ok: true, outside: 0, total: 0, artOnly: true }, at: Date.now() } };
    }
    const weight = place.weight === "Semibold" ? "Semibold" : "Regular", font = o.font(weight), lineGap = place.lineGap != null ? +place.lineGap : o.lineGap != null ? +o.lineGap : 0.216, angle = +place.angle || 0;
    if (!font) throw Object.assign(new Error("The engraving font is not loaded"), { engravingPending: true });
    const layout = G.layoutLines(place.lines, font, +place.size, lineGap, angle, place.centre.slice()), geometry = G.verifyInk(layout.cmds, mask);
    if (!geometry.ok) throw again(`The approved words no longer fit this charm's ${word} — fit the words again`);
    const metrics = G.strokeMetrics(layout.cmds, 24), capMm = +place.size * G.capPerEm(font) * MM;
    const fit = { ok: true, lines: place.lines.slice(), size: +place.size, capMm, weight, angle, centre: place.centre.slice(), fittedMax: +place.fittedMax > 0 ? +place.fittedMax : +place.size, lineGap,
      layout, glyphs: layout.glyphs, cmds: layout.cmds, metrics, small: capMm < (+o.minCapMm || 1.6),
      thin: !!((+o.minStrokeMm > 0 && metrics.strokeMm < +o.minStrokeMm) || (+o.minGapMm > 0 && metrics.gapMm > 0 && metrics.gapMm < +o.minGapMm)), rect: place.rect || null };
    return { view, mask, fit, verify: { geometry, at: Date.now() } };
  }

  /** The record of one piece's approval (Charm_Pool_Back, sent to backPut), as writeBacksOnce makes it.
   *  o = { job (its fields: text, lines, lineInput, lineMode, fontName, fontKey, fontAsked, nudged, decision, source, quote, confidence, claude, slot, groupKey, face), place-free fit, view,
   *        verify, verified (verifyBackFile's answer), built (buildBackFile's answer), sheet { sheetId, setId, runId }, poolId, order, transactionId, sku, copy, side, mirror, lineGap,
   *        approvedBy, approvedAt, seals, backFileView, outputs { ai: { path, url }, png: { path, url } }, name, previewPt { w, h } } */
  function backRecord(o) {
    const job = o.job || {}, fit = o.fit, view = o.view, front = isFront(job) || o.face === "front";
    const piece = job.slot ? { slot: job.slot, ...(o.side === "L" || o.side === "R" ? { side: o.side } : {}), ...(o.mirror ? { mirror: true } : {}), groupKey: job.groupKey || "", ...(front ? { face: "front" } : {}) } : front ? { face: "front" } : {};
    return { lineGap: o.lineGap, lineMode: job.lineMode || "auto", lineInput: job.lineInput || job.lines, materialVersion: 2, ...(fit.artOnly ? { artOnly: true } : {}), upAngle: view.upAngle, poolId: o.poolId,
      sheetId: o.sheet.sheetId, setId: o.sheet.setId || null, runId: o.sheet.runId || null, order: o.order, transactionId: o.transactionId, sku: o.sku, copy: o.copy, ...piece,
      text: job.text, lines: job.lines, font: job.fontName || "Source Sans 3", fontKey: o.fontKey, ...(job.fontAsked ? { fontAsked: job.fontAsked } : {}), weight: fit.weight,
      sizePt: +(+fit.size).toFixed(3), capMm: +(+fit.capMm).toFixed(3), box: fit.rect ? [fit.rect.x0, fit.rect.y0, fit.rect.x1, fit.rect.y1].map(v => +(+v).toFixed(2)) : null, centre: fit.centre.map(v => +(+v).toFixed(2)), angle: fit.angle,
      small: !!fit.small, thin: !!fit.thin, metrics: fit.metrics, flipChecks: view.checks, flipDetail: view.detail, verified: { geometry: o.verify.geometry, file: o.verified }, review: job.claude || null,
      approvedBy: o.approvedBy, approvedAt: o.approvedAt, engravingSeals: o.seals || [], nudged: !!job.nudged, decision: job.decision || null, source: job.source == null ? null : job.source, sourceQuote: job.quote == null ? null : job.quote,
      confidence: job.confidence == null ? null : job.confidence, view: fileViewOf(front ? "front" : "back", o.backFileView), reference: o.built.reference, outputs: o.outputs, name: o.name,
      previewWPt: o.previewPt.w, previewHPt: o.previewPt.h, pageWPt: o.built.wPt, pageHPt: o.built.hPt };
  }

  /* ── the approval's picture (renderBack(job, 500, { grid: false, hatch: false })): its paint in picture pixels (previewOps), as SVG (previewSvg), or drawn on any 2D context (drawPreview) ── */
  /** A member's (or an added part's) subpaths in picture pixels: [[["M", x, y] | ["L", x, y] | ["C", x1, y1, x2, y2, x, y] | ["Z"]]] */
  function segSubs(seg, tx) {
    const out = [];
    for (const sub of seg.subpaths || []) {
      const one = [];
      for (const s of sub) {
        if (s[0] === "m") { const p = tx(s[1][0], s[1][1]); one.push(["M", p[0], p[1]]); }
        else if (s[0] === "l") { const p = tx(s[1][0], s[1][1]); one.push(["L", p[0], p[1]]); }
        else if (s[0] === "c") { const a = tx(s[1][0], s[1][1]), b = tx(s[2][0], s[2][1]), c = tx(s[3][0], s[3][1]); one.push(["C", a[0], a[1], b[0], b[1], c[0], c[1]]); }
        else if (s[0] === "h") one.push(["Z"]);
      }
      if (one.length) out.push(one);
    }
    return out;
  }
  /** A letter's outline in picture pixels (a quadratic piece raised to a cubic, as charm-nest-pdf.js writes it). */
  function glyphSubs(g, tx) {
    const out = []; let one = [], cur = null, first = null;
    for (const c of g.cmds || []) {
      if (c.type === "M") { if (one.length) out.push(one); one = []; const p = tx(c.x, c.y); one.push(["M", p[0], p[1]]); cur = [c.x, c.y]; first = cur; }
      else if (c.type === "L") { const p = tx(c.x, c.y); one.push(["L", p[0], p[1]]); cur = [c.x, c.y]; }
      else if (c.type === "C") { const a = tx(c.x1, c.y1), b = tx(c.x2, c.y2), p = tx(c.x, c.y); one.push(["C", a[0], a[1], b[0], b[1], p[0], p[1]]); cur = [c.x, c.y]; }
      else if (c.type === "Q" && cur) { const c1 = [cur[0] + 2 / 3 * (c.x1 - cur[0]), cur[1] + 2 / 3 * (c.y1 - cur[1])], c2 = [c.x + 2 / 3 * (c.x1 - c.x), c.y + 2 / 3 * (c.y1 - c.y)], a = tx(c1[0], c1[1]), b = tx(c2[0], c2[1]), p = tx(c.x, c.y); one.push(["C", a[0], a[1], b[0], b[1], p[0], p[1]]); cur = [c.x, c.y]; }
      else if (c.type === "Z") { one.push(["Z"]); cur = first; }
    }
    if (one.length) out.push(one);
    return out;
  }
  const rgb = c => `rgb(${c.slice(0, 3).map(v => Math.round(Math.max(0, Math.min(1, +v || 0)) * 255)).join(",")})`;
  /** The picture's paint, in order: the parts added to a back (the words' blue; a traced image's grey in its grey; a coloured pen a person chose in its colour), the cut lines in black, the words.
   *  o = { view, fit, face, added (backArtOf), P (for backArtPaint), px (the long side, 500), cutLinePt (G.CUT_LINE_PT) }
   *  → { width, height, sizePt: { w, h } (the frame in points: the record's previewWPt / previewHPt), ops: [{ fill | stroke (css colour), rule, width, cap, subs }] } */
  function previewOps(o) {
    const view = o.view, fit = o.fit, px = +o.px || 500, front = o.face === "front";
    const bb = view.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
    const pad = 3 * PT, w = bb[2] - bb[0] + 2 * pad, h = bb[3] - bb[1] + 2 * pad, k = px / Math.max(w, h);
    const W = Math.max(1, Math.round(w * k)), H = Math.max(1, Math.round(h * k));
    const tx = (x, y) => [(x - bb[0] + pad) * k, (bb[3] + pad - y) * k];
    const ink = front ? FRONT_INK_CSS : BACK_INK_CSS, ops = [];
    for (const s of front ? [] : o.added || []) {
      const paint = o.P && o.P.backArtPaint ? o.P.backArtPaint(s) : {}, subs = segSubs(s, tx); if (!subs.length) continue;
      if (s.fill) ops.push({ fill: s.artGrey && Array.isArray(s.fillRGB) ? rgb(s.fillRGB) : ink, rule: String(s.paintOp || "").endsWith("*") ? "evenodd" : "nonzero", subs });
      if (s.stroke) ops.push({ stroke: paint.stroke ? rgb(paint.stroke) : ink, width: Math.max(0.6, (s.lwPt || 0.25) * k), cap: "round", subs });
    }
    const lw = Math.max(1, (+o.cutLinePt || 0.5) * k);
    for (const m of view.members) { const subs = segSubs(m, tx); if (subs.length) ops.push({ stroke: "#000", width: lw, subs }); }
    const glyphs = (fit && fit.glyphs) || [];
    for (const g of glyphs) { const subs = glyphSubs(g, tx); if (subs.length) ops.push({ fill: ink, rule: "nonzero", subs }); }
    return { width: W, height: H, sizePt: { w, h }, ops };
  }
  const f2 = v => (Math.round(v * 100) / 100).toString();
  const dOf = subs => subs.map(one => one.map(s => s[0] + (s.length > 1 ? s.slice(1).map(f2).join(" ") : "")).join("")).join("");
  /** previewOps' paint as an SVG document (white ground). */
  function svgOfOps(p) {
    const parts = [`<rect x="0" y="0" width="${p.width}" height="${p.height}" fill="#fff"/>`];
    for (const op of p.ops) {
      const d = dOf(op.subs); if (!d) continue;
      if (op.fill) parts.push(`<path d="${d}" fill="${op.fill}" fill-rule="${op.rule || "nonzero"}"/>`);
      else parts.push(`<path d="${d}" fill="none" stroke="${op.stroke}" stroke-width="${f2(op.width)}"${op.cap ? ` stroke-linejoin="round" stroke-linecap="${op.cap}"` : ""}/>`);
    }
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${p.width}" height="${p.height}" viewBox="0 0 ${p.width} ${p.height}">${parts.join("")}</svg>`;
  }
  /** The picture as an SVG document. → { svg, width, height, sizePt } */
  function previewSvg(o) { const p = previewOps(o); return { svg: svgOfOps(p), width: p.width, height: p.height, sizePt: p.sizePt }; }
  /** The picture drawn on a 2D context of previewOps' size (a canvas, or the node canvas of netlify/functions/_charmNestCanvas.js). */
  function drawPreview(ctx, p) {
    ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, p.width, p.height);
    for (const op of p.ops) {
      ctx.beginPath();
      for (const one of op.subs) for (const s of one) { if (s[0] === "M") ctx.moveTo(s[1], s[2]); else if (s[0] === "L") ctx.lineTo(s[1], s[2]); else if (s[0] === "C") ctx.bezierCurveTo(s[1], s[2], s[3], s[4], s[5], s[6]); else if (s[0] === "Z") ctx.closePath(); }
      if (op.fill) { ctx.fillStyle = op.fill; ctx.fill(op.rule || "nonzero"); }
      else { ctx.strokeStyle = op.stroke; ctx.lineWidth = op.width; if (op.cap) { ctx.lineCap = op.cap; ctx.lineJoin = "round"; } else { ctx.lineCap = "butt"; ctx.lineJoin = "miter"; } ctx.stroke(); }
    }
    return ctx;
  }

  return { VERSION, isFront, faceWord, maskKey, backArtOf, relGlyphs, fileViewOf, backName, backSpec, verifyBackFile, maskCentroid, rebuild, backRecord, previewOps, svgOfOps, previewSvg, drawPreview };
});
