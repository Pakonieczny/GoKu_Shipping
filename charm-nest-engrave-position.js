/* WHERE THE WORDS SIT on a charm (Paul, 12 Oct 2026: "update it with the text font options and positional update of the text on the charm ... everything has to be linked to its engraving,
 * whether it be front or back on the same charm or multiple charms in the same order"). Plan: plans/engrave-ui-1012 (CONTRACT.md F, ENGPOS).
 *
 * There is NO new data. A position IS the placement of the face's own job: job.fit = { centre (pt, in the job's view), angle, size, glyphs, cmds }. That is what the fit makes, what a
 * drag / arrow key / size change edits (charm-nest-bridge.js nudge / resize / refit), what the session checkpoint keeps (the whole job, `nudged` included) and what Approve cuts into the
 * laser file (prepareApproval maps fit.glyphs). So moving the words here moves what the laser cuts, and nothing else: nothing is saved to a record until someone approves, and a face
 * that nobody moves is exactly what it was. Every call takes the FACE JOB (Engrave.jobOf(key); key = Faces.jobKey(lineKey, slot, face)) and touches that one job, never its other face,
 * its other ear or the next disc. Every call is synchronous.
 *
 *   of(job)                -> { x, y, size, angle, auto } | null      where the words are, as a fraction of the drawing box, for a picture of the charm (null: no placement yet)
 *   nudge(job, dxMm, dyMm) -> { ok, clamped, moved: { dx, dy }, why }  move by millimetres (dx > 0 right, dy > 0 up, in the placement picture of the Engrave tab); stops at the safe area
 *   centre(job)            -> { ok, clamped, moved, why }              to the middle of the metal the words may use
 *   size(job, factor)      -> { ok, atLimit, why }                     1.05 = 5% bigger, 0.95 = 5% smaller, from the size on the charm now
 *   snapshot(job)          -> snap | null     restore(job, snap) -> boolean     the placement as it was, and back to it (Cancel); ROTBAR: it includes the piece's orientation (view, mask, job.turnUp), so a turn made with the work area's degree box is undone too
 *   mirrorDx(job, dxMm)    -> dx for a Left ear / any other piece, -dx for the Right ear (the pair's two views are mirror images): for a caller that moves both ears as mirrors
 *   canMove(job)           -> boolean     (the placement is in front of a person: state "review" with a fit and a mask)
 *
 * The safe area is the job's own mask (the cut outline eroded by the margin, cut-outs and keep-out layers taken out): a move that would put a letter outside it goes as far as it can
 * (clamped) or not at all, and never makes the words smaller to squeeze in. Loads in the page (window.CharmNestEngravePosition) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngravePosition = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const PT = 72 / 25.4;
  const here = typeof globalThis !== 'undefined' ? globalThis : root;
  const geom = () => here.CharmNestGeom || (typeof require === 'function' ? require('./charm-nest-geom.js') : null);
  const engrave = () => here.Engrave || null;
  const session = () => (typeof here.Session !== 'undefined' ? here.Session : null);
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const r3 = v => Math.round(v * 1000) / 1000;
  const r5 = v => Math.round(v * 100000) / 100000;
  const wrap180 = d => { d = ((d % 360) + 360) % 360; return d > 180 ? d - 360 : d; };
  const why = (text, extra) => Object.assign({ ok: false, clamped: false, moved: { dx: 0, dy: 0 }, why: text }, extra || {});

  /** The placement is in front of a person to change: under review, or approved / saved (its first change reopens it for Approve); not still being worked out. */
  function canMove(job) {
    return !!(job && (job.state === 'review' || job.state === 'approved' || job.state === 'written') && !job.stamping && !job.backSaving && !job.approvalPreparing && job.fit && Array.isArray(job.fit.centre) && job.mask && job.mask.bits);   // (NODECIDED: an approved piece can be moved: the bridge's own nudge / resize reopen it for Approve on that first change)
  }

  /** The drawing box of a job's view (the cut outline's bounding box, in the charm's own coordinates), as a picture of the charm shows it. A back picture is the drawing turned over, so its box is mirrored
   *  about the charm's centre line. */
  function drawnBox(view) {
    let bb = null;
    for (const m of view.cutMembers || []) {
      const b = m && m.bbox; if (!b || !b.every(fin)) continue;
      bb = bb ? [Math.min(bb[0], b[0]), Math.min(bb[1], b[1]), Math.max(bb[2], b[2]), Math.max(bb[3], b[3])] : b.slice();
    }
    if (!bb || !(bb[2] > bb[0]) || !(bb[3] > bb[1])) return null;
    return view.face === 'front' ? bb : [2 * view.cx - bb[2], bb[1], 2 * view.cx - bb[0], bb[3]];
  }

  /** Where the words are, for a picture of the charm. x, y are 0..1 of the drawing box (y counts down from the top, like a screen); `size` is the font's em height over the box height; `angle` is CSS degrees
   *  (clockwise). The picture is the charm as drawn (a back: turned over, like backView), NOT turned hoop-up: this undoes the view's turn, so the words land on the drawing where they will be cut.
   *  `auto` is true while nobody has moved the words. null while the job has no placement. */
  function of(job) {
    const f = job && job.fit, v = job && job.view;
    if (!f || !Array.isArray(f.centre) || !f.centre.every(fin) || !v || !fin(v.cx) || !fin(v.cy)) return null;
    const box = drawnBox(v); if (!box) return null;
    const a = (fin(v.angleDeg) ? v.angleDeg : 0) * Math.PI / 180, c = Math.cos(a), s = Math.sin(a);
    const dx = f.centre[0] - v.cx, dy = f.centre[1] - v.cy;
    const qx = v.cx + dx * c + dy * s, qy = v.cy - dx * s + dy * c;       // the view turned the picture by +angleDeg about the charm's centre; this turns it back
    const w = box[2] - box[0], h = box[3] - box[1];
    return { x: r5((qx - box[0]) / w), y: r5(1 - (qy - box[1]) / h), size: r5((fin(f.size) ? f.size : 0) / h), angle: r3(wrap180((fin(v.angleDeg) ? v.angleDeg : 0) - (f.angle || 0))), auto: !job.nudged };
  }

  /** The middle of the metal the words may use: the centre of gravity of the solid pixels (the bridge's maskCentroid, kept the same on purpose). */
  function maskCentroid(m) {
    let sx = 0, sy = 0, n = 0;
    for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) { sx += x; sy += y; n++; }
    if (!n) return [m.cx, m.cy];
    return [m.ox + (sx / n + 0.5) / m.res, m.oy + (sy / n + 0.5) / m.res];
  }

  const shifted = (cmds, dx, dy) => cmds.map(k => { if (k.type === 'Z') return { type: 'Z' }; const o = { type: k.type, x: k.x + dx, y: k.y + dy }; if (k.x1 != null) { o.x1 = k.x1 + dx; o.y1 = k.y1 + dy; } if (k.x2 != null) { o.x2 = k.x2 + dx; o.y2 = k.y2 + dy; } return o; });

  /** Move by (dxMm, dyMm) as far as the words stay inside the safe area. The words keep their size: the move is a pure shift of the letters that are already there, checked against the mask first, so
   *  the bridge's own nudge (which would shrink the words to squeeze them in) only ever sees a move that needs no shrinking. */
  function nudge(job, dxMm, dyMm) {
    const E = engrave(), G = geom();
    if (!E || !G || !job) return why('not ready');
    if (!canMove(job)) return why(job.state === 'review' ? 'no placement yet' : 'locked');
    if (!fin(dxMm) || !fin(dyMm)) return why('not a number');
    if (dxMm === 0 && dyMm === 0) return { ok: true, clamped: false, moved: { dx: 0, dy: 0 } };
    const before = job.fit, c0 = before.centre.slice(), size0 = before.size, snap = snapshot(job);
    const fits = t => G.verifyInk(shifted(before.cmds, dxMm * PT * t, dyMm * PT * t), job.mask).ok;
    let t = 1;
    if (!fits(1)) {
      let lo = 0, hi = 1;
      const steps = Math.min(24, Math.max(4, Math.ceil(Math.log2(Math.hypot(dxMm, dyMm) / 0.005))));   // (the edge to within 0.005 mm, however far was asked)
      for (let i = 0; i < steps; i++) { const mid = (lo + hi) / 2; if (fits(mid)) lo = mid; else hi = mid; }
      t = lo;
      if (t * Math.hypot(dxMm, dyMm) < 0.02) return why('no room', { clamped: true });
    }
    E.nudge(job, dxMm * t, dyMm * t);
    const after = job.fit;
    if (!after || after === before || !after.centre) return why('no room', { clamped: t < 1 });
    // (the engine fits again at the new centre; if it ever came out smaller than the words were, the move is taken back rather than leave smaller words on the charm)
    if (fin(size0) && after.size < size0 - 0.005) { restoreFit(job, snap.fit, snap.centre, snap); return why('no room', { clamped: true }); }
    const moved = { dx: r3((after.centre[0] - c0[0]) / PT), dy: r3((after.centre[1] - c0[1]) / PT) };
    return { ok: moved.dx !== 0 || moved.dy !== 0, clamped: t < 1, moved };
  }

  /** The words to the middle of the metal. */
  function centre(job) {
    if (!canMove(job)) return why(job && job.state === 'review' ? 'no placement yet' : 'locked');
    const target = maskCentroid(job.mask), c = job.fit.centre;
    const r = nudge(job, (target[0] - c[0]) / PT, (target[1] - c[1]) / PT);
    return r;
  }

  /** Bigger or smaller by a factor of the size on the charm now; the engine limits it to what fits (atLimit). */
  function size(job, factor) {
    const E = engrave();
    if (!E || !job) return why('not ready', { atLimit: false });
    if (!canMove(job)) return why(job.state === 'review' ? 'no placement yet' : 'locked', { atLimit: false });
    if (!fin(factor) || !(factor > 0)) return why('not a number', { atLimit: false });
    const before = job.fit, size0 = before.size;
    E.resize(job, size0 * Math.min(2, Math.max(0.5, factor)));
    const after = job.fit, changed = !!after && after !== before && Math.abs(after.size - size0) > 0.005;   // (the engine's own tolerance: a creep under 0.005 pt is the metal saying stop)
    return { ok: changed, atLimit: !!(after && after.atLimit) || (factor > 1 && !changed), moved: { dx: 0, dy: 0 }, why: changed ? '' : 'no room' };
  }

  /** A plain copy of the placement: enough to put it back exactly. */
  function snapshot(job) {
    if (!job || !job.fit || !Array.isArray(job.fit.centre)) return null;
    const f = job.fit;
    // ROTBAR: the piece's own orientation is part of the placement (the work area's turn box): the view, and the up angles that make it, are kept so Cancel can turn the whole piece back
    return { v: 1, key: job.key, mask: job.mask, view: job.view, turnUp: job.turnUp == null ? null : job.turnUp, upSystem: job.upSystem == null ? null : job.upSystem, text: job.text, lines: (job.lines || []).slice(), centre: f.centre.slice(), angle: f.angle || 0, size: f.size, wantSize: job.wantSize == null ? null : job.wantSize,
      nudged: !!job.nudged, fit: Object.assign({}, f, { centre: f.centre.slice() }) };
  }
  const same = (job, s) => !!job.fit && job.fit.size === s.size && (job.fit.angle || 0) === s.angle && job.fit.centre[0] === s.centre[0] && job.fit.centre[1] === s.centre[1] && (job.lines || []).join('\n') === s.lines.join('\n') && (!s.view || job.view === s.view);
  /** Put the words back where the snapshot had them. false when the job is not the one the snapshot was taken of any more (it was fitted again: other words, another font, the charm changed) or is
   *  decided: nothing is touched then. */
  function restore(job, snap) {
    if (!job || !snap || snap.v !== 1 || !canMove(job)) return false;
    // (the mask of a piece the person TURNED since the snapshot is another object than the snapshot's, but of the same lineage (mask.lin, set by Engrave.turnTo): the same piece, not one fitted again)
    if (job.key !== snap.key || (job.mask !== snap.mask && !(snap.view && snap.mask && snap.mask.lin && job.mask && job.mask.lin === snap.mask.lin)) || job.text !== snap.text) return false;
    if (same(job, snap)) { job.nudged = snap.nudged; return true; }
    const turned = !!snap.view && job.view !== snap.view;
    if (turned) {   // the whole piece back to the orientation it had: the view and mask first, then the words on them
      job.view = snap.view; job.mask = snap.mask;
      if (snap.turnUp == null) delete job.turnUp; else job.turnUp = snap.turnUp;
      if (snap.upSystem == null) delete job.upSystem; else job.upSystem = snap.upSystem;
    }
    restoreFit(job, snap.fit, snap.centre, snap);
    if (turned) { try { const E = engrave(); if (E && E.repaintTurn) E.repaintTurn(job); } catch (_) { /* the next draw shows it */ } }
    return true;
  }
  /** Reinstate a saved fit and let the engine draw it again (a nudge of nothing: it repaints the picture and schedules the checkpoint). */
  function restoreFit(job, fit, centre0, snap) {
    const E = engrave(), G = geom();
    job.fit = Object.assign({}, fit, { centre: (centre0 || fit.centre).slice() });
    if (snap) { job.lines = snap.lines.slice(); job.text = snap.text; job.wantSize = snap.wantSize; }
    try { job.verify = { geometry: G.verifyInk(job.fit.cmds, job.mask), at: Date.now() }; } catch (_) { /* the engine's own check below */ }
    try { if (E && E.nudge) E.nudge(job, 0, 0); } catch (_) { /* the fit above stands */ }
    if (snap) job.nudged = snap.nudged;
    try { const S = session(); if (S && S.schedule) S.schedule(); } catch (_) { /* the next change schedules it */ }
  }

  /** dx for the piece as the job draws it: the Right ear's view is the Left's mirror image, so the same move is the opposite dx. Every other piece (a single charm, a disc, a Left ear) as is. */
  function mirrorDx(job, dxMm) { return job && job.slot === 'R' ? (dxMm === 0 ? 0 : -dxMm) : dxMm; }

  return { of, nudge, centre, size, snapshot, restore, mirrorDx, canMove, maskCentroid };
});
