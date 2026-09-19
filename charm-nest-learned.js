/* Learned proposals + exact geometry. No language-model calls and no layout scaling.
 * Works in a dedicated Worker; the published network runs in the paired laptop runner.
 */
(function (root, factory) {
  const api = factory(typeof module === "object" ? require("./charm-nest-solver.js") : root.CharmNestSolver);
  if (typeof module === "object") module.exports = api; else root.CharmNestLearned = api;
})(typeof self !== "undefined" ? self : globalThis, function (Sv) {
  "use strict";
  const angle = a => ((a % 360) + 360) % 360;
  const pause = () => new Promise(r => setTimeout(r, 0));
  const clean = r => { const { grids, rec, ...rest } = r; return rest; };
  const compact = pl => pl.length ? Math.max(...pl.map(p => p.cxPt + p.wPt / 2)) * Math.max(...pl.map(p => p.cyPt + p.hPt / 2)) : Infinity;

  // Trace the largest exterior of a low-resolution silhouette for learned features only.
  // All disconnected components, holes, stock walls and clearances remain in the exact mask.
  function polygon(piece) {
    const { bits, w, h } = Sv.resample(piece.bits, piece.w, piece.h, piece.scale, 1);
    const edges = new Map(), stride = w + 1;
    const add = (x, y, X, Y) => { const k = y * stride + x; if (!edges.has(k)) edges.set(k, []); edges.get(k).push(Y * stride + X); };
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) {
      if (!y || !bits[(y - 1) * w + x]) add(x, y, x + 1, y);
      if (x === w - 1 || !bits[y * w + x + 1]) add(x + 1, y, x + 1, y + 1);
      if (y === h - 1 || !bits[(y + 1) * w + x]) add(x + 1, y + 1, x, y + 1);
      if (!x || !bits[y * w + x - 1]) add(x, y + 1, x, y);
    }
    let best = [], bestArea = 0;
    while (edges.size) {
      const first = edges.keys().next().value, loop = []; let k = first;
      do {
        loop.push([k % stride - w / 2, Math.floor(k / stride) - h / 2]);
        const next = edges.get(k); if (!next?.length) break;
        const old = k; k = next.pop(); if (!next.length) edges.delete(old);
      } while (k !== first);
      const area = loop.reduce((s, p, i) => { const q = loop[(i + 1) % loop.length]; return s + p[0] * q[1] - q[0] * p[1]; }, 0);
      if (area > bestArea) { bestArea = area; best = loop; }
    }
    // Remove collinear raster vertices before sampling the feature contour.
    const corners = best.filter((p, i) => { const a = best[(i + best.length - 1) % best.length], b = best[(i + 1) % best.length]; return (p[0] - a[0]) * (b[1] - p[1]) !== (p[1] - a[1]) * (b[0] - p[0]); });
    return corners.filter((_, i) => i % Math.max(1, Math.ceil(corners.length / 256)) === 0);
  }

  function fingerprint(job) {
    let h = 2166136261;
    const feed = n => { h ^= n; h = Math.imul(h, 16777619); };
    for (const p of job.pieces) for (const b of p.bits) feed(b);
    const meta = JSON.stringify([job.sheet, job.clearancePt, job.maxFill, job.learned?.modelDigest, job.pieces.map(p => [p.id, p.w, p.h, p.scale, p.order, p.orderDate, p.pinned])]);
    for (let i = 0; i < meta.length; i++) feed(meta.charCodeAt(i));
    return "learned-v1-" + (h >>> 0).toString(16);
  }

  function groups(pieces) {
    const map = new Map();
    pieces.forEach((p, i) => { const id = p.order || p.id; if (!map.has(id)) map.set(id, { date: +p.orderDate || 0, i, pieces: [] }); const g = map.get(id); g.date = Math.min(g.date, +p.orderDate || 0); g.pieces.push(p); });
    return [...map.values()].sort((a, b) => a.date - b.date || a.i - b.i);
  }

  async function propose(job, pieces, cb) {
    if (cb.propose) return cb.propose(job, pieces);
    const controller = new AbortController();
    const timer = setInterval(() => { if (cb.shouldStop?.()) controller.abort(); }, 200);
    const timeout = setTimeout(() => controller.abort(), 300000);
    try {
      const response = await fetch("http://127.0.0.1:8766/propose", {
        method: "POST", signal: controller.signal,
        headers: { "Content-Type": "application/json", Authorization: "Bearer " + job.learned.token },
        body: JSON.stringify({ sheet: job.sheet, seed: job.seed, pieces: pieces.map(p => ({ id: p.id, polygon: polygon(p) })) })
      });
      const data = await response.json(); if (!response.ok) throw new Error(data.error || "Runner unavailable");
      return data;
    } finally { clearInterval(timer); clearTimeout(timeout); }
  }

  async function solve(job, cb = {}) {
    const t0 = performance.now(), key = fingerprint(job), opts = job.learned || {};
    const restored = opts.checkpoint?.key === key ? opts.checkpoint : null;
    const priorMs = restored?.activeMs || 0;
    const budget = Math.max(1000, Math.min(36000000, +opts.budgetMs || 600000));
    const stall = Math.max(1000, Math.min(budget, +opts.stallMs || 300000));
    const elapsed = () => priorMs + performance.now() - t0;
    const stopped = () => cb.shouldStop?.() || elapsed() >= budget;
    const random = Sv.rng((job.seed || 1) + (restored?.attempts || 0));
    const cache = new Map(), byId = new Map(job.pieces.map(p => [p.id, p])), orderGroups = groups(job.pieces);
    let best = null, baseline = null, attempts = restored?.attempts || 0, lastGain = elapsed(), model = restored?.model || null;
    let history = restored?.history?.slice(-40) || [], lastCheckpoint = 0;
    const variant = (p, a) => {
      a = Math.round(angle(a) * 10) / 10; const k = p.id + "@" + a;
      if (!cache.has(k)) {
        const v = Sv.prepareVariant(p, a, job.clearancePt, job.fineRes || 2);
        if (v) v.cells = v.fine.bits.reduce((s, n) => s + n, 0);
        cache.set(k, v); if (cache.size > 160) cache.delete(cache.keys().next().value);
      }
      return cache.get(k);
    };
    const grid = () => Sv.makeSheetGrid(job.sheet, job.clearancePt, job.fineRes || 2);
    const cap = job.maxFill > 0 && job.maxFill < 1 ? job.maxFill : 1;
    const checkpoint = (stage, force = false) => {
      if (!force && performance.now() - lastCheckpoint < 1000) return;
      lastCheckpoint = performance.now();
      cb.onLearned?.({ stage, activeMs: elapsed(), attempts, sinceGainMs: elapsed() - lastGain,
        baselineCount: baseline?.placements.length || 0, bestCount: best?.placements.length || 0,
        model: model?.model || null,
        checkpoint: best ? { key, best: clean(best), baseline, activeMs: elapsed(), attempts, history, model } : null });
    };
    function measure(placements) {
      const g = grid(); let cells = 0; const ids = new Set();
      for (const pl of placements) {
        const p = byId.get(pl.id); if (!p || ids.has(pl.id)) return null;
        if (p.pinned && (Math.abs(pl.cxPt - p.pinned.cxPt) > .001 || Math.abs(pl.cyPt - p.pinned.cyPt) > .001 || Math.abs(angle(pl.angle) - angle(p.pinned.angle)) > .001)) return null;
        const v = variant(p, pl.angle); if (!v) return null;
        const r = Sv.tryPlace(g, v, pl.cxPt, pl.cyPt, 0); if (!r.ok) return null;
        Sv.stampVariant(g, v, r.x, r.y); cells += v.cells; ids.add(pl.id);
      }
      // Pins never move, but an unplaceable older order holds later pinned orders back too.
      let gap = false;
      for (const group of orderGroups) {
        const n = group.pieces.filter(p => ids.has(p.id)).length;
        if (n && (gap || n !== group.pieces.length)) return null;
        if (n < group.pieces.length) gap = true;
      }
      if (cells > g.usableCells * cap + 1) return null;
      const res = g.fineRes, pocket = g.largestPocket();
      return { placements, rejects: job.pieces.filter(p => !ids.has(p.id)).map(p => p.id), density: cells / g.usableCells,
        placedPt2: cells / res ** 2, placedCells: cells, usablePt2: g.usableCells / res ** 2,
        freePt2: g.freeCells() / res ** 2, pocket: { xPt: pocket.x / res, yPt: pocket.y / res, wPt: pocket.w / res, hPt: pocket.h / res } };
    }
    async function accept(layout, source) {
      if (!layout || best && (layout.placements.length < best.placements.length || layout.placements.length === best.placements.length && compact(layout.placements) >= compact(best.placements) - .1)) return false;
      if (!Sv.verify(job, layout.placements, 6).ok) return false;
      const countGain = !best || layout.placements.length > best.placements.length;
      best = clean(layout);
      if (countGain) { lastGain = elapsed(); history.push({ ms: elapsed(), placed: best.placements.length, source }); history = history.slice(-40); }
      cb.onBest?.(best, { trial: attempts, total: job.pieces.length, placed: best.placements.length, elapsedMs: elapsed(), source });
      checkpoint(source, true); await pause(); return true;
    }
    async function challenger() {
      const incoming = opts.incumbent; opts.incumbent = null;
      if (!incoming || baseline && incoming.placements.length <= baseline.placements.length) return;
      const layout = measure(incoming.placements);
      if (!layout || !Sv.verify(job, layout.placements, 6).ok) return;
      baseline = clean(layout);
      await accept(layout, "Standard challenger improved the incumbent");
    }
    if (restored?.best) {
      await accept(measure(restored.best.placements), "Resumed verified checkpoint");
      baseline = restored.baseline;
    }
    if (!best && Array.isArray(opts.initialLayout) && opts.initialLayout.length) {
      await accept(measure(opts.initialLayout), "Retained the previous verified sheet");
      baseline = best ? clean(best) : null;
    }
    if (!best && !stopped()) {
      checkpoint("Measuring the standard baseline", true);
      const result = await Sv.solve({ ...job, timeBudgetMs: Math.min(20000, budget - elapsed()), maxTrials: 24, stallMs: 10000, packingHints: null }, {
        shouldStop: stopped, onStage: cb.onStage, onPlaced: cb.onPlaced,
        onTrial: () => { attempts++; checkpoint("Measuring the standard baseline"); }
      });
      await accept(measure(result.placements), "Standard baseline"); baseline = best ? clean(best) : null;
    }
    let proposals = restored?.proposals || [], modelError = null;
    await challenger();
    if (!stopped() && best?.rejects.length && !model) {
      checkpoint("GFPack++ generating positions and continuous rotations", true);
      // A bounded oldest-order prefix supplies the network. Never truncate a whole order.
      const target = []; let area = 0; const limit = grid().usableCells / (job.fineRes || 2) ** 2 * cap;
      for (const group of orderGroups) {
        if (target.length + group.pieces.length > 160) break;
        target.push(...group.pieces); area += group.pieces.reduce((s, p) => s + (p.areaPt2 || p.w * p.h / p.scale ** 2), 0);
        if (area > limit * 1.15) break;
      }
      try {
        const reply = await propose(job, target, { ...cb, shouldStop: stopped });
        if (!reply?.model || !Array.isArray(reply.candidates) || reply.candidates.length > 8) throw new Error("Invalid learned-model response");
        proposals = reply.candidates.filter(c => Array.isArray(c) && c.length <= 160 && c.every(p => byId.has(p.id) && [p.cxPt, p.cyPt, p.angle].every(Number.isFinite)));
        if (!proposals.length) throw new Error("Model supplied no usable candidates");
        model = { model: reply.model, weightsSha256: reply.weightsSha256, elapsedMs: reply.elapsedMs, device: reply.device };
      } catch (e) { if (!cb.shouldStop?.()) modelError = e.message; }
    }

    // Repair proposals or remove a local cluster from the incumbent, then reinsert it
    // alongside the oldest missing order. The complete sheet is never blindly randomized.
    async function repair(proposal, local) {
      // Translation only: retain the learned scale, relative positions and rotations.
      // The training strip's origin is arbitrary; align the proposed bounds to our inset.
      const bounds = proposal.map(pl => { const v = variant(byId.get(pl.id), pl.angle); return v ? { x: pl.cxPt - v.solid.cx / (job.fineRes || 2), y: pl.cyPt - v.solid.cy / (job.fineRes || 2) } : null; }).filter(Boolean);
      const dx = bounds.length ? (job.sheet.insetPt || 0) - Math.min(...bounds.map(p => p.x)) : 0;
      const dy = bounds.length ? (job.sheet.insetPt || 0) - Math.min(...bounds.map(p => p.y)) : 0;
      const guide = new Map(proposal.map(p => [p.id, { ...p, cxPt: p.cxPt + dx, cyPt: p.cyPt + dy }]));
      const g = grid(), placements = [], kept = new Set(); let cells = 0;
      const missing = orderGroups.find(gr => gr.pieces.some(p => !best?.placements.some(pl => pl.id === p.id)));
      const targetIds = new Set(orderGroups.slice(0, missing ? orderGroups.indexOf(missing) + 1 : orderGroups.length).flatMap(gr => gr.pieces.map(p => p.id)));
      let remove = new Set();
      if (local && best) {
        const pocket = best.pocket, cx = pocket.xPt + pocket.wPt / 2, cy = pocket.yPt + pocket.hPt / 2;
        const flexible = best.placements.filter(p => !byId.get(p.id).pinned);
        const center = attempts % 3 ? { cxPt: cx, cyPt: cy } : flexible[Math.floor(random() * flexible.length)] || { cxPt: cx, cyPt: cy };
        remove = new Set(flexible.sort((a, b) => Math.hypot(a.cxPt - center.cxPt, a.cyPt - center.cyPt) - Math.hypot(b.cxPt - center.cxPt, b.cyPt - center.cyPt)).slice(0, 4 + attempts % 9).map(p => p.id));
      }
      const fixed = job.pieces.filter(p => p.pinned).map(p => ({ id: p.id, ...p.pinned }));
      if (local) fixed.push(...best.placements.filter(p => !byId.get(p.id).pinned && !remove.has(p.id)));
      for (const pl of fixed) {
        const v = variant(byId.get(pl.id), pl.angle), r = v && Sv.tryPlace(g, v, pl.cxPt, pl.cyPt, 0);
        if (!r?.ok) return null;
        Sv.stampVariant(g, v, r.x, r.y); placements.push({ ...pl, wPt: v.solid.w / g.fineRes, hPt: v.solid.h / g.fineRes }); kept.add(pl.id); cells += v.cells;
      }
      const todo = job.pieces.filter(p => targetIds.has(p.id) && !kept.has(p.id)).map(p => ({ p, weight: (p.areaPt2 || 1) * (.8 + random() * .4) })).sort((a, b) => b.weight - a.weight).map(x => x.p);
      for (const p of todo) {
        if (stopped()) return null;
        const hint = guide.get(p.id) || best?.placements.find(pl => pl.id === p.id);
        const pivot = hint?.angle ?? random() * 360;
        const spread = [30, 10, 3, 1][attempts % 4];
        const angles = [...new Set([pivot, pivot - spread, pivot + spread, pivot + (random() * 2 - 1) * spread, ...(!local ? job.angles : [random() * 360])].map(a => Math.round(angle(a) * 10) / 10))];
        const variants = angles.map(a => variant(p, a)).filter(v => v && cells + v.cells <= g.usableCells * cap);
        let spot = null, v = null;
        if (hint && !local) {
          v = variants[0]; const r = v && Sv.tryPlaceTight(g, v, hint.cxPt, hint.cyPt, 3);
          if (r?.ok) spot = { ...r, angle: v.angle };
        }
        if (!spot) { spot = Sv.bestSpots(g, variants, 1)[0]; v = spot && variants.find(v => v.angle === spot.angle); }
        if (!spot || !v) continue;
        Sv.stampVariant(g, v, spot.x, spot.y); cells += v.cells;
        placements.push({ id: p.id, angle: v.angle, cxPt: (spot.x + v.solid.cx) / g.fineRes, cyPt: (spot.y + v.solid.cy) / g.fineRes, wPt: v.solid.w / g.fineRes, hPt: v.solid.h / g.fineRes });
        cb.onPlaced?.(placements[placements.length - 1], { trial: attempts, placed: placements.length, total: job.pieces.length });
        checkpoint("Refining rotations and filling gaps"); await pause();
      }
      // Discard the first incomplete order and every later flexible order together.
      const ids = new Set(placements.map(p => p.id)); let cutoff = false;
      const allowed = new Set();
      for (const gr of orderGroups) { if (gr.pieces.some(p => !ids.has(p.id))) cutoff = true; for (const p of gr.pieces) if (!cutoff) allowed.add(p.id); }
      return measure(placements.filter(p => allowed.has(p.id)));
    }
    if (!modelError) {
      for (const proposal of proposals) { if (stopped()) break; attempts++; await accept(await repair(proposal, false), "Learned proposal + geometric repair"); }
      while (best?.rejects.length && !stopped() && elapsed() - lastGain < stall) {
        await challenger();
        attempts++; await accept(await repair(proposals[attempts % Math.max(1, proposals.length)] || [], true), "Rotation refinement");
        checkpoint("Refining rotations and filling gaps"); await pause();
      }
    }
    await challenger();
    checkpoint(modelError ? "Runner failed — retained verified baseline" : "Finished", true);
    if (!best) throw new Error("No verified layout found; original sheet retained");
    return { ...best, trials: attempts, elapsedMs: elapsed(),
      endedBy: cb.shouldStop?.() ? "stopped" : !best.rejects.length ? "complete" : elapsed() >= budget ? "budget" : modelError ? "model-unavailable" : "plateau",
      params: { seed: job.seed, angles: [...new Set(best.placements.map(p => p.angle))], clearancePt: job.clearancePt, insetPt: job.sheet.insetPt, fineRes: job.fineRes || 2, engine: "learned", model, modelError,
        baselineCount: baseline?.placements.length || 0, gainedPieces: best.placements.length - (baseline?.placements.length || 0), history, maxFill: cap }
    };
  }
  return { solve, polygon, fingerprint, groups };
});
