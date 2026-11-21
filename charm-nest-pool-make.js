/*  charm-nest-pool-make.js — the piece maker: an order line made up into its pieces (pool rows and their charms), from the library's design files.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Cloud plan v2, build step 3 (Paul, 20 Nov 2026: no production work runs on any computer, the cloud does it all): the code that made a line's pieces lived inside
 *  charm-nest-bridge.js's Pool, so only a page could make them. It is here now, unchanged, and the page calls it (Pool binds every function below and exports
 *  them as before: Pool.sizeEntry, Pool.masterCharm, Pool.ensureBase, ...). A node host makes the same pieces from the same code (netlify/functions/
 *  _charmNestPdf.js withCanvas() draws the silhouettes there; tests/charm-nest/cloud-silhouette-parity.cjs holds node to the page's canvas).
 *
 *  Loads two ways, one source:  window.CharmNestPoolMake (the sorter page, before charm-nest-bridge.js)
 *                               module.exports          (node: require("../../charm-nest-pool-make.js"))
 *
 *  create(host) → the piece maker for one host, made the first time one of its functions is called (nothing of the host is read before). Everything it reads of the
 *  world comes through the host; nothing here reaches a page global, the network or Etsy on its own (it has no SKU-table code at all: no listing SKU tables, no
 *  inventory; a design file is read through host.api "url" and host.CharmNestAssets().bytes).
 *    window        the page's window (node: an object holding what is loaded, CharmNestPair; CustomPrint and CustomSheet are absent there)
 *    S, B          the page's state objects (S.settings, S.poolSources, S.cloud; B.pool.rows, B.pool.sources, B.master.entries): the page reassigns
 *                  S.settings, S.poolSources, B.pool.rows and B.master.entries, so the maker keeps the two objects and reads their fields when it uses them
 *    O, P, MM, Master, PoolPieces   CharmNestOrders, CharmNestPDF, 25.4 / 72, the library (entryFor, fetchEntry, keepOutOf, skuRegex), CharmNestPoolPieces
 *    agent, api, stockFor, labelOf, now          the page's log, its function calls, the plate of a metal, a metal's name, performance.now()
 *    CharmNestAssets(), Orders(), Review(), CustomPrint(), CustomSheet(), ArtEdit()   read when used (ArtEdit: undefined where none is loaded)
 *    keptMaster, keepMaster, pinPooled, charmOf, greenHeld, cancelled          the page's own: a design kept on this computer, a line pinned to the pieces it
 *                  was pooled with, a piece on one of this page's sheets, a line waiting for a green line, an order cancelled
 *    cloud         true for a node host only: the cloud's retry rules (an Etsy stop or a 429 never passes by itself, m:E2) and pool rows that say which body
 *                  each piece is cut from (bodyIndex, oneBody). Absent on the page: its pieces, rows and retries are exactly what they were.
 *    pace          optional (node): awaited before each piece of a line after the first (pace(): a turn of the event loop, then a time budget, C7). The page passes none.
 *  lineRecord(row, B) is the run record's copy of a line (Orders.lineRecord); PAGE / PASSING / RETRY_MS the pool's retry rules (create(host).passing reads them).
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestPoolMake = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";

  /* A failure that passes by itself (the cloud slow to answer, the edge's HTML "Inactivity Timeout" page, a 5xx or a
     gateway fault, the network gone for a moment) is no problem with the line. Holding the line over one left order
     4330132720 held with that page as its reason and nothing to lift it (25 Sep). Such a line stays pulled, in progress,
     and the intake takes it up again 30 s, 1 and 2 minutes, then every 5 minutes after the failure while it lasts; only a
     real problem holds a line. api() marks these failures e.transient; a reason kept from before says so only in words. */
  const PAGE = /<(?:!doctype|html|head|body|title)\b/i, PASSING = /inactivity timeout|timed out|answer in time|not answering right now|HTTP (?:408|429|5\d\d)\b|\((?:408|429|5\d\d)\)|failed to fetch|networkerror|load failed/i;
  const passing = e => !!e && (e.transient === true || +e.status >= 500 || [408, 429].includes(+e.status) || PAGE.test(String(e.message ?? e)) || PASSING.test(String(e.message ?? e)));
  const RETRY_MS = 5 * 60000;

  /* OUTLINE TOLERANCE (Paul, 20 Nov 2026: a charm's outline made in the cloud may differ from Chrome's by up to 0.06 mm, never more). charm-nest-pdf.js buildSilhouettes draws at 6 px a point,
     where one pixel is 25.4 / 72 / 6 = 0.0588 mm: a silhouette the cloud draws is accepted when every pixel it draws differently from Chrome's lies within ONE pixel of Chrome's own edge (one of the
     8 pixels around it has the other value in Chrome's silhouette: the edge moved by at most a pixel there, or a gap thinner than a pixel closed), on a canvas of the same size, with the area
     within 1 %. Its hash is not compared: the hash is a 24 x 24 grid of the bits, and it flips on single edge pixels. The cloud therefore never knows a design, or a body of one, by its
     silhouette's hash (BODYID: soloBase, bodyOf; the pool row's bodyIndex and oneBody). */
  const OUTLINE_TOL_MM = 0.06, OUTLINE_TOL_PX = 1, MM_PER_PT = 25.4 / 72;
  /** Chrome's silhouette `page` against the cloud's `cloud`, each { w, h, bits (0/1 a pixel, row by row), areaPt2?, hash?, scale? }. o: { tolPx (1), areaTol (0.01), scale (6 px a point) }.
   *  → { ok, why, diff (pixels that differ), off (of them, further than tolPx from Chrome's edge), worstPx, worstMm (the furthest, measured to Chrome's edge), areaDiff (a share), sameHash }. */
  function silhouetteWithin(page, cloud, o) {
    o = o || {};
    const tol = Number.isFinite(+o.tolPx) && +o.tolPx >= 0 ? +o.tolPx : OUTLINE_TOL_PX, areaTol = Number.isFinite(+o.areaTol) && +o.areaTol >= 0 ? +o.areaTol : 0.01;
    const scale = +o.scale || +(page && page.scale) || 6, far = tol + 2;
    if (!page || !cloud || !page.bits || !cloud.bits) return { ok: false, why: "a silhouette is missing" };
    if (page.w !== cloud.w || page.h !== cloud.h) return { ok: false, why: `canvas ${cloud.w}x${cloud.h}, Chrome's ${page.w}x${page.h}` };
    const W = page.w, Hh = page.h, n = W * Hh, A = page.bits, C = cloud.bits;
    if (A.length < n || C.length < n) return { ok: false, why: "a silhouette holds fewer pixels than its canvas" };
    /* the distance, in whole pixels (the square around it), from a pixel to the nearest pixel of the other value in Chrome's silhouette: 1 = on Chrome's edge; far = further than tol + 1 */
    const edgeDist = (x, y, v) => {
      for (let r = 1; r < far; r++) {
        for (let dy = -r; dy <= r; dy++) {
          const yy = y + dy; if (yy < 0 || yy >= Hh) continue;
          const ring = dy === -r || dy === r;
          for (let dx = -r; dx <= r; dx += ring ? 1 : 2 * r) { const xx = x + dx; if (xx >= 0 && xx < W && A[yy * W + xx] !== v) return r; }
        }
      }
      return far;
    };
    let diff = 0, off = 0, worst = 0;
    for (let i = 0; i < n; i++) {
      if ((A[i] ? 1 : 0) === (C[i] ? 1 : 0)) continue;
      diff++;
      const x = i % W, y = (i - x) / W, d = edgeDist(x, y, A[i]);
      if (d > worst) worst = d;
      if (d > tol) off++;
    }
    const pa = +page.areaPt2, ca = +cloud.areaPt2, areaDiff = pa > 0 && ca >= 0 ? Math.abs(ca - pa) / pa : 0;
    const worstMm = worst * MM_PER_PT / scale, why = [off ? `${off} pixel(s) further than ${tol} px (${(tol * MM_PER_PT / scale).toFixed(3)} mm) from Chrome's edge, the furthest ${worst >= far ? "more than " + (far - 1) : worst} px` : "", areaDiff > areaTol ? `the area differs by ${(areaDiff * 100).toFixed(2)} %` : ""].filter(Boolean).join("; ");
    return { ok: !why, why, diff, off, worstPx: worst, worstMm: +worstMm.toFixed(4), areaDiff, sameHash: page.hash != null && cloud.hash != null && String(page.hash) === String(cloud.hash) };
  }
  /* m:E2 (cloud plan v2, step 3): in the cloud an Etsy stop never passes by itself. The page keeps its rule (a 429 is tried again 30 s, 1, 2 and every 5 minutes
     later), but a node host must not keep asking while the shop's Etsy stop switch is on (netlify/functions/_etsyStop.js answers 429 { error: "etsy_stopped" },
     header x-etsy-stopped: 1): a 429 and every stop answer are real failures there, held and said, until a person presses Resume Etsy. */
  const PASSING_CLOUD = /inactivity timeout|timed out|answer in time|not answering right now|HTTP (?:408|5\d\d)\b|\((?:408|5\d\d)\)|failed to fetch|networkerror|load failed/i;
  const stopAnswer = e => {
    if (!e) return false;
    if (+e.status === 429 || e.stopped === true || e.error === "etsy_stopped") return true;
    try { const h = e.headers || (e.res && e.res.headers); if (h && typeof h.get === "function" && h.get("x-etsy-stopped") === "1") return true; } catch (_) { /* no headers */ }
    return /etsy_stopped|Etsy is stopped|HTTP 429\b|\(429\)/i.test(String(e.message ?? e));
  };
  const passingCloud = e => !!e && !stopAnswer(e) && (e.transient === true || +e.status >= 500 || +e.status === 408 || PAGE.test(String(e.message ?? e)) || PASSING_CLOUD.test(String(e.message ?? e)));

  /* The record used to keep a line's state and little else, so a finished run could be listed but never opened: no
     title, no listing, no metal, no ship-by meant no card, no thumbnail and no filter. It keeps a compact copy of the
     line now — everything a row is built from, capped so four hundred of them still fit in one document. */
  const cap = (v, n) => String(v == null ? "" : v).slice(0, n);
  function lineRecord(row, B) {
    const l = row.line, o = row.order;
    // A line a person completed by hand (Review → Complete Order, or its QR label printed: CharmNestReadiness.isHand) needs no sheet. The run record only HINTS
    // that (handDone): the server reads the custom order's own record (a run saves its lines when it pools, not when a card is completed, and a Reopen since
    // would leave this copy stale) and trusts nothing here. Such a line is not noDesign for that reason, so a stale record can never keep a reopened piece resolved.
    // (only a line with no pool ids: the card a person completes was never pooled, and the server looks up exactly those lines)
    const cd = row.spec && row.spec.customDone, hand = !!cd && cd.state !== "open" && cd.how !== "sheet" && !(row.poolIds || []).length;
    return [row.key, { state: row.state, poolIds: row.poolIds, reason: row.reason, ...(row.etsyClosed ? { etsyClosed: +row.etsyClosed } : {}), hold: row.hold || null, wait: row.wait || null, ...(row._lp ? { _lp: row._lp } : {}), sku: row.spec && row.spec.designSku, material: row.material || (row.spec && row.spec.material) || null, quantity: row.spec ? row.spec.quantity : 1, pieceCount: row.spec ? (row.spec.pieceCount || row.spec.quantity) : 1,
      engrave: row.engrave ? { needed: !!row.engrave.needed, state: row.engrave.state, approved: !!row.engrave.approved, approvedAt:row.engrave.approvedAt || 0, approvedBy:row.engrave.approvedBy || "", decidedAt:row.engrave.decidedAt || 0, seals:row.engrave.seals || [], text: row.engrave.text || null, ...(row.engrave.pieces ? { pieces: row.engrave.pieces } : {}) } : null,
      // The server reads this record, not the row, before it records a set as complete: a line with nothing to engrave
      // must read as plain there too, including before its engraving check has run.
      engraveCandidate: row.spec ? !!row.spec.engraveCandidate : null, noDesign: hand ? !!(row.spec.special && row.spec.special.notCut) : !!row.spec?.noDesign,
      // the group its pieces say they are (the pool rows' groupSize: set by Pool.pinPooled) and which ear each copy is (spec.pair.sides, only when some copy has one): the server waits for a piece
      // whose record was never written (Readiness.copyIds) and names the ear in its refusals. A line pooled before the rule carries neither.
      ...(row.groupSize > 1 ? { groupSize: row.groupSize } : {}), ...(row.spec && row.spec.pair && Array.isArray(row.spec.pair.sides) && row.spec.pair.sides.some(x => x === "L" || x === "R") ? { sides: row.spec.pair.sides.slice(0, 400).map(x => x === "L" || x === "R" ? x : "") } : {}),
      ...(hand ? { handDone: { at: +cd.completedAt || 0, by: cap(cd.completedBy, 80), how: cd.how === "button" ? "button" : "print" } } : {}),
      activityAt:row.activityAt || 0, changePending:!!row.changePending,repoolChanged:!!row.repoolChanged,arrivedAt: row.arrivedAt || 0, createTs: o.createTs || 0, materialOverride: row.materialOverride || null, sizeOverride: row.sizeOverride || null, ...(row.engravingOverride ? { engravingOverride: row.engravingOverride } : {}), problems: (row.problems || []).map(p => p.kind), updateTs: o.updateTs, orderId: o.receiptId, transactionId: l.transactionId,
      ...(+row.frontAt > 0 ? { frontAt: +row.frontAt } : {}), ...(row.releasing ? { releasing: { at: +row.releasing.at || 0, by: cap(row.releasing.by, 80), stage: String(row.releasing.stage || "").slice(0, 20) } } : {}), ...(row.skuOverride && typeof row.skuOverride === "object" && row.skuOverride.sku ? { skuOverride: { sku: cap(row.skuOverride.sku, 80), by: cap(row.skuOverride.by, 80), at: +row.skuOverride.at || 0, was: cap(row.skuOverride.was, 80) } } : {}), ...(row.blockOverride && typeof row.blockOverride === "object" && row.blockOverride.sku ? { blockOverride: { sku: cap(row.blockOverride.sku, 80), by: cap(row.blockOverride.by, 80), at: +row.blockOverride.at || 0, ...(Array.isArray(row.blockOverride.skus) && row.blockOverride.skus.length ? { skus: row.blockOverride.skus.slice(0, 20).map(x => cap(x, 80)) } : {}) } } : {}), ...(row.releaseHold && +row.releaseHold.at > 0 ? { releaseHold: { at: +row.releaseHold.at, by: cap(row.releaseHold.by, 80), id: cap(row.releaseHold.id, 120) } } : {}), ...(row.releaseAnyway && +row.releaseAnyway.at > 0 ? { releaseAnyway: { at: +row.releaseAnyway.at, by: cap(row.releaseAnyway.by, 80), kinds: (row.releaseAnyway.kinds || []).slice(0, 8).map(x => cap(x, 24)) } } : {}),   // (RELEASE, Paul 19 Oct 2026: the questions a Release Hold set aside for this line; RELHOLD: the Review card's Release Hold press, who and when; BLOCKREL: a person's "Release to nesting queue" on a blocked SKU; released from hold: ahead of the orders coming in; a release not finished yet, taken up again by the next page, OrderHold)
      ...(B.run ? { crh: ((((B.maps && B.maps.customRead) || {})[row.key] || {}).hash) || null } : {}),   // (RVSAME: which of Claude's readings the run uses for this line, so every computer uses the same one)
      snap: { title: cap(l.title, 160), listingId: cap(l.listingId, 24), metalKey: cap(l.metalKey, 24), metalLabel: cap(l.metalLabel, 40), bought: cap(row.spec && row.spec.boughtSku != null ? row.spec.boughtSku : l.sku, 80),   // (ANYWHERE: the SKU Etsy sold it under, for a computer that follows the run)

        orderNumber: cap(o.orderNumber, 24), buyer: cap(o.buyer && o.buyer.name, 60), shipBy: +o.shipBy || 0, isGift: !!o.isGift,
        vars: (l.variations || []).map(v => String(v.name ?? v.formatted_name ?? "") + "\u241f" + String(v.value ?? v.formatted_value ?? "")),
        pers: (l.personalization || []).slice(0, 4).map(x => cap(x, 200)) } }];
  }

  /** (cloud plan v2, step 3, C7) A node host's pace for piece making: before each piece of a line after the first the event loop gets a turn (setImmediate), and once
   *  `budgetMs` (45 s) have passed since the pace was made it throws { budget: true }: the unit ends there and is taken up again (the line is made again whole:
   *  nothing of it is written before all its pieces are made). */
  function pace(o) {
    const budget = Math.max(1000, +(o && o.budgetMs) || 45000), t0 = Date.now();
    const turn = typeof setImmediate === "function" ? () => new Promise(r => setImmediate(r)) : () => new Promise(r => setTimeout(r, 0));
    return async () => { await turn(); if (Date.now() - t0 > budget) throw Object.assign(new Error("the piece maker's time budget is spent: the unit is taken up again"), { budget: true }); };
  }

  /* SIZEFIT (Paul, 15 Oct 2026: "automatically adapts the charm size from the repo to match the buyer chosen size/diameter"): the design for a size the library does not hold is the nearest design of that
     round disc drawn at the diameter chosen (charm-nest-orders.js sizeFit decides; charm-nest-pdf.js scaleSource draws it). It is a design of its own as far as the page is concerned: its file key
     says what it is, `<the file>#fit-12MM@0.71429` (the size asked and the factor), so a source made from it, a pool row and a sheet record all name it, and a record read again later is drawn the same. */
  const FIT_PATH = /#fit-([A-Z0-9.]+)@(\d+(?:\.\d+)?)$/;

  /* ═══ the piece maker for one host (see the head of this file) ═══════════════════════════════════════════════════════════════
     create(host) reads nothing of the host: the maker is made the first time one of its functions is called (make), and only then are host.S, host.B, host.O ...
     read, so a page makes its Pool while the bridge loads exactly as before (the Pool read nothing of the page until a line was made), and a host may give
     them as getters. Each function below is that maker's own, called through. */
  const MADE = ["sizeEntry", "sizeKeyOf", "sizeHold", "masterCharm", "entryHashes", "loadMasterCharm", "readMasterCharm", "splitBodies", "traceBodies", "soloBodies", "traceSolo", "soloBase",
    "fitFactor", "fitBytes", "cloneCharm", "makePool", "makePoolRead", "stampDesignHold", "bodyPlanFor", "soloFor", "memberSet", "memberSkus", "earMirror", "pairLine", "sidedPiece", "wholeBody",
    "baseFor", "ensureBase", "gluedLine", "poolAnswer", "trySig", "due", "passing", "bodyOf"];
  function create(H) {
    const masterLoads = new Map(); let made = null;
    const maker = () => made || (made = make(H, masterLoads));
    const out = { FIT_PATH, masterLoads };
    for (const k of MADE) out[k] = (...a) => maker()[k](...a);
    return out;
  }
  function make(H, masterLoads) {
    const win = H.window || {}, S = H.S, B = H.B, O = H.O, P = H.P, MM = H.MM, Master = H.Master, PoolPieces = H.PoolPieces;
    const agent = (...a) => H.agent(...a), api = (...a) => H.api(...a), stockFor = m => H.stockFor(m), labelOf = m => H.labelOf(m);
    const fitCache = new WeakMap();
    function scaledGeom(entry, size, fit) {
      const base = entry.sizes && Object.keys(entry.sizes).length ? entry.sizes[fit.base] : entry;
      if (!base || !base.aiPath || !(fit.scale > 0)) return null;
      const sig = [base.aiPath, base.charmHash || "", base.widthPt, base.heightPt, fit.scale.toFixed(5), base.facing || "", base.areaPt2 || 0].join("|");   // (the design's facing is set in place by Master.patch: a copy made before must not keep the old word)
      let m = fitCache.get(entry); if (!m) fitCache.set(entry, m = new Map());
      const hit = m.get(size); if (hit && hit.sig === sig) return hit.geom;
      const k = fit.scale, M = O.SIZE_PAD_PT || 0, dim = v => (v - M) * k + M, tok = String(+fit.wantMm.toFixed(2)) + "MM", geom = Object.assign({}, base, {   // (tok: the size by its number, whatever words a saved answer typed: "10 mm", "1 cm")   // (the index's size is the outer box: the drawing's own size scales, its margin does not)
        aiPath: `${base.aiPath}#fit-${tok}@${k.toFixed(5)}`, base, scaled: true, scale: k, fromMm: fit.haveMm, wantMm: fit.wantMm,
        charmHash: base.charmHash ? `${base.charmHash}~${tok}` : base.charmHash, widthPt: dim(base.widthPt), heightPt: dim(base.heightPt), areaPt2: base.areaPt2 > 0 ? base.areaPt2 * k * k : base.areaPt2, thumbPath: null, thumbUrl: null });
      m.set(size, { sig, geom }); return geom;
    }
    /** The design of an entry for a size: the size it holds, else (a round disc, a size in mm) the nearest one drawn at the size chosen, else (a design that holds sizes but not this one) nothing. */
    const sizeEntry = (entry, size) => {
      const sized = !!(entry.sizes && Object.keys(entry.sizes).length);
      if (sized && size && entry.sizes[size]) return entry.sizes[size];
      let fit = null; try { fit = size ? O.sizeFit(entry, size) : null; } catch (_) { fit = null; }
      if (fit && fit.action === "scale") { const g = scaledGeom(entry, size, fit); if (g) return g; }
      if (fit && fit.action === "same" && sized) return entry.sizes[fit.base] || null;
      return sized ? null : entry;
    };
    /** The size key a design file belongs to: the library's own size of that file, or the size a scaled drawing was made for. */
    const sizeKeyOf = (entry, path) => { const m = FIT_PATH.exec(String(path || "")); return m ? m[1] : Object.entries(entry.sizes || {}).find(([, v]) => v && v.aiPath === path)?.[0] || null; };
    /** Why a line of this size cannot be made up from this design, or null: a size more than 2.5 times the drawn disc (or under 0.4 of it) is a mistake of the listing or the drawing, a design that holds sizes but not this one has none to draw. */
    const sizeHold = (entry, size, sku) => {
      const sized = !!(entry.sizes && Object.keys(entry.sizes).length);
      let fit = null; try { fit = size ? O.sizeFit(entry, size) : null; } catch (_) { fit = null; }
      if (fit && fit.action === "ask") return { reason: fit.why, problem: { kind: "missingSize", sku, size, available: sized ? Object.keys(entry.sizes) : [fit.offer], why: fit.why } };   // (a design with no sizes of its own offers the one it is drawn at: the Review card's list was empty, nobody could release the line)
      if (sized && !sizeEntry(entry, size)) return { reason: `no design for size ${size || "(none)"}`, problem: { kind: "missingSize", sku, size, available: Object.keys(entry.sizes) } };
      return null;
    };
    /** Fetch the per-SKU .ai once per session, parse it, trace it; every copy shares the geometry. (masterLoads: one per host, made by create) */
    async function masterCharm(entry, size) {
      const key=sizeEntry(entry,size)?.aiPath;
      if(!key)return loadMasterCharm(entry,size);
      if(masterLoads.has(key))return masterLoads.get(key);
      const task=loadMasterCharm(entry,size).finally(()=>{if(masterLoads.get(key)===task)masterLoads.delete(key);});   // (REDRAW: a read forgotten by forgetDrawing does not take the read that replaced it out of the map)
      masterLoads.set(key,task);return task;
    }
    /** The hashes an entry's drawings go by (the record's own and each size's): REDRAW's and HELDRECHECK's (the page's Pool.forgetDrawing and drawingHashes; holdStamp below). */
    const entryHashes = e => (e ? [e.charmHash].concat(Object.values(e.sizes || {}).map(z => z && z.charmHash)) : []).filter(Boolean).map(String);
    async function loadMasterCharm(entry, size) {
      const geom = sizeEntry(entry, size); if (!geom || !geom.aiPath) throw new Error(`no design file for ${entry.sku}${size ? " · " + size : ""}`);
      const key = geom.aiPath;
      if (B.pool.sources.has(key)) { const src = B.pool.sources.get(key); src.usedAt = Date.now(); return src; }   // (Upkeep lets idle ones go)
      { const kept = await H.keptMaster(key, geom.scaled ? geom.charmHash : entry.charmHash || geom.charmHash); if (kept) return kept; }   // (LOCAL: read before on this computer)
      const {url,bytes,parsed,g,charm}=await readMasterCharm(entry,size);
      // A design file indexed while the grouping lost track of the labels (22–23 Sep) kept the SKU written under its charm.
      // The text stays part of the charm, so the nest keeps its room and no neighbour is placed under it, but it still goes
      // onto the sheet and stops the sheet's .dxf. Indexing the master again writes the file without it.
      const label = charm.members.find(m => m.kind === "text" && m.bbox && m.bbox[3] <= charm.outline.bbox[1] + 1 && P.parseSkuLabel(m.str, Master.skuRegex()));
      if (label) agent({ pool: true }, "warn", `${entry.sku}: its design file still has the SKU label “${String(label.str).trim()}” under the charm, so the label goes onto every sheet with it and the sheet's .dxf fails — index its master again in the Library with “rebuild every charm” chosen under SKUs already held`);
      await P.buildSilhouettes(parsed, [charm], +S.settings.silhouetteRes || 6);
      const hash = geom.scaled ? charm.hash : entry.charmHash || charm.hash;
      let srcId = "pool:" + key.replace(/[^\w]+/g, "_");
      // (REDRAW: the same path read again as ANOTHER drawing keeps the old design under its id, for the sheets that hold pieces of it: the new one takes an id of its own)
      { const had = S.poolSources && S.poolSources[srcId]; if (had && had.hash && hash && String(had.hash) !== String(hash)) srcId += "_" + String(hash).replace(/[^\w]+/g, "").slice(0, 12); }
      const src = { id: srcId, pool: true, name: `${entry.sku}${size ? " · " + size : ""} (master)`, sku: entry.sku, bytes, hash, parsed, group: g, charms: [charm], metal: null, state: "ready", t0: H.now(), cloud: { path: geom.aiPath, url }, persisting: null, usedAt: Date.now() };
      Object.assign(charm, { id: srcId + ":0", sourceId: srcId, sourceName: src.name, index: 0, name: entry.sku, sku: entry.sku, namedBy: "master", excluded: false, cloud: { ai: url, aiPath: geom.aiPath, png: geom.thumbUrl || null, pngPath: geom.thumbPath || null }, upAngle: entry.upAngle, engravable: true, backKeepOut: Master.keepOutOf(charm), frontKeepOut: Master.keepOutOf(charm, "front") });
      // a design that draws two DIFFERENT bodies under one label (a mismatched pair, charm-nest-pair.js) also gets one charm per body
      // which way the drawing faces, when a person set it on the master record (charm-nest-pair.js facingOf reads these two): decides which piece of a pair is the mirror image
      { const f = geom.facing || entry.facing; if (f === "L" || f === "R" || f === "X") charm.facing = f;   /* ("X": the old "reads one way" word; it no longer stops a Right being turned over) */ if (Array.isArray(entry.facings)) charm.facings = entry.facings.slice(0, 10).map(x => (x === "L" || x === "R" ? x : null)); }
      await splitBodies(src);
      S.poolSources[srcId] = src;
      // (REDRAW: a read that began before the library re-pointed the SKU to another drawing is not kept as the design of that path)
      { const cur = Master.entryFor(entry.sku), cg = cur && cur !== entry ? sizeEntry(cur, size) : null; if (!(cg && cg.charmHash && geom.charmHash && String(cg.charmHash) !== String(geom.charmHash))) { B.pool.sources.set(key, src); H.keepMaster(key, src); } }
      return src;
    }
    /* Mismatched pair (Paul, 9 Oct 2026): src.charms stays the ONE folded charm it always was (every picture of the design, the Master tab and
       every old record read that), and src.bodies = [left, right] are two derived charms, each with its own outline, members, silhouette and
       hash, so the nester can place two different charms. A design whose two bodies share one drawing group of the master cannot be written
       body by body (the sheet writer copies whole top-level groups), so it stays one glued piece. Bodies are never stored in Firestore: they are
       made from the folded charm and the traced source (`src.parsed`), here and again for a source recovered from a checkpoint (ensureBase). */
    const pending = new WeakMap();   // src → { key: promise }: two lines of one design made up side by side trace its bodies once (a promise is never kept on the source itself: the checkpoint copies sources)
    const once = (src, key, make) => { const m = pending.get(src) || pending.set(src, {}).get(src); return m[key] || (m[key] = make().finally(() => { delete m[key]; })); };
    const splitBodies = src => src.bodiesChecked ? Promise.resolve(src.bodies) : once(src, "bodies", () => traceBodies(src));
    async function traceBodies(src) {
      src.bodies = null;
      try {
        const Pair = win.CharmNestPair, charm = src.charms[0];
        if (!Pair || !Pair.bodiesOf || !Pair.isMismatched || !Pair.isMismatched(charm)) return null;
        const bodies = Pair.bodiesOf(charm); if (bodies.length !== 2) return null;
        const tops = bodies.map(b => new Set(b.members.map(m => (m.parent != null ? m.parent : m.index)).filter(t => t != null)));
        if ([...tops[0]].some(t => tops[1].has(t))) { agent({ pool: true }, "warn", `${src.sku}: its two bodies are drawn in one group of the master, so each cannot be written on its own — kept as one glued piece`); return null; }
        const made = bodies.map((b, i) => Object.assign({}, charm, { outline: b.outline, members: b.members.slice(), bbox: b.bbox.slice(), extras: (charm.extras || []).filter(x => b.members.includes(x)), topIndices: [...tops[i]], id: `${src.id}:b${i}`, bodyIndex: i, side: Pair.sideOf(i, 2) }));
        delete made[0].frontAt; delete made[1].frontAt;
        await P.buildSilhouettes(src.parsed, made, +S.settings.silhouetteRes || 6);
        for (const c of made) { c.backKeepOut = Master.keepOutOf(c); c.frontKeepOut = Master.keepOutOf(c, "front"); }
        if (made[0].hash === made[1].hash) throw new Error("both bodies trace to one shape");
        return (src.bodies = made);
      } catch (e) { src.bodies = null; agent({ pool: true }, "warn", `${src.sku}: its two bodies could not be told apart (${e.message}) — kept as one piece`); return null; }
      finally { src.bodiesChecked = true; }
    }
    /* ONE charm of a design that draws the same outline twice, on a line that is not an earring pair (Paul, 11 Oct 2026; charm-nest-pair.js section 4c: soloOf). src.solo = one one-body charm per body of the
       design (its own outline, members, tops, silhouette and hash), made the way traceBodies makes the bodies of a pair, except that two bodies of the SAME shape are fine here: that is the very case (traceBodies
       refuses them). The line is cut from solo[index] only. Nothing new is stored in Firestore: the piece's stored charm hash (the pool row's charmHash, the sheet record's charm hash) is a body's, not the folded
       charm's, and soloBase below reads that back. A line pooled before this holds the folded charm's hash and stays whole. A design whose bodies share a group of the master cannot be written body by body
       (the sheet writer copies whole groups): src.solo stays null and such a line keeps both bodies, as it always did. */
    const soloBodies = src => src.soloChecked ? Promise.resolve(src.solo) : once(src, "solo", () => traceSolo(src));
    async function traceSolo(src) {
      src.solo = null; src.soloWhy = "";
      try {
        const Pair = win.CharmNestPair, charm = src.charms[0];
        // (SOLOWHY, Paul 20 Oct 2026, order 4204452679: why no body could be cut alone is kept on the source, and a line held for it says so: src.soloWhy)
        if (!Pair || !Pair.bodiesOf || !Pair.isMismatched) { src.soloWhy = "the pair code did not load"; return null; }
        if (!Pair.isMismatched(charm)) { src.soloWhy = `the library's record of ${src.sku} is not a two-body (Left + Right) design${charm && charm.pair ? "" : " (it carries no pair field: index the master file again)"}`; return null; }
        const bodies = Pair.bodiesOf(charm); if (bodies.length < 2) { src.soloWhy = `the drawing in the library's file gives ${bodies.length} body`; return null; }
        const tops = bodies.map(b => new Set(b.members.map(m => (m.parent != null ? m.parent : m.index)).filter(t => t != null)));
        if (tops.some((t, i) => tops.some((u, j) => j > i && [...t].some(x => u.has(x))))) { src.soloWhy = "its bodies are drawn in one group of the library's file, so one cannot be written alone"; agent({ pool: true }, "warn", `${src.sku}: its bodies are drawn in one group of the master, so one cannot be written alone — a piece of it keeps both bodies`); return null; }
        const made = bodies.map((b, i) => Object.assign({}, charm, { outline: b.outline, members: b.members.slice(), bbox: b.bbox.slice(), extras: (charm.extras || []).filter(x => b.members.includes(x)), topIndices: [...tops[i]], id: `${src.id}:s${i}`, oneBody: i }));
        for (const c of made) delete c.frontAt;
        await P.buildSilhouettes(src.parsed, made, +S.settings.silhouetteRes || 6);
        for (const c of made) { c.backKeepOut = Master.keepOutOf(c); c.frontKeepOut = Master.keepOutOf(c, "front"); }
        // (a design a person named as ONE body, charm-nest-pair.js 4d: the record's up angle is the other body's hole, 97 degrees for the wolf's moon; the named body keeps the up angle it is drawn with)
        const nm = Pair.onlyBodyOf && Pair.onlyBodyOf(charm.sku || src.sku); if (nm && made[nm.index] && Number.isFinite(nm.upAngle)) made[nm.index].upAngle = nm.upAngle;
        return (src.solo = made);
      } catch (e) { src.solo = null; src.soloWhy = `one of its bodies could not be traced (${e.message})`; agent({ pool: true }, "warn", `${src.sku}: one of its bodies could not be cut alone (${e.message}) — a piece of it keeps both bodies`); return null; }
      finally { src.soloChecked = true; }
    }
    /** The charm a piece with no side is cut from, when its stored record says it is ONE body of the design: `rec` is the pool row or the sheet record's charm. null when it is not (an old record,
     *  a line that keeps both bodies, an earring pair): the design's one charm, whole, as before.
     *  BODYID (cloud plan v2, step 3; Paul, 20 Nov 2026: the cloud's outlines may differ from Chrome's by up to 0.06 mm, so a silhouette's hash is never how the cloud knows a design): the body is
     *  read from what the record SAYS (bodyOf: oneBody and bodyIndex, on the record, on its pool row, or, in the cloud, from the line's own reading), never from a hash, wherever that is said. Only a
     *  record that says nothing, on a page, is still matched by its charm hash (the page's own hash against the page's own bodies: today's rule). In the cloud such a record is refused in words,
     *  never cut from a guessed body. */
    async function soloBase(src, rec) {
      const said = bodyOf(rec, true);
      if (said) { if (!said.oneBody) return null; const bodies = await soloBodies(src); return (bodies && bodies[said.bodyIndex]) || null; }
      const h = rec ? String(rec.charmHash || rec.hash || "").replace(/~ae[0-9a-z]+$/, "") : ""; if (!h || h === String(src.charms[0].hash || "")) return null;   // (ART EDIT: a piece whose own artwork was edited has "~ae…" after the hash it was cut from, charm-nest-art-edit.js)
      if (H.cloud) throw Object.assign(new Error(`${src.sku || "a design"}${rec && rec.poolId ? " · " + rec.poolId : ""}: which body of the design this piece is cut from cannot be told in the cloud (its record says no body, and a silhouette's hash is never read as one): a person checks it`), { bodyUnknown: true });
      const bodies = await soloBodies(src); return bodies && bodies.find(c => String(c.hash) === h) || null;
    }
    /** BODYID: which body of its design a stored piece (a pool row, a sheet record's charm, or a charm) is cut from, from what it says, never from a hash: { oneBody: true, bodyIndex } (one body
     *  alone), { oneBody: false } (the whole design; a piece of an earring pair says its body with its side), or null (the record says nothing: a piece a page made before this). A pool row says it
     *  with oneBody (true / false) and bodyIndex; a charm with oneBody as the body's index (traceSolo). A record that says nothing is looked for under its pool id in the pool rows this maker holds;
     *  with `spec`, a node host may then answer from the line's own reading (H.bodyOf: the reading's one body, or the body its listing's profile cuts for that copy). */
    function bodyOf(rec, spec) {
      const said = r => {
        if (!r || typeof r !== "object") return null;
        if (r.oneBody === true && Number.isInteger(+r.bodyIndex) && +r.bodyIndex >= 0 && +r.bodyIndex <= 9) return { oneBody: true, bodyIndex: +r.bodyIndex };
        if (typeof r.oneBody === "number" && Number.isInteger(r.oneBody) && r.oneBody >= 0 && r.oneBody <= 9) return { oneBody: true, bodyIndex: r.oneBody };
        if (r.oneBody === false) return { oneBody: false };
        return null;
      };
      const own = said(rec); if (own) return own;
      const row = rec && rec.poolId != null && B && B.pool && B.pool.rows ? B.pool.rows.get(String(rec.poolId)) : null;
      const its = row && row !== rec ? said(row) : null; if (its) return its;
      if (spec && typeof H.bodyOf === "function") { try { const b = H.bodyOf(row || rec); if (b) return b; } catch (_) { /* nothing known */ } }
      return null;
    }
    /** The factor that makes the drawing in these bytes exactly `wantMm` across: the index's sizes carry a margin and a jump ring, so the file itself is measured (its outline's narrow side is the disc's diameter). `guess` (the factor the
     *  index gave) is only used when the file cannot be measured. Refuses a drawing that is not a round disc or is more than 2.5 times off, so a wrong index never cuts a wrong disc. */
    async function fitFactor(bytes, sku, wantMm, guess) {
      if (!(wantMm > 0)) throw new Error(`${sku}: the size asked is not a diameter`);
      const parsed = await P.parseSource(bytes, `${sku}.ai`), g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
      const big = g.charms.reduce((a, b) => (!a || (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a), null);
      const ob = big && (big.outline && big.outline.bbox || big.bbox); if (!ob) return guess;
      const w = ob[2] - ob[0], h = ob[3] - ob[1], lo = Math.min(w, h), hi = Math.max(w, h);
      if (!(lo > 0) || hi / lo > 1.4) throw new Error(`${sku}: its drawing is not a round disc, so it is not resized to ${wantMm} mm`);
      const k = wantMm / (lo * 25.4 / 72);
      if (k > 2.5 || k < 0.4) throw new Error(`${sku}: ${wantMm} mm asked, its drawing is ${(lo * 25.4 / 72).toFixed(1)} mm`);
      return k;
    }
    /** The bytes of a design file as the design a path names draws it: `<file>#fit-12MM@0.857` is the file at the diameter the size says (the factor measured on the file itself), any other path the bytes as they are.
     *  A saved sheet keeps the library's file under such a path (its link is the file's), so whatever reads it back for a sheet restores the disc at the size it was cut. */
    async function fitBytes(bytes, path, sku) {
      const m = FIT_PATH.exec(String(path || "")); if (!m) return bytes;
      return P.scaleSource(bytes, await fitFactor(bytes, sku, O.sizeMmOf(m[1]), +m[2]));
    }
    async function readMasterCharm(entry,size) {
      const geom=sizeEntry(entry,size), fitP=FIT_PATH.exec(String(geom.aiPath||"")), real=geom.base||geom, filePath=fitP?String(geom.aiPath).slice(0,fitP.index):geom.aiPath;   // (a scaled design is read from the file it is scaled from)
      // (a stored download link dies when the file under it is written again, and a record that shares the file keeps the old one: the file is asked for again through a fresh link before the design is given up)
      const fresh = async () => (await api("charmNestOutput", { op: "url", path: filePath })).url;
      let url = real.aiUrl, bytes;
      if (url) { try { bytes = await H.CharmNestAssets().bytes(url); } catch (e) { url = await fresh(); bytes = await H.CharmNestAssets().bytes(url); real.aiUrl = url; } } else { url = await fresh(); bytes = await H.CharmNestAssets().bytes(url); }
      if (fitP) bytes = await fitBytes(bytes, geom.aiPath, entry.sku);   // (SIZEFIT: the same drawing at the diameter the buyer chose; the reader below sees an ordinary file of that size)
      const parsed = await P.parseSource(bytes, `${entry.sku}.ai`);
      const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
      if (!g.charms.length) throw new Error(`${entry.sku}: no outline in the master copy`);
      const charm = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
      if (g.charms.length > 1) { for (const c of g.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.topIndices = [...new Set(charm.topIndices.concat(c.topIndices))]; charm.bbox = [Math.min(charm.bbox[0], c.bbox[0]), Math.min(charm.bbox[1], c.bbox[1]), Math.max(charm.bbox[2], c.bbox[2]), Math.max(charm.bbox[3], c.bbox[3])]; /* the silhouette canvas is cut to the bbox: a merged piece outside it would be drawn but never collide */ } agent({ pool: true }, "warn", `${entry.sku}: the master copy split into ${g.charms.length} pieces — folded back into one charm`); }
      // The sheet draws this charm's whole drawing, cut to its box, so whatever ink the grouping left loose inside that
      // drawing and within reach of the box is part of the charm too: the nest must pack around everything the sheet shows.
      for (let grew = true; grew;) {
        grew = false;
        for (const m of g.orphans) {
          if (!m.bbox || charm.members.includes(m) || !charm.topIndices.includes(m.parent ?? m.index)) continue;
          const pad = Math.max(charm.strokePt || 0.5, m.lwPt || 0) / 2 + 1, b = charm.bbox;
          if (m.bbox[2] < b[0] - pad || m.bbox[0] > b[2] + pad || m.bbox[3] < b[1] - pad || m.bbox[1] > b[3] + pad) continue;
          charm.members.push(m); charm.bbox = [Math.min(b[0], m.bbox[0]), Math.min(b[1], m.bbox[1]), Math.max(b[2], m.bbox[2]), Math.max(b[3], m.bbox[3])]; grew = true;
        }
      }
      { const r = P.integrateRings(charm); if (r.left.length) throw new Error(`${entry.sku}: a hoop could not join its charm — ${r.left[0]}`); }
      return {url,bytes,parsed,g,charm};
    }
    function cloneCharm(c, id) { const k = Object.assign({}, c, { id, pinned: null }); delete k.frontAt; delete k.manual; return k; }   // (a copy never carries another order's place in the queue, nor a size a person gave another piece by hand: Manual Control)
    /* (RELEASE, Paul 19 Oct 2026: "the user technically doesn't have to do anything to release this order") A line a person released anyway (row.releaseAnyway: Release Hold on a line that still asked something) is cut as it reads when it has a
       design; when it has NOTHING TO PLACE (no design in any master file, "No vector available", no outline, no metal, no such size, too big: releaseAway) it is "noDesign" with no pieces, as a chain only line is: released, nothing is made up for it
       and nothing is nested, its QR label and its completion go on, and no question is raised again. The reason says who released it and why nothing is placed. */
    async function makePool(row, run) {
      const ra = row.releaseAnyway, bare = !!ra && !row.hold && !(row.poolIds || []).length && !!row.spec && !row.spec.noDesign;
      const nothing = why => { row.state = "noDesign"; row.problems = []; row.reason = `Released anyway${ra.by ? " by " + ra.by : ""}: ${String(why).replace(/[.\s]+$/, "")}, so nothing is placed for it`; return null; };
      const none = bare ? H.Orders().releaseAway(row) : ""; if (none) return nothing(none);
      designRead.delete(row);
      const prep = await makePoolRead(row, run);
      stampDesignHold(row, prep);
      if (!prep && bare && ["held", "unmatched", "oversize"].includes(row.state)) return nothing(row.reason || "it cannot be placed");
      return prep;
    }
    /* HELDRECHECK (Paul, 19 Oct 2026, order 4204452679): a line held by the SYSTEM for its DESIGN (read after its library entry was found: the entry blocked, no such size, a design that would not load, two bodies where one is
       cut, fewer bodies than its listing's profile cuts (bodyPlanFor), no outline, too big for the plate, a Right that could not be mirrored) carries the version of each entry it was held against: row.designHold = { skus,
       hashes: { SKU: [the entry's charm hashes, whole and of each size] }, charmHash, indexedAt, masterHash, at }. Review.heldRecheck reads it: when the library draws such a SKU differently (REDRAW's detection, on
       this page or in a cloud read, or a page loaded after the change with the hold restored from its checkpoint) the line is checked again against the new drawing, without a person. Gone when the line is made up,
       or held for anything else (a question, a person). */
    const designRead = new WeakMap();
    const holdStamp = es => { const sk = es.map(e => String(e.sku || "").trim().toUpperCase()); return { skus: sk, hashes: Object.fromEntries(es.map((e, i) => [sk[i], entryHashes(e)])), charmHash: es[0].charmHash || null, indexedAt: +es[0].indexedAtMs || +es[0].indexedAt || +es[0].updatedAt || 0, masterHash: es[0].masterHash || null, at: Date.now() }; };
    function stampDesignHold(row, prep) {
      const es = designRead.get(row);
      if (!prep && es && es.length && !row.hold && ["held", "oversize"].includes(row.state)) row.designHold = holdStamp(es); else delete row.designHold;
    }
    async function makePoolRead(row, run) {
      const sp = row.spec;
      // a custom line whose QR label is being printed (Custom Orders) waits: printed, it is completed by hand, not cut
      if (win.CustomPrint && H.CustomPrint().printing(row.key)) return null;
      // a custom order sent from its card is cut from its own designs, each on the metal picked for it (CustomSheet)
      /* KEPTWINS (Paul, 16 Nov 2026: "now that the charm repository has correctly updated this needs to go on a sheet"): the card's own design was saved to the charm repository and the listing
         linked to it (Save to the charm repository and link this listing), so the line now reads as that repository charm: it is made from the repository, as every other order of the listing
         is (a pair as its two ears), not from the card's file a second way. Only when the line reads clean from a saved link AND that repository charm was made from a file this card sent. */
      const keptFromCard = () => { try { if (!sp || !sp.designSku || sp.noDesign || sp.ownDesign || (sp.problems || []).length) return false;   /* (17 Nov 2026: the SKU itself can be given to the saved design, "SKU CHEETAH taken from ... and given to this order's design": then the line reads it directly, not through a saved link) */ const en = Master.entryFor(sp.designSku), e = H.CustomSheet().sentOf(row); if (!en || !e || !/^charmnest\/master\/custom\//.test(String(en.masterPath || en.aiPath || ""))) return false; const nm = String(en.masterName || "").trim().toLowerCase(); return !!nm && (e.files || []).some(F => String(F.name || "").trim().toLowerCase() === nm); } catch (_) { return false; } };
      if (sp && !sp.customDone && win.CustomSheet && H.CustomSheet().sentOf(row) && !keptFromCard()) return H.CustomSheet().prepare(row, run);
      if (!sp || sp.noDesign) { row.state = "noDesign"; return null; }
      // an add-on listing's line (specialOf: own) has no master design: the one its SKU happens to match is not its design. It waits, asked nothing, for the order's own designs
      // (Send to Sheet) or a person's Complete Order, and its order waits with it
      if (sp.ownDesign) { row.state = "held"; row.reason = O.specialOf.addOn.wait; return null; }
      row.problems = row.problems.filter(p => !["unmatchedSku", "blockedSku", "missingSize", "oversize"].includes(p.kind));   // re-derived below on every attempt
      if (row.problems.length) { row.state = "held"; row.reason = H.Review().problemText(row.problems[0]); return null; }
      // HUGGIEGUARD: a line the form guard holds (a necklace that read as a huggie design, charm-nest-orders.js section 3b) has no design on the row and is never pooled: it waits for a person with the plain line
      if (sp.formHold) { const hp = (sp.problems || []).find(p => p.formHold); if (hp) row.problems.push(hp); row.state = "unmatched"; row.reason = sp.formHold.reason; return null; }
      let entry = Master.entryFor(sp.designSku) || await Master.fetchEntry(sp.designSku);
      if (!entry) { row.state = "unmatched"; row.reason = "not in any master file"; row.problems.push(Object.assign({ kind: "unmatchedSku", reason: "not in any master file", sku: sp.designSku, listingId: String(row.line.listingId || ""), title: row.line.title }, sp.siblings && sp.siblings.length ? { siblings: sp.siblings } : {})); agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: not in any master file`); return null; }
      const read = [entry]; designRead.set(row, read);   // (HELDRECHECK: what is held from here on is held for a design of the library: the version it was held against is kept on the line, stampDesignHold)
      // BLOCKREL: a block that exists only because the back-view (flip) check threw does not stop cutting (O.entryBlock), and a person's "Release to nesting queue" (row.blockOverride) sets a genuine one aside for this SKU on this line
      { const why = O.blockFor(entry, sp.designSku, row.blockOverride); if (why) { row.state = "held"; row.reason = `SKU blocked: ${why}`; row.problems.push({ kind: "blockedSku", reason: why, sku: sp.designSku }); return null; } }
      { const sh = sizeHold(entry, sp.size, sp.designSku); if (sh) { row.state = "held"; row.reason = sh.reason; row.problems.push(sh.problem); return null; } }
      const src = await masterCharm(entry, sp.size);
      const base = src.charms[0];
      /* A mismatched pair named by TWO master designs (Paul, 10 Oct 2026: a SKU or title that names a Tennis Ball and a Tennis Racket, Left and Right options, a 2-symbols line's two signs;
         spec.pair.members): the Left ear is cut from the first design and the Right ear from the second, each facing its own side. Without this the second design was never cut and the
         line fell to the one glued piece of the first. A line whose second design cannot be had is held, never made as the first alone. */
      // A line pooled before the pair and count rule keeps the pieces it has (its pool rows' own quantity; never added to silently): spec.pieceCount is pinned to
      // them and spec.pair.legacy says so, so piecesFor below gives it no side and no plan. (27 Oct 2026: before the pair's and the set's designs are read: a line whose pieces were
      //  all taken off is unpinned here (KITEPAIR), and a set read while it was still pinned came out as N pieces of the first design)
      H.pinPooled(row);
      const two = memberSkus(sp); let ear2 = null;
      if (two && two.L === sp.designSku) {
        const entry2 = Master.entryFor(two.R) || await Master.fetchEntry(two.R); if (entry2) read.push(entry2);
        const hold = (why, problem) => { row.state = "held"; row.reason = why; if (problem) row.problems.push(problem); agent({ pool: true }, "warn", `${row.order.receiptId} · ${two.R}: ${why}`); return null; };
        if (!entry2) { row.state = "unmatched"; row.reason = "not in any master file"; row.problems.push({ kind: "unmatchedSku", reason: "not in any master file", sku: two.R, listingId: String(row.line.listingId || ""), title: row.line.title }); agent({ pool: true }, "warn", `${row.order.receiptId} · ${two.R}: not in any master file`); return null; }
        { const why2 = O.blockFor(entry2, two.R, row.blockOverride); if (why2) return hold(`SKU blocked: ${why2}`, { kind: "blockedSku", reason: why2, sku: two.R }); }
        { const sh = sizeHold(entry2, sp.size, two.R); if (sh) return hold(sh.reason, sh.problem); }
        const src2 = await masterCharm(entry2, sp.size);
        if (src.bodies || src2.bodies) return hold(`${src.bodies ? two.L : two.R} draws two bodies: it cannot be one ear of a pair of two designs`);
        ear2 = { entry: entry2, src: src2, sku: two.R };
      }
      const earOf = p => (ear2 && p && p.side === "R" ? ear2 : { entry, src, sku: sp.designSku });
      /* A SET OF TWO DESIGNS on a line that is not an earring pair (SIBLINGS, Paul, 12 Oct 2026: a necklace with SCHRODINGER CAT 1 and SCHRODINGER CAT 2, the pair of designs a person saved
         for the listing and SKU, spec.pair.set): two pieces for each unit, the first piece the first design, the second piece the second, alternating; every piece its own charm, never a Left or a
         Right, never mirrored; one group (the line). A design the master cannot give holds the line, as the second design of an earring pair does: never made as the first alone. */
      const setSkus = memberSet(sp); let setDs = null;
      if (setSkus && setSkus[0] === sp.designSku) {
        setDs = [{ entry, src, sku: sp.designSku }];
        for (const k of setSkus.slice(1)) {
          const e2 = Master.entryFor(k) || await Master.fetchEntry(k); if (e2) read.push(e2); const hold = (why, problem) => { row.state = "held"; row.reason = why; if (problem) row.problems.push(problem); agent({ pool: true }, "warn", `${row.order.receiptId} · ${k}: ${why}`); return null; };
          if (!e2) { row.state = "unmatched"; row.reason = "not in any master file"; row.problems.push({ kind: "unmatchedSku", reason: "not in any master file", sku: k, listingId: String(row.line.listingId || ""), title: row.line.title }); agent({ pool: true }, "warn", `${row.order.receiptId} · ${k}: not in any master file`); return null; }
          { const why2 = O.blockFor(e2, k, row.blockOverride); if (why2) return hold(`SKU blocked: ${why2}`, { kind: "blockedSku", reason: why2, sku: k }); }
          { const sh = sizeHold(e2, sp.size, k); if (sh) return hold(sh.reason, sh.problem); }
          const s2 = await masterCharm(e2, sp.size);
          if (s2.bodies) return hold(`${k} draws two bodies: it cannot be one piece of a set of two designs`);
          setDs.push({ entry: e2, src: s2, sku: k });
        }
        if (src.bodies) { row.state = "held"; row.reason = `${sp.designSku} draws two bodies: it cannot be one piece of a set of two designs`; agent({ pool: true }, "warn", `${row.order.receiptId} · ${row.reason}`); return null; }
      }
      /* An earring pair (Paul, 9 Oct 2026, amendment 2: stud, hoop or huggie; matching or mismatched) makes a LEFT and a RIGHT piece per unit, copies
         alternating L, R, all of one group (the line). Each piece carries its side and `mirror` (the Right is the mirror image of the Left, so each
         ear faces its own side); a mismatched pair's two pieces are also cut from two different bodies of the design, each its own charm with its own
         outline. What a line makes is charm-nest-pair.js piecesFor's word. A line an older run made before pieces had a side (and a piece of it is still
         on a sheet) stays as it was until it is taken off. Every other line makes `quantity` pieces from the one charm (or the count the intake set),
         exactly as before. */
      if (sp.pieceNote) agent({ metal: sp.material, pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: ${sp.pieceNote}`);
      const Pair = win.CharmNestPair, wanted = Pair && Pair.piecesFor && PoolPieces && !gluedLine(row) ? Pair.piecesFor(pairLine(row), base) : null;
      // (a plan is the pieces of an earring pair, or ONE piece with its side: a single earring whose line names its ear is cut as that ear, the Right the mirror image by the same facing rule; a single that names no ear has no side and is cut as drawn)
      // (a pair of two designs: each piece is body 0 of ITS OWN design, the Right marked body 1 of the pair, and its direction is that design's own: the Right earring is turned over unless its drawing already faces right)
      const plan = wanted && wanted.length >= 1 && wanted.length <= PoolPieces.MAX_PIECES && wanted.every(p => (p.side === "L" || p.side === "R") && (ear2 ? true : wholeBody(src, +p.bodyIndex))) ? (ear2 ? wanted.map(p => Object.assign({}, p, { bodyIndex: p.side === "R" ? 1 : 0, mirror: earMirror(p, earOf(p), row) })) : wanted) : null;
      // ONE body (Paul, 11 Oct 2026: "This listing is not an earring so it should NOT have duplicate charms"): a line that is not an earring pair, of a design that draws the SAME outline twice, is cut from one body of it
      // (the left one, charm-nest-pair.js soloOf): one piece per unit, no side, nothing mirrored, nothing glued. A line pooled before this (its pool rows hold the folded charm), an old glued line and a design whose
      // two bodies are different charms keep what they made. The piece's stored charm hash is that body's: the only mark it carries (soloBase reads it back).
      // MOONSTARS (Paul, 19 Oct 2026, order 4203628477): a line whose listing's profile says which BODY of a two-body design each piece is (spec.pair.pieceBodies: the stars the left body, the moon the right one,
      // charm-nest-orders.js PIECE_LISTINGS) is cut piece by piece from that body alone (bodyPlanFor); a design whose bodies cannot be cut apart holds the line, it is never made as the whole drawing N times
      const byBody = !plan && !ear2 && !setDs ? await bodyPlanFor(row, src) : null;
      if (byBody && byBody.hold) { row.state = "held"; row.reason = byBody.hold; row.problems.push({ kind: "blockedSku", reason: byBody.hold, sku: sp.designSku, noOutline: true }); agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: ${byBody.hold}`); return null; }
      const one = !plan && !ear2 && !setDs && !byBody ? await soloFor(row, src) : null, cutBase = one ? one.charm : base;
      if (one) O.solo(sp, one); else if (sp.pair && sp.pair.oneBody) O.solo(sp, null);   // (the intake said one body from the master entry; the geometry says it is not: the old words)
      if (!plan && !one && sp.pair && sp.pair.mismatched && !sp.pair.glued) O.glue(sp);   // (a mismatched pair whose two bodies could not be told apart is the one glued piece per unit it always was, never two copies of the folded charm)
      const fixed = Number.isInteger(+sp.pieceCount) && +sp.pieceCount >= 1 && +sp.pieceCount <= PoolPieces.MAX_PIECES ? +sp.pieceCount : 0;   // (the count the intake set for a line that is not an earring pair: charm-nest-pair.js pieceCountOf)
      const count = plan ? plan.length : fixed || sp.quantity;
      /* Only a GROUP carries a group (Paul 9 Oct, after ADVCOMPAT 1 and 2): an earring pair (sided pieces), a necklace of counted discs, letters or charms (n pieces of ONE necklace), a line
         the intake marks as several pieces per unit. Each of its pieces says so with groupSize 2 or more (and the group key); the copies of a plain quantity-N line say nothing, each stands alone. */
      const grouped = !!(Pair && Pair.isGroupLine && Pair.isGroupLine(pairLine(row), base)), loose = grouped && !plan && count >= 2 && count <= PoolPieces.MAX_PIECES;
      // oversize: the charm cannot fit the plate under the ceiling (each body of a pair must fit; a mirror image is as big as the drawing)
      const st = stockFor(sp.material); const usable = (st.wPt - 2 * (+S.settings.insetPt || 0)) * (st.hPt - 2 * (+S.settings.insetPt || 0)) * (+S.settings.maxFill || 0.80);
      for (const b of plan ? (ear2 ? [base, ear2.src.charms[0]] : [...new Set(plan.map(p => +p.bodyIndex))].map(i => wholeBody(src, i))) : setDs ? setDs.map(d => d.src.charms[0]) : byBody ? [...new Set(byBody.charms)] : [cutBase]) {
        // a design whose outline reads as nothing (no area or no size: collapsed, invalid) cannot be seated: first in line of its metal it stopped the whole sheet ("The oldest order cannot fit on an empty sheet"), so the line waits for a person instead
        if (!(b.areaPt2 > 0) || !(b.widthPt > 0) || !(b.heightPt > 0)) { row.state = "held"; row.reason = "SKU blocked: its design has no outline to cut"; row.problems.push({ kind: "blockedSku", reason: "its design has no outline to cut", sku: sp.designSku, noOutline: true }); return null; }   // (noOutline: nothing to cut, so no "Release to nesting queue" can set it aside)
        if (b.areaPt2 > usable || Math.min(b.widthPt, b.heightPt) > Math.max(st.wPt, st.hPt) - 2 * (+S.settings.insetPt || 0)) { row.state = "oversize"; row.reason = `charm ${(b.widthPt * MM).toFixed(1)} × ${(b.heightPt * MM).toFixed(1)} mm does not fit the ${labelOf(sp.material)} plate under the ceiling`; row.problems.push({ kind: "oversize", sku: sp.designSku, widthMm: b.widthPt * MM, heightMm: b.heightPt * MM, material: sp.material }); return null; }
      }
      // a Right piece is drawn the other way round (its mirror image); a line whose mirrored charm cannot be made is held, never drawn the wrong way
      if (plan) { try { for (const pp of plan) await ensureBase(earOf(pp).src, ear2 ? Object.assign({}, pp, { bodyIndex: 0 }) : pp); } catch (e) { row.state = "held"; row.reason = `its right-hand earring could not be mirrored: ${e.message}`; agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: ${row.reason}`); return null; } }
      const pools = [], charms = [];
      for (let copy = 1; copy <= count; copy++) {
        if (H.pace && copy > 1) await H.pace(row, copy, count);   // (a node host: piece by piece, a turn of the event loop between pieces under its time budget, C7; the page passes none and never waits here)
        const poolId = O.poolId(row.order, row.line, copy), pp = plan ? plan[copy - 1] : null;
        // (an ear piece of a line that is not a group, a single earring that names its ear: a group of ONE, however many are bought; discs, letters, charms of a counted necklace: group fields, no ear)
        const mine = pp ? PoolPieces.fieldsOf(grouped ? pp : Object.assign({}, pp, { groupSize: 1 })) : loose ? PoolPieces.cleanGroupFields({ poolId, groupSize: count }) : null;
        const ear = setDs && !pp ? setDs[(copy - 1) % setDs.length] : earOf(pp), cut = ear.src;   // (the ear's own design: the line's one design for every piece but the Right of a pair of two designs; a set of two designs: piece 1 the first design, piece 2 the second, alternating)
        const charm = pp ? cloneCharm(baseFor(cut, ear2 ? Object.assign({}, pp, { bodyIndex: 0 }) : pp), `${cut.id}:${poolId}`) : setDs ? cloneCharm(cut.charms[0], `${cut.id}:${poolId}`) : byBody ? cloneCharm(byBody.charms[copy - 1], `${src.id}:${poolId}`) : one ? cloneCharm(cutBase, `${src.id}:${poolId}`) : cloneCharm(base, `${src.id}:${poolId}`);
        /* (DUPID, Paul 27 Oct 2026, order 4208640203: "the system does not recognize their presence on the sheet". The first copy of a line used to BE the master's own charm, id "<design>:0", while
            nothing marked it taken (base.poolId); a design read again (a reload, a redraw, upkeep) has a fresh master charm, so the next line of it took "<design>:0" too, and the two pieces of two
            orders on one sheet were one placement: the nest placed the id once, the page gave the spot to one piece, the files and the record to the other. Every piece is a copy named by its pool
            id, as every other copy always was; what was made before is mended by Pool's index, CharmNestPoolPieces.uniqueIds) */
        charm.name = `${row.order.receiptId} · ${ear.sku}${count > 1 ? ` · ${copy}/${count}` : ""}`;
        charm.order = row.order.receiptId; charm.orderDate = +row.order.createTs || 0; charm.arrivedAt = row.arrivedAt || 0; charm.orderInfo = { receiptId: row.order.receiptId, transactionId: row.line.transactionId, sku: ear.sku, copy, quantity: count, form: sp.form, size: sp.size };
        charm.poolId = poolId; charm.metal = sp.material; charm.lineKey = row.key; charm.pinned = null; charm.excluded = false; if (+row.frontAt > 0) charm.frontAt = +row.frontAt; else delete charm.frontAt;
        if (mine) Object.assign(charm, mine);   // side, mirror, bodyIndex, groupKey, groupSize: only a piece of an earring pair carries them (a counted necklace's pieces: groupKey and groupSize)
        if (H.cloud) { const cb = cloudBody(mine, byBody, one, copy); charm.oneBody = cb.oneBody ? cb.bodyIndex : false; }   // (BODYID: a node host's piece says which body it is cut from, on the charm too: the body's index, or false for the whole design; a writer that records it lets bodyOf read it back)
        pools.push({ poolId, runId: run ? run.runId : null, setId: run ? run.setId || null : null, sheetId: null, orderId: row.order.receiptId, orderDate: +row.order.createTs || 0, arrivedAt: row.arrivedAt || 0, ...(+row.frontAt > 0 ? { frontAt: +row.frontAt } : {}), transactionId: row.line.transactionId, sku: ear.sku, material: sp.material, size: sp.size || null, form: sp.form || null, chain: sp.chain || null, ...(Array.isArray(sp.optionNotes) && sp.optionNotes.length ? { optionNote: sp.optionNotes.join(" · ").slice(0, 240) } : {}), copy, quantity: count, charmHash: charm.hash, masterHash: ear.entry.masterHash || null, aiPath: sizeEntry(ear.entry, sp.size).aiPath, engrave: !!(row.engrave && row.engrave.needed) && !(byBody && byBody.blank[copy - 1]), state: "ready", lineKey: row.key, updateTs: row.order.updateTs, ...(mine || {}), ...(H.cloud ? cloudBody(mine, byBody, one, copy) : {}) });
        // (ART EDIT, Paul 27 Oct 2026: a line pooled again keeps the pool ids it had; a piece whose own artwork was edited, its pool row says so, is made with that edit: charm-nest-art-edit.js piece)
        const AE = H.ArtEdit(); charms.push(AE !== undefined ? await AE.piece(charm, poolId, B.pool.rows.get(poolId)) : charm);
      }
      return { sp, pools, charms };
    }
    /** The ONE body a line that is not an earring pair is cut from, or null (it keeps what it always made): { index, count, rule, how, charm } (charm-nest-pair.js soloOf says which body and why; the charm is that body
        cut alone). Never for an old glued line, nor for a line a live pool row of which holds the folded charm (pooled before this: it is cut whole as it was). */
    /** MOONSTARS: the charm each piece of a line is cut from when its listing's profile names a body for every piece (CharmNestOrders.pieceBodiesOf: [0, 0, 1] for 2 stars and the moon), else null: { charms: [one body's
        charm for each piece, in order], bodies }. The bodies are the ones traceSolo cuts (each its own outline, members, drawing groups, silhouette and hash; the piece's stored charm hash is its body's, so soloBase reads it back
        on a reload). { hold } when the design cannot give those bodies (one body only, or its bodies drawn in one group of the master): the line waits for a person. Never for an old glued line or one pinned to the pieces it had. */
    async function bodyPlanFor(row, src) {
      const per = O.pieceBodiesOf ? O.pieceBodiesOf(row.spec) : null; if (!per || gluedLine(row)) return null;
      const bodies = await soloBodies(src), need = Math.max(...per) + 1;
      // (BROKENHEART, Paul 19 Oct 2026, order 4204452679: a heart broken in two whose master draws ONE outline with the zigzag as a line inside it gives one body: the line waits, it is never cut as the whole heart twice, nor as one heart)
      const named = row.spec.pair && Array.isArray(row.spec.pair.labels) && row.spec.pair.labels.length === need ? row.spec.pair.labels.map(x => "the " + String(x).toLowerCase()).join(" and ") : "";
      if (!bodies || bodies.length < need) return { hold: `its design ${row.spec.designSku} does not give the ${need} separate bodies this listing's profile cuts (${row.spec.pair && row.spec.pair.companion ? `the ${row.spec.pair.noun || "piece"}s and the ${row.spec.pair.companion.noun}` : named ? `${named}, each its own closed outline` : "one body for each piece"})${src.soloWhy ? ` — ${src.soloWhy}` : bodies ? ` — the drawing gives ${bodies.length}` : ""}: a person checks the design` };
      const pr = row.spec.pair || {}, unit = Array.isArray(pr.pieceBodies) ? pr.pieceBodies.length : per.length, cn = pr.companion ? Math.max(0, Math.floor(+pr.companion.n) || 0) : 0;
      return { charms: per.map(i => bodies[i]), bodies: per.slice(), blank: per.map((_, k) => cn > 0 && (k % unit) >= unit - cn) };   // (blank: the companion, the moon: its pool record says it is not engraved)
    }
    async function soloFor(row, src) {
      const Pair = win.CharmNestPair, whole = src.charms[0]; if (!Pair || !Pair.soloOf || gluedLine(row)) return null;
      // (a live piece of the line cut whole keeps the line whole. BODYID: a record that says so is believed (oneBody false, no ear); only one that says nothing is matched by its hash, as before. In the cloud
      //  the cloud's own rows always say it, and a line whose live rows a page made is never made up again there: the intake keeps those pieces as they are, _charmNestIntake.js)
      if ((row.poolIds || []).some(id => { const r = B.pool.rows.get(id); if (!r || ["abandoned", "superseded"].includes(r.state)) return false; const said = bodyOf(r); if (said) return !said.oneBody && !(r.side === "L" || r.side === "R"); return !H.cloud && !!r.charmHash && String(r.charmHash) === String(whole.hash); })) return null;
      const one = Pair.soloOf(pairLine(row), whole); if (!one) return null;
      const bodies = await soloBodies(src), charm = bodies && bodies[one.index];
      return charm ? Object.assign({}, one, { charm }) : null;
    }
    /** The designs of a SET a person saved for a line that is not an earring pair (spec.pair.set, charm-nest-orders.js; ["SCHRODINGER CAT 1", "SCHRODINGER CAT 2"]), or null: a line that is not one, an earring pair, a glued or an old line. */
    function memberSet(sp) {
      const pr = sp && sp.pair, d = pr && !pr.glued && !pr.legacy && !pr.earring && pr.set && Array.isArray(pr.set.designs) ? pr.set.designs.map(x => String(x || "")) : null;
      return d && d.length >= 2 && d.every(x => x) && new Set(d).size === d.length ? d : null;
    }
    /** The two master designs of a mismatched pair named by two SKUs ({ L, R }), or null: a line made of one design, a mismatched design of two bodies, a glued or an old line. */
    function memberSkus(sp) {
      const pr = sp && sp.pair, m = pr && pr.mismatched && !pr.glued && !pr.legacy && Array.isArray(pr.members) ? pr.members : null;
      const l = m && m.find(x => x && x.side === "L"), r = m && m.find(x => x && x.side === "R");
      return l && r && l.sku && r.sku && String(l.sku) !== String(r.sku) ? { L: String(l.sku), R: String(r.sku) } : null;
    }
    /** Is this ear (its own design) cut turned over? The same word charm-nest-pair.js gives a matching pair of that design: the Left as drawn and the Right turned over, unless the drawing faces the other way (a person's facing; letters and numbers are turned over like any other design). */
    function earMirror(piece, ear, row) {
      const Pair = win.CharmNestPair, c = ear.src.charms[0];
      const line = Object.assign({}, row.line, { receiptId: row.order.receiptId, transactionId: row.line.transactionId, quantity: 1, form: row.spec.form, spec: { quantity: 1, form: row.spec.form, pieceCount: 2, pair: { earring: true, mismatched: false, glued: false, perUnit: 2, sides: ["L", "R"] } } });
      const mine = Pair.piecesFor(line, c).find(x => x.side === piece.side);
      return !!(mine && mine.mirror);
    }
    /** The order line as charm-nest-pair.js reads it (receipt, transaction, quantity, form, the listing's own fields and the spec). */
    const pairLine = row => Object.assign({}, row.line, { receiptId: row.order.receiptId, transactionId: row.line.transactionId, quantity: row.spec.quantity, form: row.spec.form, spec: row.spec });
    /** A piece that carries a side and a body: a piece of an earring pair (matching or mismatched). Any other piece is cut from the design's one charm. */
    const sidedPiece = piece => !!(piece && (piece.side === "L" || piece.side === "R") && Number.isInteger(+piece.bodyIndex));
    /** The design's charm for one body as drawn: a mismatched pair's own body, or the one charm for body 0 of any other design; null when the design has no such body. */
    const wholeBody = (src, i) => (src.bodies ? src.bodies[i] || null : i === 0 ? src.charms[0] : null);
    /** The charm one piece is cut from, as made so far: the design's one charm for an ordinary piece; for a piece of an earring pair its own body
        (a mismatched pair) as drawn, or, when the piece is the MIRROR IMAGE of the drawing (piece.mirror: a Right when the master faces left),
        the mirrored charm ensureBase made. null when the design has no such body or the mirrored charm is not made yet: the caller never
        draws a sided piece from the wrong body or the wrong way round. */
    function baseFor(src, piece) {
      if (!sidedPiece(piece)) return src.charms[0];
      const i = +piece.bodyIndex, whole = wholeBody(src, i);
      if (!whole) return null;
      return piece.mirror === true ? (src.mirrors && src.mirrors[i]) || null : whole;
    }
    /** baseFor, making what it needs: the bodies of a recovered source, and the mirrored charm (CharmNestPair.pieceGeometry: the outline and art
        reflected about the body's own vertical axis, its silhouette traced again) once per body and source. Throws when a mirrored charm cannot be made. */
    async function ensureBase(src, piece, rec) {
      if (!sidedPiece(piece)) return (rec && await soloBase(src, rec)) || src.charms[0];   // (`rec`, the piece's stored record, may say it is ONE body of the design: soloBase)
      await splitBodies(src);
      // (TAKEOVERSAFE2, Paul 14 Nov 2026: "Yes, fix resume first"): an ear of a pair of TWO designs (a Tennis Ball Left, a Tennis Racket Right) is cut from its own design's one body, body 0, as the pool cuts it (ear2 above); its records keep the pair's numbering (the Right is body 1),
      //  so a sheet rebuilt from the cloud on another computer left that ear out and was nested again from scratch
      if (+piece.bodyIndex > 0 && !src.bodies && (src.charms || []).length === 1 && !wholeBody(src, +piece.bodyIndex)) piece = Object.assign({}, piece, { bodyIndex: 0 });
      const i = +piece.bodyIndex, whole = wholeBody(src, i);
      if (!whole || piece.mirror !== true || (src.mirrors && src.mirrors[i])) return baseFor(src, piece);
      return once(src, "mirror" + i, async () => {
      if (src.mirrors && src.mirrors[i]) return src.mirrors[i];
      const Pair = win.CharmNestPair, geo = Pair && (Pair.pieceGeometry ? Pair.pieceGeometry(whole, { side: piece.side, bodyIndex: i, mirror: true }) : Pair.mirrorOf ? Pair.mirrorOf(whole) : null);
      if (!geo) throw new Error("the mirrored (right-hand) charm cannot be made: charm-nest-pair.js has no pieceGeometry");
      const made = Object.assign({}, whole, geo, { id: `${src.id}:${i}m`, bodyIndex: i, mirror: true });
      delete made.frontAt; made.pinned = null;
      await P.buildSilhouettes(src.parsed, [made], +S.settings.silhouetteRes || 6);
      made.backKeepOut = Master.keepOutOf(made); made.frontKeepOut = Master.keepOutOf(made, "front");
      (src.mirrors || (src.mirrors = {}))[i] = made;
      return made;
      });
    }
    /** An older line made before pieces carried a side (as `quantity` pieces from one charm, or a mismatched design as ONE glued piece), and a piece
        of it still on a sheet or on this page's sheets: it stays as it was until it is taken off. Its records are never rewritten. */
    function gluedLine(row) {
      if (row.glued) return true;
      const ids = row.poolIds || []; if (!ids.length) return false;
      return PoolPieces.legacyGlued(ids.map(id => B.pool.rows.get(id) || { poolId: id }), r => !!r.sheetId || !!H.charmOf(r.poolId)) && !ids.some(id => H.charmOf(id)?.side);
    }
    /** The answer poolPut gave for ONE line, read (the first part of the page's attachPool; the cloud's intake reads it the same way). The row is changed as attachPool
     *  always changed it; what the page does next (settle, the log, taking the rows off) is the caller's.
     *  → { kind: "short", had } | { kind: "gone", ids } | { kind: "contended", runId } | { kind: "place", saved: Map(poolId → where the cloud has it), on: Set(poolIds already on a sheet) } */
    function poolAnswer(row, prep, contended, placed, short) {
      const { sp, pools } = prep;
      // the cloud already holds this line with fewer pieces than the rule now gives (it was pooled before the pair and count rule): its
      // rows were not written, and none is added; the line is what it was, with a plain note
      const kept = (short || []).filter(x => pools.some(p => p.poolId === x.poolId));
      if (kept.length) {
        const had = Math.max(...kept.map(x => +x.had || 0));
        O.pinPieces(sp, had); row.poolIds = pools.slice(0, had).map(p => p.poolId); row.state = "pooled"; row.reason = null; delete row.poolTry; delete row.poolError;
        return { kind: "short", had };
      }
      // a line that went (cancelled, AutoCancel, or gone from Etsy) while it was made up never goes onto a sheet: the run's
      // pool step is not waited for by AutoCancel, and it used to place the cancelled order after it had been taken off
      if (row.state === "gone" || H.cancelled(row.order.receiptId)) return { kind: "gone", ids: pools.map(p => p.poolId) };
      const taken = (contended || []).filter(c => pools.some(p => p.poolId === c.poolId));
      if (taken.length) { row.state = "contended"; row.reason = `claimed by run ${taken[0].runId}`; return { kind: "contended", runId: taken[0].runId }; }
      // a line already on a sheet is never placed again (Paul, 29 Sep: one Send to Sheet put an order's own design on its
      // Rose Gold sheet twice): a piece a sheet of this sorter holds, or one the cloud has on a saved sheet (placed), stays
      // where it is, and no second copy of it goes on
      const saved = new Map((placed || []).filter(x => pools.some(p => p.poolId === x.poolId)).map(x => [x.poolId, x]));
      const on = new Set(pools.map(p => p.poolId).filter(id => saved.has(id) || H.charmOf(id)));
      return { kind: "place", saved, on };
    }
    /** (cloud plan v2, step 3) A cloud pool row says which body of its design it is cut from: bodyIndex (a sided piece's own; a piece cut from one body alone, that body;
     *  else 0) and oneBody (cut from one body alone: a listing's profile, bodyPlanFor, or one body of a design drawn twice, soloFor). Only a node host (H.cloud) adds them. */
    const cloudBody = (mine, byBody, one, copy) => ({ bodyIndex: mine && (mine.side === "L" || mine.side === "R") && Number.isInteger(+mine.bodyIndex) ? +mine.bodyIndex : byBody ? +byBody.bodies[copy - 1] || 0 : one ? +one.index || 0 : 0, oneBody: !!(byBody || one) });
    /* A line that could not go on a sheet (its SKU in no master file or blocked, a size its design lacks, too big for the
       plate, a design that would not load) is made up again only when something it depends on has changed: the line as
       read (Etsy or a person), its design in the library, the plate it would go on. One whose design would not load is
       tried again after RETRY_MS. Every update used to make up each such line again, and look up each unknown SKU again,
       so an update took longer the longer the open orders list grew (Paul, 24 Sep: "Why do we have to prepare all the
       orders every single time"). */
    function trySig(row) {
      const sp = row.spec || {}, e = sp.designSku ? Master.entryFor(sp.designSku) : null, st = sp.material ? stockFor(sp.material) : null;
      return JSON.stringify([+row.order.updateTs || 0, sp.designSku || "", sp.size || "", sp.material || "", sp.quantity || 0, !!sp.noDesign, (sp.problems || []).map(p => p.kind), row.materialOverride || "", row.sizeOverride || "", row.blockOverride ? JSON.stringify(row.blockOverride) : "", row.skuOverride ? JSON.stringify(row.skuOverride) : "",
        e ? [e.masterHash || "", e.updatedAt || 0, e.blocked || "", Object.keys(e.sizes || {}).sort().join(","), sizeEntry(e, sp.size)?.aiPath || ""] : B.master.entries.size,
        st ? [Math.round(st.wPt), Math.round(st.hPt)] : 0, +S.settings.insetPt || 0, +S.settings.maxFill || 0]);
    }
    const due = row => !H.greenHeld(row) && (["pulled", "waiting"].includes(row.state) || row.poolTry !== trySig(row) || (row.poolError > 0 && Date.now() - row.poolError >= RETRY_MS));   // (RGC1: a line waiting for an open layout's green line is not made up again until that line is cut: greenHeld)
    return { sizeEntry, sizeKeyOf, sizeHold, masterCharm, entryHashes, loadMasterCharm, readMasterCharm, splitBodies, traceBodies, soloBodies, traceSolo, soloBase, fitFactor, fitBytes,
      cloneCharm, makePool, makePoolRead, stampDesignHold, bodyPlanFor, soloFor, memberSet, memberSkus, earMirror, pairLine, sidedPiece, wholeBody, baseFor, ensureBase, gluedLine,
      poolAnswer, trySig, due, passing: H.cloud ? passingCloud : passing, bodyOf };
  }

  return { create, lineRecord, FIT_PATH, PAGE, PASSING, RETRY_MS, passing, stopAnswer, pace, silhouetteWithin, OUTLINE_TOL_MM, OUTLINE_TOL_PX };
});
