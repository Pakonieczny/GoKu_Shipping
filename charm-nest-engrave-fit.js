/* Initial engraving preparation runs off the UI thread. Geometry is shared
 * with the editor and exporter; this module never approves or saves a job. */
(function(root,factory){
  const api=factory();
  if(typeof module === "object" && module.exports) module.exports=api;
  else root.CharmNestEngraveFit=api;
})(typeof self !== "undefined" ? self : this,function(){
  "use strict";
  function calculate(input,F_,G) {
    const {charm,opts}=input, MM=25.4/72;
    const job={lineInput:input.lines.slice(),lines:input.lines.slice(),lineMode:input.lineMode};
    const fontFor=weight=>(weight === "Semibold" && F_.Semibold) || F_.Regular;
    let view;
    try { view=G.backView(charm,input.viewOptions); }
    catch(e){e.stage="flip";throw e;}
    job.mask=G.engraveMask(view,input.maskOptions);
    job.lineInput ||= job.lines.slice();
    let fit = G.fitMultiline(job.lineInput, F_.Regular, job.mask, opts, job.lineMode || "auto");
    if (fit.ok) {job.lines=fit.lines.slice();job.text=job.lines.join("\n");}
    if (fit.ok && fit.weight === "Semibold" && F_.Semibold) { const sb = G.fitText(job.lines, F_.Semibold, job.mask, opts); if (sb.ok) fit = Object.assign(sb, { weight: "Semibold" }); }
    if (!fit.ok) return {view, mask:job.mask, fit:null, lines:job.lines, reason:fit.reason};
    fit.fittedMax = fit.size; job.fit = fit; job.fitAt = Date.now();
    // the largest that fits is the ceiling; the default is sized to how much there is to say (design §7.4)
    {
      const bw = charm.widthPt || (charm.bbox ? charm.bbox[2] - charm.bbox[0] : 0), bh = charm.heightPt || (charm.bbox ? charm.bbox[3] - charm.bbox[1] : 0);
      const sizeOpts = font => ({
        capPerEm: G.capPerEm(font), minCapMm: opts.minCapMm || 1.6,
        charmMinMm: Math.min(bw, bh) * MM, charmMaxMm: Math.max(bw, bh) * MM,
        usableAreaMm2: G.area(job.mask) / (job.mask.res * job.mask.res) * MM * MM,
        advanceOf: t => font.getAdvanceWidth(t, 1, { kerning: true })
      });
      let want = G.defaultSize(job.lines, fit.fittedMax, sizeOpts(fontFor(fit.weight)));
      // the weight follows the size that will actually be cut, not the ceiling: a name that came down to 2.0 mm is
      // Semibold even though the largest that fitted was 3.7 mm. Advances differ by ~3%, so one re-fit settles it.
      const semiBelow = opts.semiboldBelowMm || 2.2;
      const wantWeight = want * G.capPerEm(fontFor(fit.weight)) * MM < semiBelow ? "Semibold" : "Regular";
      if (wantWeight !== fit.weight && F_[wantWeight]) {
        const re = G.fitText(job.lines, F_[wantWeight], job.mask, opts);
        if (re.ok) { re.fittedMax = re.size; fit = Object.assign(re, { weight: wantWeight }); job.fit = fit; want = G.defaultSize(job.lines, fit.fittedMax, sizeOpts(fontFor(fit.weight))); }
      }
      if (want < fit.size - 0.01) {
        const L = G.layoutLines(job.lines, fontFor(fit.weight), want, opts.lineGap, fit.angle, fit.centre);
        if (G.verifyInk(L.cmds, job.mask).ok) {
          const capMm = want * G.capPerEm(fontFor(fit.weight)) * MM;
          job.fit = fit = Object.assign({}, fit, { size: want, capMm, layout: L, glyphs: L.glyphs, cmds: L.cmds, fittedMax: fit.fittedMax, sized: "default" });
        }
      }

    }
    // The final default size and the display line count are one decision.
    const flow=G.reflowAt(job.lineInput,fontFor(fit.weight),job.mask,opts,{centre:fit.centre,angle:fit.angle,size:fit.size},job.lineMode || "auto");
    if(flow.ok){fit=job.fit=Object.assign({},fit,flow,{weight:fit.weight});job.lines=flow.lines.slice();job.text=job.lines.join("\n");}
    job.wantSize=fit.size;
    // a font with ONE weight (a script) has no Semibold cut: the Regular is what is cut at every size, and the record says so
    if(fit.weight === "Semibold" && !F_.Semibold)fit=job.fit=Object.assign({},fit,{weight:"Regular",weightNote:"this font has one weight"});
    const check = G.verifyInk(fit.cmds, job.mask);                          // 7.4 · geometry: zero ink outside the eroded mask, zero in any hole
    if (!check.ok) { throw new Error(`ink outside the eroded mask after fitting (${check.outside} px) — a bug, not a review item`); }
    return {view,mask:job.mask,fit,lines:job.lines,check};
  }

  /* QUICK PLACE (moved here from charm-nest-engrave-preview.js quickRun, 28 Oct 2026, REFIT: Paul, "Everything must be immediate and responsive"). Words with no job fit to read (no job yet,
     or a face waiting for words) are placed on the card's pictures inside the engravable area by the same geometry the fit uses, cut short: the charm turned (backView), the engravable area
     (engraveMask), the largest empty rectangle of it at a few turns as the place, refitAt there, the shop's size rule (defaultSize) and the line breaks (reflowAt). It ran on the page's own
     thread and took 0.4 s on a calendar; it now runs in this worker (input.kind "quick"), and the page runs this very function only when it has no worker. Nothing is saved.
     input = { charm, face, lines, viewOptions, maskOptions, opts, mask? (a mask the job already has) } -> { status: 'ready', fit: { lines, centre, angle, size }, rect } | { status: 'none', why, rect? } */
  const QUICK_CANDS = typeof WeakMap === "function" ? new WeakMap() : null;
  function quickPlace(input, F_, G) {
    const font = F_.Regular, opts = input.opts, charm = input.charm, lines = input.lines || [];
    let mask = input.mask || null;
    if (!mask) {
      try { mask = G.engraveMask(G.backView(charm, input.viewOptions), input.maskOptions); }
      catch (e) { return { status: "none", why: "view: " + String(e && e.message || "") }; }
    }
    try {
      if (!G.glyphCoverage(font, lines.join("\n")).ok) return { status: "none", why: "glyphs" };
      // the places to try, once per eroded mask (the words do not change them): the largest empty rectangle of the area at a few turns, as fitText finds them (a turn is searched on the mask turned the other way)
      let cands = QUICK_CANDS && QUICK_CANDS.get(mask);
      if (!cands) {
        cands = [];
        for (const angle of [0, 15, -15, 45, -45, 90, -90]) {
          const mA = angle ? G.rotateMask(mask, -angle) : mask, r = G.largestRectangles(mA, 1)[0]; if (!r) continue;
          const cA = [(r.x0 + r.x1) / 2, (r.y0 + r.y1) / 2];
          cands.push({ angle, centre: angle ? G.ap(G.rotateAbout(mask.cx, mask.cy, angle), cA[0], cA[1]) : cA, hPt: r.hPt, wPt: r.wPt });
        }
        if (QUICK_CANDS) QUICK_CANDS.set(mask, cands);
      }
      if (!cands.length) return { status: "none", why: "no area" };
      const rect = { centre: cands[0].centre, hPt: cands[0].hPt, wPt: cands[0].wPt };
      // the page's own order of work (fitMultiline, then calculate): every way of breaking the lines is tried in turn, a turned placement is taken only when it is clearly larger (12 %, as fitText), a new
      // line break only when it is clearly larger (5 %), the size is the shop's rule for how much there is to say, and the lines are laid again at that size and place (reflowAt)
      const MMPT = 25.4 / 72, bw = charm.widthPt || (charm.bbox ? charm.bbox[2] - charm.bbox[0] : 0), bh = charm.heightPt || (charm.bbox ? charm.bbox[3] - charm.bbox[1] : 0);
      const sizeOpts = { capPerEm: G.capPerEm(font), minCapMm: opts.minCapMm, charmMinMm: Math.min(bw, bh) * MMPT, charmMaxMm: Math.max(bw, bh) * MMPT, usableAreaMm2: G.area(mask) / (mask.res * mask.res) * MMPT * MMPT, advanceOf: t => font.getAdvanceWidth(t, 1, { kerning: true }) };
      const variants = G.flowVariants(lines, font, "auto"); if (!variants.length) return { status: "none", why: "no text", rect };
      const maxH = opts.maxHeightFrac * (mask.hPt || mask.h / mask.res), want0 = G.defaultSize(variants[0], maxH, sizeOpts);
      const bestOf = ls => { let b0 = null, bR = null; for (const c of cands) { const f = G.refitAt(ls, font, mask, opts, { centre: c.centre, angle: c.angle, maxSize: want0 }); if (!f.ok) continue; if (c.angle === 0) b0 = f; else if (!bR || f.size > bR.size) bR = f; } return bR && (!b0 || (bR.size - b0.size) / b0.size >= 0.12) ? bR : b0; };
      let first = null;
      for (const v of variants) { const f = bestOf(v); if (f && (!first || f.size > first.size * 1.05)) first = Object.assign({ lines: v }, f); if (f && f.size >= want0 - 0.01) break; }
      if (!first) return { status: "none", why: "no room", rect };
      const want = G.defaultSize(first.lines, first.size, sizeOpts);
      const fit = G.reflowAt(lines, font, mask, opts, { centre: first.centre, angle: first.angle, size: want }, "auto");
      return { status: "ready", rect, fit: fit && fit.ok ? { lines: fit.lines.slice(), centre: fit.centre, angle: fit.angle || 0, size: fit.size } : { lines: lines.slice(), centre: first.centre, angle: first.angle, size: first.size } };
    } catch (e) { return { status: "none", why: "fit: " + String(e && e.message || "") }; }
  }

  // One worker, one active calculation. Queued inputs stay on the caller's
  // side; they are not cloned into dozens of workers or an unbounded inbox.
  function createClient({WorkerClass, url, fonts, fontsFor, scriptsFor, timeoutMs=120000}) {
    let worker=null, active=null, seq=0, sent=new Set();   // sent: the other fonts (a piece in Stylish, Typewriter...) this worker already has, and the script fonts ("script:id") and shaping engine ("script:wasm")
    const queue=[];
    function finish(error,result) {
      if(!active)return;
      const task=active;active=null;clearTimeout(task.timer);
      if(error)task.reject(error);else task.resolve(result);
      pump();
    }
    function failed(error) {
      worker?.terminate();worker=null;
      finish(error instanceof Error ? error : new Error(error?.message || "Engraving worker stopped. Retry this placement."));
    }
    function pump() {
      if(active || !queue.length)return;
      active=queue.shift();
      try {
        if(!worker) {
          worker=new WorkerClass(url); sent=new Set();
          const current=worker;
          worker.onerror=error=>{if(worker===current)failed(error);};
          worker.onmessageerror=()=>{if(worker===current)failed(new Error("The engraving worker returned an unreadable result."));};
          worker.onmessage=({data})=>{
            if(!active || data.id!==active.id)return;
            if(data.error) finish(Object.assign(new Error(data.error.message),data.error));
            else finish(null,data.result);
          };
          worker.postMessage({type:"fonts",fonts});
        }
        // a piece engraved in another font than the default sends that font's files once to this worker, before its first fit
        const key=active.input && active.input.fontKey;
        if(key && fontsFor && !sent.has(key)) {
          const set=fontsFor(key);
          if(set){worker.postMessage({type:"fonts",key,fonts:set});sent.add(key);}
        }
        // (WORLD, Paul 19 Oct 2026) the words are in a script that needs a script font (Arabic, Hebrew, Indic, CJK): this worker gets those font files, and the shaping engine, once, before the fit
        const need=((active.input && active.input.scriptIds) || []).filter(id=>!sent.has("script:"+id));
        if(need.length && scriptsFor) {
          const pack=scriptsFor(need,!sent.has("script:wasm"));
          if(pack){worker.postMessage(Object.assign({type:"scripts"},pack));need.forEach(id=>sent.add("script:"+id));if(pack.wasm)sent.add("script:wasm");}
        }
        active.timer=setTimeout(()=>failed(new Error("This engraving preview took too long. Retry this placement.")),timeoutMs);
        worker.postMessage({type:"fit",id:active.id,input:active.input});
      } catch(error) {failed(error);}
    }
    return {run(input){return new Promise((resolve,reject)=>{queue.push({id:++seq,input,resolve,reject});pump();});}};
  }
  return {calculate,quickPlace,createClient};
});
