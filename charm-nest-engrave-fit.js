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
    const check = G.verifyInk(fit.cmds, job.mask);                          // 7.4 · geometry: zero ink outside the eroded mask, zero in any hole
    if (!check.ok) { throw new Error(`ink outside the eroded mask after fitting (${check.outside} px) — a bug, not a review item`); }
    return {view,mask:job.mask,fit,lines:job.lines,check};
  }

  // One worker, one active calculation. Queued inputs stay on the caller's
  // side; they are not cloned into dozens of workers or an unbounded inbox.
  function createClient({WorkerClass, url, fonts, timeoutMs=120000}) {
    let worker=null, active=null, seq=0;
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
          worker=new WorkerClass(url);
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
        active.timer=setTimeout(()=>failed(new Error("This engraving preview took too long. Retry this placement.")),timeoutMs);
        worker.postMessage({type:"fit",id:active.id,input:active.input});
      } catch(error) {failed(error);}
    }
    return {run(input){return new Promise((resolve,reject)=>{queue.push({id:++seq,input,resolve,reject});pump();});}};
  }
  return {calculate,createClient};
});
