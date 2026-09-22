/* Bounded group search, owned by the solver worker. No DOM or network access.
 * GPU ranks positions for every eligible group member in the same dispatch.
 * CPU validates each branch exactly; only a verified prefix can be committed.
 */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestLookahead=api;})(typeof self!=='undefined'?self:globalThis,function(){
'use strict';
function uniqueVariants(variants){
 const buckets=new Map(),out=[];
 for(const v of variants){
  const bits=v.fine.pm.variants[0];let hash=2166136261;
  for(const word of bits)hash=Math.imul(hash^word,16777619);
  const key=v.fine.w+':'+v.fine.h+':'+hash,prior=buckets.get(key)||[];
  // Hashes only locate candidates; byte equality proves equivalent geometry.
  if(prior.some(p=>p.fine.pm.variants[0].length===bits.length&&p.fine.pm.variants[0].every((word,i)=>word===bits[i])))continue;
  prior.push(v);buckets.set(key,prior);out.push(v);
 }
 return out;
}
function stamp(state,p,pos,ratio){
 const fine=state.fine.clone(),coarse=state.coarse.clone(),v=pos.v;
 fine.stamp(v.fine.bits,v.fine.w,v.fine.h,pos.x,pos.y,p.id);
 for(let y=0;y<v.fine.h;y++)for(let x=0;x<v.fine.w;x++)if(v.fine.bits[y*v.fine.w+x])coarse.set(Math.floor((pos.x+x)/ratio),Math.floor((pos.y+y)/ratio));
 coarse.buildSAT();
 return {...state,fine,coarse,moves:state.moves.concat({p,...pos}),cells:state.cells+v.cells,contact:state.contact+(pos.score||0)};
}
// A heuristic remaining-space probe. Failure is a ranking penalty, never a
// proof of impossibility: sampled angles/positions may miss a narrow valid slot.
function availability(p,grid,metrics){
 const vs=p.variants;if(!vs?.length)return false;
 for(let i=0;i<vs.length;i+=Math.max(1,Math.floor(vs.length/6))){const pm=vs[i].coarse.pm,rx=grid.W-pm.w,by=grid.H-pm.h;if(rx<0||by<0)continue;
  const step=Math.max(1,Math.floor(Math.min(grid.W,grid.H)/10));
  for(let y=0;y<=by;y+=step)for(let x=0;x<=rx;x+=step){metrics.positions++;if(grid.fits(pm,x,y))return true;}
 }
 return false;
}
async function plan(ctx,S){
 const {gpu,ratio,metrics}=ctx,expired=()=>ctx.shouldStop?.()||performance.now()>=ctx.deadline;
 const cohort=ctx.pieces.filter(p=>!p.pinned&&p.variants.length).slice(0,7).map(p=>({...p,footprintCells:Math.min(...p.variants.map(v=>v.cells)),variants:uniqueVariants(p.variants)}));
 if(cohort.length<2||expired())return [];
 const bytesOf=g=>g.free.byteLength+g.occ.byteLength+(g.sat?.byteLength||0)+(g.material?bytesOf(g.material):0);
 const gridBytes=bytesOf(ctx.fine)+bytesOf(ctx.coarse);
 if(gridBytes*14>32*1024*1024)return []; // Leave very large sheets to the baseline.
 const depthLimit=cohort.length,beamWidth=Math.max(1,Math.min(3,Math.floor(32*1024*1024/Math.max(1,gridBytes*14)))),base={fine:ctx.fine,coarse:ctx.coarse,moves:[],cells:0,contact:0};
 const ahead=ctx.remaining.filter(p=>!p.pinned&&p.variants?.length);
 const viable=new Set(ahead.filter(p=>availability(p,ctx.coarse,metrics)).map(p=>p.id));
 const axis=ctx.fine.W>=ctx.fine.H?'x':'y';
 const occupiedEnd=ctx.strip?.end??(ctx.fine.parts||[]).reduce((n,p)=>Math.max(n,axis==='x'?p.x+p.w:p.y+p.h),0);
 const extent=state=>state.moves.reduce((n,m)=>Math.max(n,axis==='x'?m.x+m.v.fine.w:m.y+m.v.fine.h),occupiedEnd);
 const rank=(a,b)=>b.moves.length-a.moves.length || (a.risk||0)-(b.risk||0) || extent(a)-extent(b) || b.contact-a.contact;
 let beam=[base],best=base;const batches=new Map();
 for(let depth=0;depth<depthLimit&&!expired();depth++){
  const next=[],seen=new Set();
  for(const state of beam){
   if(expired())break;
   const used=new Set(state.moves.map(m=>m.p.id));
   const options=cohort.filter(p=>!used.has(p.id)&&ctx.placedCells+state.cells+p.footprintCells<=ctx.maxCells);
   if(!options.length)continue;
   // Even perfect interlocking cannot exceed this area-only piece-count bound.
   let capacity=ctx.maxCells-ctx.placedCells-state.cells,possible=state.moves.length;
   for(const cells of options.map(p=>p.footprintCells).sort((a,b)=>a-b)){if(cells>capacity)break;capacity-=cells;possible++;}
   if(possible<best.moves.length){metrics.pruned++;continue;}
   const batchKey=JSON.stringify(options.map(p=>p.id));let chunks=batches.get(batchKey);
   if(!chunks){
    const flat=[],owners=[];options.forEach((p,pi)=>p.variants.forEach((v,vi)=>{flat.push(v);owners.push({pi,vi});}));
    const size=Math.max(1,Math.floor(8000000/(state.coarse.W*state.coarse.H)));chunks=[];
    for(let start=0;start<flat.length;start+=size)chunks.push({variants:flat.slice(start,start+size),owners:owners.slice(start,start+size)});
    batches.set(batchKey,chunks); // Stable arrays let GPU geometry uploads be reused.
   }
   const lists=options.map(p=>p.variants.map(()=>[]));
   for(const {variants,owners} of chunks){
    if(expired())break;
    const args=[{variants},state.fine,state.coarse,ratio,.35,0,S.rng(1),0,0,ctx.strip?{...ctx.strip,end:extent(state)}:null,false];
    const rows=await gpu.candidates(args,S,(ctx.seed||1)+depth);
    const checks=variants.reduce((n,v)=>n+Math.max(0,state.coarse.W-v.coarse.w+1)*Math.max(0,state.coarse.H-v.coarse.h+1),0);
    metrics.positions+=checks;metrics.gpuPositions+=checks;metrics.searches++;ctx.report?.();
    rows.forEach((row,i)=>{const {pi,vi}=owners[i];lists[pi][vi]=row;});
   }
   if(expired())break;
   for(let pi=0;pi<options.length&&!expired();pi++){
    const p=options[pi],proposals=lists[pi].flatMap((row,vi)=>row.map(c=>({...c,vi}))).sort((a,b)=>b.s-a.s);
    // Retain two distinct fine fits per charm. A coarse proposal can fail fine
    // validation or refine onto the same spot, so examine up to eight seeds.
    const seeds=[];for(const c of proposals){if(seeds.every(s=>s.vi!==c.vi||Math.abs(s.x-c.x)+Math.abs(s.y-c.y)>2))seeds.push(c);if(seeds.length===8)break;}
    let accepted=0;
    for(const candidate of seeds){
     if(expired()||accepted===2)break;
     const rows=p.variants.map(()=>[]);rows[candidate.vi]=[candidate];
     const pos=S.search(p,state.fine,state.coarse,ratio,.35,0,S.rng(1),0,0,ctx.strip?{...ctx.strip,end:extent(state)}:null,false,null,rows,metrics,ctx.report,true);
     if(!pos||ctx.placedCells+state.cells+pos.v.cells>ctx.maxCells){metrics.pruned++;continue;}
     const key=state.moves.concat({p,...pos}).map(m=>[m.p.id,m.v.angle,m.x,m.y]).sort((a,b)=>String(a[0]).localeCompare(String(b[0]))).map(x=>JSON.stringify(x)).join('|');
     if(seen.has(key)){metrics.pruned++;continue;}seen.add(key);
     const branch=stamp(state,p,pos,ratio);metrics.branches++;accepted++;
     next.push(branch);if(rank(branch,best)<0)best=branch;
    }
   }
  }
  // Evaluate the space left for every remaining charm, including those outside
  // this group. Keep the full set of feasible branches until this cheap screen.
  for(const state of next){
   const used=new Set(state.moves.map(m=>m.p.id));state.risk=0;
   for(const p of ahead){if(expired())break;if(!used.has(p.id)&&viable.has(p.id)&&!availability(p,state.coarse,metrics))state.risk++;}
  }
  next.sort(rank);beam=next.slice(0,beamWidth);metrics.pruned+=Math.max(0,next.length-beam.length);
  if(beam.length&&rank(beam[0],best)<=0)best=beam[0];
  if(depth===depthLimit-1)metrics.groups+=next.length;
  ctx.report?.();if(!beam.length)break;
 }
 // A timed-out branch is useful only after at least two legal placements.
 return best.moves.length>=2?best.moves:[];
}
return {plan,uniqueVariants};
});
