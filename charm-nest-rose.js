/* Physical Rose Gold stock geometry. Coordinates are points, origin top left.
 * A conservative, stepped envelope preserves one connected reusable offcut.
 * History geometry is separate from current production artwork. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestRose=api;})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const STEP=.5, MM=25.4/72;
  function validate(p,w,h) {
    if(!p||p.version!==1||!['x','y'].includes(p.axis)||!(p.step>0&&p.step<=1)||Math.abs(p.wPt-w)>.01||Math.abs(p.hPt-h)>.01)throw new Error('Invalid Rose Gold remnant dimensions');
    const length=p.axis==='x'?h:w,depth=p.axis==='x'?w:h;
    if(!Array.isArray(p.values)||p.values.length!==Math.ceil(length/p.step)||p.values.length>6000||p.values.some(v=>!Number.isFinite(v)||v<0||v>depth))throw new Error('Invalid Rose Gold remnant contour');
    return p;
  }
  function area(p){return p.values.reduce((sum,v,i)=>sum+v*Math.min(p.step,(p.axis==='x'?p.hPt:p.wPt)-i*p.step),0);}
  function frontier(p,lo,hi,pad=0){
    let max=0;const a=Math.max(0,Math.floor((lo-pad)/p.step)),b=Math.min(p.values.length-1,Math.floor((hi+pad)/p.step));
    for(let i=a;i<=b;i++)max=Math.max(max,p.values[i]);return max?max+pad:0;
  }
  function intersects(p,x,y,w,h,pad=0){if(!p)return false;return p.axis==='x'?x<frontier(p,y,y+h,pad):y<frontier(p,x,x+w,pad);}
  // Stamp the entire pixel footprint, not just its centre. Negative charm
  // clearance never authorizes cutting through an already removed region.
  function stamp(grid,p,res,pad=0){
    if(!p)return;validate(p,p.wPt,p.hPt);
    if(Math.abs(grid.W/res-p.wPt)>2/res||Math.abs(grid.H/res-p.hPt)>2/res)throw new Error("Remnant grid dimensions changed");
    for(let y=0;y<grid.H;y++)for(let x=0;x<grid.W;x++)if(intersects(p,x/res,y/res,1/res,1/res,pad)){grid.set(x,y);grid.free[y*grid.W+x]=0;}
  }
  function compact(points){const out=[];for(const p of points){const n=out.length;if(n&&p[0]===out[n-1][0]&&p[1]===out[n-1][1])continue;if(n>1){const a=out[n-2],b=out[n-1];if((a[0]===b[0]&&b[0]===p[0])||(a[1]===b[1]&&b[1]===p[1]))out.pop();}out.push(p);}return out;}
  function lines(p,prior){
    const result=[],old=prior?.values||p.values.map(()=>0),swap=(d,t)=>p.axis==='x'?[d,t]:[t,d],len=p.axis==='x'?p.hPt:p.wPt;
    let i=0;while(i<p.values.length){if(p.values[i]<=old[i]+1e-7){i++;continue;}const start=i,pts=[swap(old[i],i*p.step)];
      while(i<p.values.length&&p.values[i]>old[i]+1e-7){pts.push(swap(p.values[i],i*p.step),swap(p.values[i],Math.min(len,(i+1)*p.step)));i++;}
      pts.push(swap(old[i-1],Math.min(len,i*p.step)));result.push(compact(pts));
    }return result;
  }
  function profile(shapes,w,h,axis,prior,allowancePt){
    const step=prior?.step||STEP,len=axis==='x'?h:w,depth=axis==='x'?w:h,values=prior?prior.values.slice():Array(Math.ceil(len/step)).fill(0);
    for(const shape of shapes)for(const path of shape.paths)for(let j=0;j<path.length;j++){
      const a=path[j],b=path[(j+1)%path.length],t0=axis==='x'?a[1]:a[0],t1=axis==='x'?b[1]:b[0],d0=axis==='x'?a[0]:a[1],d1=axis==='x'?b[0]:b[1];
      const lo=Math.max(0,Math.floor((Math.min(t0,t1)-allowancePt)/step)),hi=Math.min(values.length-1,Math.floor((Math.max(t0,t1)+allowancePt)/step));
      for(let i=lo;i<=hi;i++){
        const ta=Math.max(Math.min(t0,t1),i*step-allowancePt),tb=Math.min(Math.max(t0,t1),(i+1)*step+allowancePt);
        const da=t0===t1?Math.max(d0,d1):d0+(d1-d0)*(ta-t0)/(t1-t0),db=t0===t1?da:d0+(d1-d0)*(tb-t0)/(t1-t0);
        values[i]=Math.min(depth,Math.max(values[i],Math.ceil((Math.max(da,db)+allowancePt)*1000)/1000));
      }
    }return {version:1,wPt:w,hPt:h,axis,step,values};
  }
  function plan(shapes,w,h,prior,allowanceMm=.2){
    if(!(w>0&&h>0&&w<=1420&&h<=1420)||!Array.isArray(shapes)||!shapes.length||!Number.isFinite(allowanceMm)||allowanceMm<.05||allowanceMm>2)throw new Error('Invalid Rose Gold cut plan');
    if(prior)validate(prior,w,h);
    let points=0;for(const s of shapes){if(!s.paths?.length)throw new Error('Missing charm outline');for(const path of s.paths){if(path.length<3)throw new Error('Incomplete charm outline');for(const pt of path){if(++points>120000||pt.length!==2||!pt.every(Number.isFinite)||pt[0]<-.01||pt[1]<-.01||pt[0]>w+.01||pt[1]>h+.01)throw new Error('Charm outline outside Rose Gold sheet');}}}
    const candidates=(prior?[prior.axis]:['x','y']).map(axis=>profile(shapes,w,h,axis,prior,allowanceMm/MM+.015));
    candidates.sort((a,b)=>area(a)-area(b));const remaining=candidates[0];
    return {version:1,profile:remaining,lines:lines(remaining,prior),shapes,allowanceMm,removedPt2:area(remaining)-(prior?area(prior):0),remainingPt2:w*h-area(remaining)};
  }
  function flatten(seg){
    const paths=[];for(const sub of seg?.subpaths||[]){let path=[],cur=null;
      const cubic=(a,b,c,d,depth=0)=>{const len=Math.hypot(d[0]-a[0],d[1]-a[1]);const distance=p=>len?Math.abs((d[0]-a[0])*(a[1]-p[1])-(a[0]-p[0])*(d[1]-a[1]))/len:Math.hypot(p[0]-a[0],p[1]-a[1]);
        if(depth>=18||Math.max(distance(b),distance(c))<.008){path.push(d);return;}const mid=(p,q)=>[(p[0]+q[0])/2,(p[1]+q[1])/2],ab=mid(a,b),bc=mid(b,c),cd=mid(c,d),abc=mid(ab,bc),bcd=mid(bc,cd),m=mid(abc,bcd);cubic(a,ab,abc,m,depth+1);cubic(m,bcd,cd,d,depth+1);};
      for(const op of sub){if(op[0]==='m'){if(path.length>2)paths.push(path);path=[op[1]];cur=op[1];}else if(op[0]==='l'){path.push(op[1]);cur=op[1];}else if(op[0]==='c'&&cur){cubic(cur,op[1],op[2],op[3]);cur=op[3];}}
      if(path.length>2)paths.push(path);
    }return paths;
  }
  function shapes(charms,placements){const byId=new Map(charms.map(c=>[c.id,c]));return placements.map(p=>{const c=byId.get(p.id);if(!c?.outline)throw new Error('Reload the charm vectors before preparing a cut');const angle=p.angle*Math.PI/180,cos=Math.cos(angle),sin=Math.sin(angle),scale=p.scale||.975;
    const transform=paths=>paths.map(path=>path.map(([x,y])=>{const dx=(x-c.centerPt[0])*scale,dy=(c.centerPt[1]-y)*scale;return [+(p.cxPt+dx*cos-dy*sin).toFixed(4),+(p.cyPt+dx*sin+dy*cos).toFixed(4)];}));
    return {id:p.id,paths:transform(flatten(c.outline)),ink:transform((c.members||[]).filter(m=>m!==c.outline).flatMap(flatten))};
  });}
  return {flatten,shapes,validate,area,frontier,intersects,stamp,plan,lines,MM};
});
