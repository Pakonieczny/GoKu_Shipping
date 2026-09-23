/* Physical Rose Gold stock geometry. Coordinates are points, origin top left.
 * A conservative, stepped envelope preserves one connected reusable offcut.
 * History geometry is separate from current production artwork. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestRose=api;})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const STEP=.5, MM=25.4/72;
  // Saved cut outlines are simplified once, so a full sheet fits one saved
  // record. Every dropped point stays within OUTLINE_PT (plus 3-decimal
  // rounding) of the kept outline, and contours add that distance back.
  // Engraving detail is only drawn in the grey history, so it is coarser.
  const OUTLINE_PT=.015, INK_PT=.05, SLIM_MARGIN_PT=OUTLINE_PT+.001;
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
    const candidates=(prior?[prior.axis]:['x','y']).map(axis=>profile(shapes,w,h,axis,prior,allowanceMm/MM+.015+SLIM_MARGIN_PT));
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
  function segmentDistance([x,y],[ax,ay],[bx,by]){
    const dx=bx-ax,dy=by-ay,l2=dx*dx+dy*dy,t=l2?Math.max(0,Math.min(1,((x-ax)*dx+(y-ay)*dy)/l2)):0;
    return Math.hypot(x-ax-t*dx,y-ay-t*dy);
  }
  // Douglas-Peucker against segments, not infinite lines, so closed loops are safe.
  function simplify(path,tolerance){
    if(path.length<=3)return path;
    const keep=new Uint8Array(path.length),stack=[[0,path.length-1]];keep[0]=keep[path.length-1]=1;
    while(stack.length){
      const [a,b]=stack.pop();let far=-1,at=-1;
      for(let i=a+1;i<b;i++){const d=segmentDistance(path[i],path[a],path[b]);if(d>far){far=d;at=i;}}
      if(far>tolerance){keep[at]=1;stack.push([a,at],[at,b]);}
    }
    return path.filter((_,i)=>keep[i]);
  }
  function slimPaths(paths,tolerance){return (paths||[]).map(path=>{const round=pts=>pts.map(([x,y])=>[+x.toFixed(3),+y.toFixed(3)]),slim=round(simplify(path,tolerance));return slim.length>=3||path.length<3?slim:round(path);});}
  // Idempotent: already slimmed outlines are returned unchanged.
  function slimShape(s){return s.slim?s:{...s,paths:slimPaths(s.paths,OUTLINE_PT),ink:slimPaths(s.ink,INK_PT),slim:1};}
  function shapes(charms,placements){const byId=new Map(charms.map(c=>[c.id,c]));return placements.map(p=>{const c=byId.get(p.id);if(!c?.outline)throw new Error('Reload the charm vectors before preparing a cut');const angle=p.angle*Math.PI/180,cos=Math.cos(angle),sin=Math.sin(angle),scale=p.scale||.975;
    const transform=paths=>paths.map(path=>path.map(([x,y])=>{const dx=(x-c.centerPt[0])*scale,dy=(c.centerPt[1]-y)*scale;return [+(p.cxPt+dx*cos-dy*sin).toFixed(4),+(p.cyPt+dx*sin+dy*cos).toFixed(4)];}));
    return slimShape({id:p.id,paths:transform(flatten(c.outline)),ink:transform((c.members||[]).filter(m=>m!==c.outline).flatMap(flatten))});
  });}
  // Synthetic rehearsal artwork. The same vectors and conservative bitmaps
  // are used by the browser worker and the server's independent verifier.
  function demoBatch(batch,prior=null){
    if(!Number.isInteger(batch)||batch<1||batch>3)throw new Error('Choose rehearsal batch 1–3');
    const charms=[],pieces=[],scale=2;
    for(let i=0;i<7;i++){
      const wPt=24+(i%3)*4,hPt=27+((i+batch)%3)*4,n=40,kind=(i+batch)%3;
      const points=Array.from({length:n},(_,j)=>{const a=j*Math.PI*2/n,r=kind===0?1:kind===1?.79+.2*Math.cos(a*5):.8+.18*Math.cos(a*2);return [wPt/2+(wPt/2-1)*r*Math.cos(a),hPt/2+(hPt/2-1)*r*Math.sin(a)];});
      const id='demo-'+batch+'-'+i,outline={subpaths:[[['m',points[0]],...points.slice(1).map(p=>['l',p]),['h']]]};
      charms.push({id,name:['Oval','Flower','Leaf'][kind],outline,centerPt:[wPt/2,hPt/2],members:[]});
      const w=wPt*scale,h=hPt*scale,bits=new Uint8Array(w*h);
      const inside=(x,y)=>{let on=false;for(let a=0,b=points.length-1;a<points.length;b=a++){const p=points[a],q=points[b];if((p[1]>y)!==(q[1]>y)&&x<(q[0]-p[0])*(y-p[1])/(q[1]-p[1])+p[0])on=!on;}return on;};
      // One-pixel outward guard makes the mask conservative for vector edges.
      for(let y=0;y<h;y++)for(let x=0;x<w;x++)if(inside((x+.5)/scale,hPt-(y+.5)/scale))for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){const xx=x+dx,yy=y+dy;if(xx>=0&&xx<w&&yy>=0&&yy<h)bits[yy*w+xx]=1;}
      pieces.push({id,w,h,scale,bits,areaPt2:bits.reduce((a,b)=>a+b,0)/(scale*scale)});
    }
    return {charms,job:{sheet:{wPt:100/MM,hPt:50/MM,insetPt:1.5,remnant:prior},pieces,angles:Array.from({length:36},(_,i)=>i*10),clearancePt:.6,fineRes:2,coarseRes:.5,maxFill:.95,maxTrials:3,timeBudgetMs:2500,seed:41+batch,verifyResult:true}};
  }
  return {flatten,shapes,slimShape,simplify,validate,area,frontier,intersects,stamp,plan,lines,MM,demoBatch};
});
