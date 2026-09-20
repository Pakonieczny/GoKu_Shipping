/* Shared vector booleans. Curves deviate by at most 0.002 mm; integer
 * clipping precision is 0.00001 pt. PDF winding rules remain authoritative. */
(function(root){
  'use strict';
  const MM=25.4/72, SCALE=1e5;
  const C=root.ClipperLib || (typeof require==='function' ? require('./vendor/clipper-6.4.2.js') : null);
  function flatten(sub, tolerance=.002/MM) {
    const points=[];let current=null,closed=false;
    const dist=(p,a,b)=>{const dx=b[0]-a[0],dy=b[1]-a[1],l=dx*dx+dy*dy;const t=l?Math.max(0,Math.min(1,((p[0]-a[0])*dx+(p[1]-a[1])*dy)/l)):0;return Math.hypot(p[0]-a[0]-t*dx,p[1]-a[1]-t*dy);};
    const mid=(a,b)=>[(a[0]+b[0])/2,(a[1]+b[1])/2];
    function curve(a,b,c,d,depth=0) {
      if(Math.max(dist(b,a,d),dist(c,a,d))<=tolerance){points.push(d);return;}
      if(depth>=24)throw new Error('Curve exceeds DXF precision limits.');
      const ab=mid(a,b),bc=mid(b,c),cd=mid(c,d),abc=mid(ab,bc),bcd=mid(bc,cd),m=mid(abc,bcd);
      curve(a,ab,abc,m,depth+1);curve(m,bcd,cd,d,depth+1);
    }
    for(const op of sub){
      if(op[0]==='m'){current=op[1];points.push(current);}
      else if(op[0]==='l'){current=op[1];points.push(current);}
      else if(op[0]==='c'){if(!current)throw new Error('Invalid curve');curve(current,op[1],op[2],op[3]);current=op[3];}
      else if(op[0]==='h')closed=true;
    }
    if(points.length>2 && Math.hypot(points[0][0]-points.at(-1)[0],points[0][1]-points.at(-1)[1])<1e-8){closed=true;points.pop();}
    return {points,closed};
  }

  function boolean(subject, clip=[], operation='union', subjectRule='nonzero', clipRule='nonzero') {
    if(!C)throw new Error('Vector geometry library is unavailable. Refresh the page.');
    const points=paths=>paths.filter(p=>p.length>=3).map(p=>p.map(([x,y])=>{
      if(!Number.isFinite(x)||!Number.isFinite(y))throw new Error('Vector contains a non-finite coordinate.');
      return {X:Math.round(x*SCALE),Y:Math.round(y*SCALE)};
    }));
    const engine=new C.Clipper(C.Clipper.ioStrictlySimple),tree=new C.PolyTree();
    engine.AddPaths(points(subject),C.PolyType.ptSubject,true);
    engine.AddPaths(points(clip),C.PolyType.ptClip,true);
    const rule=r=>r==='evenodd'?C.PolyFillType.pftEvenOdd:C.PolyFillType.pftNonZero;
    if(!engine.Execute(operation==='difference'?C.ClipType.ctDifference:C.ClipType.ctUnion,tree,rule(subjectRule),rule(clipRule)))return [];
    const out=[];
    function visit(node,depth){for(const child of node.Childs()) {out.push({points:child.Contour().map(p=>[p.X/SCALE,p.Y/SCALE]),hole:child.IsHole(),depth});visit(child,depth+1);}}
    visit(tree,0);return out;
  }
  const filled=path=>boolean((path.subpaths||[]).map(s=>flatten(s).points),[],'union',path.paintOp?.endsWith('*')?'evenodd':'nonzero');
  const subpath=points=>[['m',points[0]],...points.slice(1).map(p=>['l',p]),['h']];
  const api={flatten,boolean,filled,subpath};root.CharmNestVector=api;
  if(typeof module==='object'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
