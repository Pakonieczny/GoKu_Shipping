/* Production exports: original vector AI pages, exact-copy backs, millimetre DXF.
 * DXF R2004: true colour (420), indexed fallback (62), named layers and closed
 * outline polylines. Filled PDF artwork becomes contours for EZCAD hatching.
 * Curves are adaptively flattened to <= 0.002 mm, never resized to fit a page.
 */
(function(root) {
  'use strict';
  const MM = 25.4 / 72, GAP = 10 / MM, SPACE = 3 / MM;
  const pdf = () => root.CharmNestPDF, lib = () => root.PDFLib;
  const union = (a,b) => !a ? b.slice() : [Math.min(a[0],b[0]),Math.min(a[1],b[1]),Math.max(a[2],b[2]),Math.max(a[3],b[3])];
  function leaves(parsed) {
    const seen = new Set(), out = [], clips = [];
    function visit(s, parent) {
      if (seen.has(s)) return; seen.add(s);
      const layer = s.layer || parent || 'Artwork';
      if (s.kind === 'xobj' && s.children?.length) s.children.forEach(k => visit(k,layer));
      else if (s.kind === 'path') out.push({...s, layer});
      else if(s.kind === 'clip') clips.push(s);
      else if(s.kind === 'noop') return;
      else throw new Error(`Cannot export ${s.kind} on ${layer} to vector DXF. Outline text and remove raster artwork first.`);
    }
    parsed.segments.forEach(s => visit(s));
    // Illustrator commonly includes huge rectangular artboard clips. They do
    // not change any ink. Accept only provably non-intersecting clips; never
    // silently export geometry that the AI file hides behind a real mask.
    const bounds=out.reduce((a,s)=>union(a,s.bbox),null);
    for(const clip of clips) {
      const sub=clip.subpaths?.[0]||[],pts=sub.filter(o=>o[0]!=='h').map(o=>o[1]);
      const cross=(a,b,p)=>(b[0]-a[0])*(p[1]-a[1])-(b[1]-a[1])*(p[0]-a[0]);
      const same=xs=>xs.every(x=>x>=-1e-6)||xs.every(x=>x<=1e-6);
      const convex=clip.subpaths.length===1&&pts.length===4&&sub.every(o=>['m','l','h'].includes(o[0]))&&same(pts.map((p,i)=>cross(p,pts[(i+1)%4],pts[(i+2)%4])));
      const corners=bounds?[[bounds[0],bounds[1]],[bounds[0],bounds[3]],[bounds[2],bounds[1]],[bounds[2],bounds[3]]]:[];
      if(!convex||!corners.every(p=>same(pts.map((a,i)=>cross(a,pts[(i+1)%4],p)))))throw new Error('This artwork uses a clipping mask. Expand the clipped artwork before DXF export.');
    }
    return out;
  }
  function parentScale(front, sheet, back) {
    const charm = (sheet.charms || []).find(c => c.poolId === back.poolId);
    const placement = (sheet.placements || []).find(p => p.id === charm?.id || p.poolId === back.poolId);
    if (!placement) throw new Error(`Back ${back.poolId}: no matching placed front charm.`);
    const forms = front.segments.filter(s => s.kind === 'xobj');
    const layer = placement.layer || placement.layerName;
    const candidates = layer ? forms.filter(s => s.layer === layer) : [];
    const form = candidates.length === 1 ? candidates[0] : forms.length === sheet.placements.length ? forms[(placement.n || sheet.placements.indexOf(placement)+1)-1] : null;
    if (!form?.matrix) throw new Error(`Back ${back.poolId}: cannot verify its parent scale.`);
    const [a,b,c,d] = form.matrix, sx = Math.hypot(a,b), sy = Math.hypot(c,d);
    if (!(sx > 0) || Math.abs(sx-sy) > 1e-5 || Math.abs(a*c+b*d)>1e-5) throw new Error('Nonuniform parent scaling cannot be exported safely.');
    if (placement.scale && Math.abs(placement.scale-sx)>1e-5) throw new Error('Saved placement scale disagrees with the actual front artwork.');
    return sx;
  }
  async function compose(frontBytes, sheet, backs) {
    const P = pdf(), L = lib();
    const front = await P.parseSource(frontBytes, 'Front sheet');
    const inputs = [], layout = []; let x = SPACE, y = front.pageH + GAP, rowH = 0;
    for (const b of backs) {
      const parsed = await P.parseSource(b.bytes, 'Back '+b.poolId), paths = leaves(parsed);
      const bounds = paths.reduce((a,s)=>union(a,s.bbox),null);
      if (!bounds) throw new Error('Back engraving contains no vector paths.');
      const scale = parentScale(front,sheet,b);
      const w = (bounds[2]-bounds[0])*scale, h = (bounds[3]-bounds[1])*scale;
      if (w > front.pageW+1e-5) throw new Error('Back engraving is wider than its sheet; export would require resizing.');
      if (x>SPACE && x+w > front.pageW-SPACE) {x=SPACE;y+=rowH+SPACE;rowH=0;}
      if (x===SPACE && x+w > front.pageW) x=Math.max(0,(front.pageW-w)/2);
      const tx=x-bounds[0]*scale, ty=y-bounds[1]*scale;
      inputs.push({parsed, scale, x:tx, y:ty, back:b});
      layout.push({poolId:b.poolId, scale, boundsPt:[x,y,x+w,y+h], parentSheetId:sheet.id || sheet.sheetId});
      x+=w+SPACE;rowH=Math.max(rowH,h);
    }
    const out = await L.PDFDocument.create();
    const page = out.addPage([front.pageW,inputs.length ? y+rowH+SPACE : front.pageH]);
    out.setTitle(sheet.fileBase || 'Charm production sheet');out.setProducer('Brites Charm Nesting Station');
    async function place(parsed, x, y, scale, prefix) {
      const [copy] = await out.copyPages(parsed.doc,[0]);
      // copyPages preserves the page's OCG resource references. Register those
      // same references in the output catalogue so Illustrator retains layers.
      if (prefix) {
        const resources=copy.node.Resources();
        const seen=new Set();
        function walk(obj) {
          if (obj instanceof L.PDFRef) {if(seen.has(obj.toString()))return;seen.add(obj.toString());obj=out.context.lookup(obj);}
          if (obj instanceof L.PDFDict) {
            if(obj.get(L.PDFName.of('Type'))?.toString()==='/OCG') {
              const old=obj.get(L.PDFName.of('Name'));
              obj.set(L.PDFName.of('Name'),L.PDFString.of(prefix+(old?.decodeText?.() || 'Artwork')));
            }
            obj.entries().forEach(([,v])=>walk(v));
          } else if(obj instanceof L.PDFArray) obj.asArray().forEach(walk);
          else if(obj?.dict) walk(obj.dict);
        }
        walk(resources);
      }
      const embedded=await out.embedPage(copy);
      page.drawPage(embedded,{x,y,width:parsed.pageW*scale,height:parsed.pageH*scale});
    }
    await place(front,0,0,1,'');
    for(const item of inputs) await place(item.parsed,item.x,item.y,item.scale,'BACK '+item.back.poolId+' / ');
    const refs=out.context.enumerateIndirectObjects().filter(([,o])=>o instanceof L.PDFDict && o.get(L.PDFName.of('Type'))?.toString()==='/OCG').map(([r])=>r);
    if(refs.length) out.catalog.set(L.PDFName.of('OCProperties'),out.context.obj({OCGs:refs,D:{Order:refs,ON:refs}}));
    const ai=await out.save({useObjectStreams:false});
    return {ai,layout,widthPt:front.pageW,heightPt:page.getHeight(),cutHeightPt:front.pageH};
  }
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
  function dxf(paths, declaredLayers=[]) {
    const colors=[[0,0,0],[255,0,0],[255,255,0],[0,255,0],[0,255,255],[0,0,255],[255,0,255],[0,0,0],[128,128,128],[192,192,192]];
    const rgb=v=>(v||[0,0,0]).map(n=>Math.max(0,Math.min(255,Math.round(n*255))));
    const trueColor=c=>(c[0]<<16)+(c[1]<<8)+c[2];
    const aci=c=>{let best=7,score=Infinity;for(let i=1;i<colors.length;i++){let d=c.reduce((n,v,k)=>n+(v-colors[i][k])**2,0);if(d<score){best=i;score=d;}}return best;};
    const ascii=s=>String(s).replace(/[^\x20-\x7e]/g,c=>'\\U+'+c.charCodeAt(0).toString(16).toUpperCase().padStart(4,'0'));
    const names=new Map(),used=new Set(['0']);
    const layer=s=>{if(!names.has(s)){let n=ascii(s.replace(/[<>/\\":;?*|=]/g,'_')).slice(0,180)||'Artwork',i=2,base=n;while(used.has(n))n=base+'_'+i++;used.add(n);names.set(s,n);}return names.get(s);};
    const entities=[];const layerColors=new Map();
    for(const name of declaredLayers)layerColors.set(layer(name),[0,0,0]);
    for(const path of paths) {
      const modes=[];
      if(path.fill)modes.push({color:rgb(path.fillRGB),fill:true});
      if(path.stroke && (!path.fill || trueColor(rgb(path.fillRGB))!==trueColor(rgb(path.strokeRGB))))modes.push({color:rgb(path.strokeRGB),fill:false});
      for(const mode of modes)for(const sub of path.subpaths||[]) {
        const f=flatten(sub);if(f.points.length<2)continue;
        const name=layer(path.layer||'Artwork');if(!entities.some(e=>e.layer===name))layerColors.set(name,mode.color);
        entities.push({...f,closed:f.closed||mode.fill,layer:name,...mode});
      }
    }
    let text='',handle=16;const add=(...pairs)=>{for(let i=0;i<pairs.length;i+=2)text+=pairs[i]+'\r\n'+pairs[i+1]+'\r\n';};
    add(0,'SECTION',2,'HEADER',9,'$ACADVER',1,'AC1018',9,'$INSUNITS',70,4,9,'$MEASUREMENT',70,1,0,'ENDSEC');
    add(0,'SECTION',2,'TABLES',0,'TABLE',2,'LTYPE',5,(handle++).toString(16),100,'AcDbSymbolTable',70,1,0,'LTYPE',5,(handle++).toString(16),100,'AcDbSymbolTableRecord',100,'AcDbLinetypeTableRecord',2,'CONTINUOUS',70,0,3,'Solid line',72,65,73,0,40,0,0,'ENDTAB');
    add(0,'TABLE',2,'LAYER',5,(handle++).toString(16),100,'AcDbSymbolTable',70,layerColors.size+1);
    for(const [name,c] of [['0',[0,0,0]],...layerColors])add(0,'LAYER',5,(handle++).toString(16),100,'AcDbSymbolTableRecord',100,'AcDbLayerTableRecord',2,name,70,0,62,aci(c),420,trueColor(c),6,'CONTINUOUS');
    add(0,'ENDTAB',0,'TABLE',2,'APPID',5,(handle++).toString(16),100,'AcDbSymbolTable',70,1,0,'APPID',5,(handle++).toString(16),100,'AcDbSymbolTableRecord',100,'AcDbRegAppTableRecord',2,'BRITES',70,0,0,'ENDTAB',0,'ENDSEC',0,'SECTION',2,'ENTITIES');
    for(const ent of entities) {
      add(0,'LWPOLYLINE',5,(handle++).toString(16),100,'AcDbEntity',8,ent.layer,62,aci(ent.color),420,trueColor(ent.color),100,'AcDbPolyline',90,ent.points.length,70,ent.closed?1:0);
      for(const p of ent.points)add(10,+(p[0]*MM).toFixed(7),20,+(p[1]*MM).toFixed(7));
      add(1001,'BRITES',1000,ent.fill?'FILL_CONTOUR':'STROKE');
    }
    add(0,'ENDSEC',0,'EOF');return {text,entityCount:entities.length,layers:[...names.entries()],colors:[...new Set(entities.map(e=>trueColor(e.color)))]};
  }
  function layerNames(parsed) {
    const L=lib();return [...new Set(parsed.doc.context.enumerateIndirectObjects().filter(([,o])=>o instanceof L.PDFDict&&o.get(L.PDFName.of('Type'))?.toString()==='/OCG').map(([,o])=>o.get(L.PDFName.of('Name'))?.decodeText?.()).filter(Boolean))];
  }
  const api={compose,parentScale,leaves,flatten,dxf,layerNames,MM};
  root.CharmNestExport=api;
  if(typeof module==='object'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
