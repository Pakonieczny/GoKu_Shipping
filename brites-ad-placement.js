/* Charm placement check for every ad size. It confirms where the charm really
 * is in each photograph (the AI's box is only a starting point), then checks
 * each layout keeps the complete charm inside the ad and clear of text,
 * buttons and the logo. Pure geometry and pixel counting: no provider
 * requests, no charges. Shared by the browser editor and the Netlify design
 * engine, so a size that fails here fails everywhere. */
(function(root){
  'use strict';
  const VERSION=1;
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  const finite=v=>typeof v==='number'&&Number.isFinite(v);
  const validBox=b=>!!b&&['x','y','width','height'].every(k=>finite(b[k]))&&b.width>0&&b.height>0;
  const union=(a,b)=>{const x=Math.min(a.x,b.x),y=Math.min(a.y,b.y);return {x,y,width:Math.max(a.x+a.width,b.x+b.width)-x,height:Math.max(a.y+a.height,b.y+b.height)-y};};
  const EDGE_NAMES={top:'top',right:'right side',bottom:'bottom',left:'left side'};
  const joinEdges=edges=>{const n=edges.map(e=>EDGE_NAMES[e]||e);return n.length<2?n.join(''):n.slice(0,-1).join(', ')+' and '+n[n.length-1];};

  // sRGB 0-255 to CIE Lab. Distances in Lab track what a person sees, so gold
  // on pale fabric and silver on dark wood both read as clearly different.
  function lab(r,g,b){
    const lin=v=>{v/=255;return v<=.04045?v/12.92:((v+.055)/1.055)**2.4;};
    const R=lin(r),G=lin(g),B=lin(b);
    const f=t=>t>.008856?Math.cbrt(t):7.787*t+16/116;
    const X=f((R*.4124+G*.3576+B*.1805)/.95047),Y=f(R*.2126+G*.7152+B*.0722),Z=f((R*.0193+G*.1192+B*.9505)/1.08883);
    return [116*Y-16,500*(X-Y),200*(Y-Z)];
  }

  // Average the photo down to a small grid. Callers may pass a pre-shrunk
  // image (browser canvas, sharp resize); large inputs are shrunk here.
  function grid(image,maxSide=256){
    const width=Number(image?.width),height=Number(image?.height),data=image?.data;
    if(!data||!(width>0)||!(height>0))throw Error('Photo pixels are unavailable for the charm check.');
    const channels=Number(image.channels)||Math.round(data.length/(width*height));
    if(channels<3)throw Error('The charm check needs a colour photograph.');
    const s=Math.min(1,maxSide/Math.max(width,height)),w=Math.max(1,Math.round(width*s)),h=Math.max(1,Math.round(height*s));
    const sum=new Float64Array(w*h*3),count=new Float64Array(w*h);
    for(let y=0;y<height;y++){const gy=Math.min(h-1,Math.floor(y*h/height));
      for(let x=0;x<width;x++){const gx=Math.min(w-1,Math.floor(x*w/width)),i=(y*width+x)*channels,o=gy*w+gx;
        sum[o*3]+=data[i];sum[o*3+1]+=data[i+1];sum[o*3+2]+=data[i+2];count[o]++;}}
    const L=new Float32Array(w*h),A=new Float32Array(w*h),B=new Float32Array(w*h);
    for(let o=0;o<w*h;o++){const c=count[o]||1,[l,a,b]=lab(sum[o*3]/c,sum[o*3+1]/c,sum[o*3+2]/c);L[o]=l;A[o]=a;B[o]=b;}
    return {w,h,L,A,B};
  }

  // A few colour centres describe the photo around the charm (fabric, shadow,
  // prop edge) and inside the AI box. A colour far more common inside the box
  // than around it is the charm's own, so it never counts as surface even
  // when a misplaced box leaves part of the charm in the surrounding ring.
  // k-means over a bounded sample keeps this fast.
  function surfaceModel(g,ring,core){
    const pick=(list,limit)=>{const step=Math.max(1,Math.floor(list.length/limit)),out=[];for(let i=0;i<list.length;i+=step)out.push(list[i]);return out;};
    const ringSample=pick(ring,5000),coreSample=pick(core,2500),sample=ringSample.concat(coreSample);
    const k=Math.min(5,sample.length),centres=[];
    for(let i=0;i<k;i++){const o=sample[Math.floor((i+.5)*sample.length/k)];centres.push([g.L[o]*.7,g.A[o],g.B[o]]);}
    const nearest=(o,list)=>{let best=-1,bd=Infinity;for(let c=0;c<list.length;c++){const d=(g.L[o]*.7-list[c][0])**2+(g.A[o]-list[c][1])**2+(g.B[o]-list[c][2])**2;if(d<bd){bd=d;best=c;}}return [best,bd];};
    for(let iter=0;iter<10;iter++){
      const acc=centres.map(()=>[0,0,0,0]);
      for(const o of sample){const [best]=nearest(o,centres),a=acc[best];a[0]+=g.L[o]*.7;a[1]+=g.A[o];a[2]+=g.B[o];a[3]++;}
      acc.forEach((a,c)=>{if(a[3])centres[c]=[a[0]/a[3],a[1]/a[3],a[2]/a[3]];});
    }
    const share=list=>{const n=new Array(centres.length).fill(0);for(const o of list)n[nearest(o,centres)[0]]++;return n.map(v=>v/Math.max(1,list.length));};
    const ringShare=share(ringSample),coreShare=share(coreSample);
    let surface=centres.filter((_,c)=>!(coreShare[c]>=.05&&coreShare[c]>=ringShare[c]*1.8+.01));
    if(!surface.length)surface=centres;
    const distance=o=>Math.sqrt(nearest(o,surface)[1]);
    const spread=ringSample.map(distance).sort((a,b)=>a-b),p85=spread[Math.floor(spread.length*.85)]||0;
    return {distance,threshold:Math.max(12,Math.min(40,p85*2.4))};
  }

  // Remove thin structures (chains, cords, fabric threads) so only solid
  // shapes such as the charm itself remain: erode, then dilate, 3x3.
  function open(mask,w,h){
    const eroded=new Uint8Array(w*h),out=new Uint8Array(w*h);
    for(let y=1;y<h-1;y++)for(let x=1;x<w-1;x++){let all=1;for(let dy=-1;dy<=1&&all;dy++)for(let dx=-1;dx<=1;dx++)if(!mask[(y+dy)*w+x+dx]){all=0;break;}eroded[y*w+x]=all;}
    // Pixels on the photo's own edge cannot be eroded with a full window; keep
    // them when their inner neighbours survive so a charm cut by the frame
    // still reaches that edge.
    for(let x=0;x<w;x++){if(mask[x]&&h>1&&eroded[w+x])eroded[x]=1;if(mask[(h-1)*w+x]&&h>1&&eroded[(h-2)*w+x])eroded[(h-1)*w+x]=1;}
    for(let y=0;y<h;y++){if(mask[y*w]&&w>1&&eroded[y*w+1])eroded[y*w]=1;if(mask[y*w+w-1]&&w>1&&eroded[y*w+w-2])eroded[y*w+w-1]=1;}
    for(let y=0;y<h;y++)for(let x=0;x<w;x++){if(!eroded[y*w+x])continue;for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){const nx=x+dx,ny=y+dy;if(nx>=0&&ny>=0&&nx<w&&ny<h&&mask[ny*w+nx])out[ny*w+nx]=1;}}
    return out;
  }

  // Where is the complete charm in this photograph? `focus` is the AI's box in
  // fractions of the full image. Returns a box that covers the whole charm
  // (the AI box joined with the measured shape when the measurement is
  // trustworthy) and which photo edges cut the charm itself.
  function measureProduct(image,focus,options={}){
    const result={version:VERSION,confident:false,box:validBox(focus)?{x:focus.x,y:focus.y,width:focus.width,height:focus.height}:null,measured:null,cut:{top:false,right:false,bottom:false,left:false},cutEdges:[],reason:null};
    if(!validBox(focus)){result.reason='No charm position is known for this photo.';return result;}
    const g=grid(image,options.maxSide||256),{w,h}=g;
    const bx0=clamp(Math.floor(focus.x*w),0,w-1),by0=clamp(Math.floor(focus.y*h),0,h-1),bx1=clamp(Math.ceil((focus.x+focus.width)*w),bx0+1,w),by1=clamp(Math.ceil((focus.y+focus.height)*h),by0+1,h),bw=bx1-bx0,bh=by1-by0,size=Math.max(bw,bh);
    // Surface samples: a ring around the AI box. When the box fills most of the
    // photo, fall back to the photo's outer border.
    const ringOuter=Math.round(size*.8),ringInner=Math.round(size*.25),ring=[];
    for(let y=Math.max(0,by0-ringOuter);y<Math.min(h,by1+ringOuter);y++)for(let x=Math.max(0,bx0-ringOuter);x<Math.min(w,bx1+ringOuter);x++)if(x<bx0-ringInner||x>=bx1+ringInner||y<by0-ringInner||y>=by1+ringInner)ring.push(y*w+x);
    if(ring.length<Math.max(60,w*h*.04)){ring.length=0;const m=Math.max(1,Math.round(Math.min(w,h)*.06));for(let y=0;y<h;y++)for(let x=0;x<w;x++)if(x<m||y<m||x>=w-m||y>=h-m)ring.push(y*w+x);}
    // Seeds come from the centre of the AI box, where the charm most likely is.
    const cx0=bx0+Math.floor(bw*.2),cy0=by0+Math.floor(bh*.2),cx1=Math.max(cx0+1,bx1-Math.floor(bw*.2)),cy1=Math.max(cy0+1,by1-Math.floor(bh*.2)),core=[],seeds=[];
    for(let y=cy0;y<cy1;y++)for(let x=cx0;x<cx1;x++)core.push(y*w+x);
    const surface=surfaceModel(g,ring,core),mask=new Uint8Array(w*h);
    for(let o=0;o<w*h;o++)if(surface.distance(o)>surface.threshold)mask[o]=1;
    const solid=open(mask,w,h);
    for(const o of core)if(solid[o])seeds.push(o);
    if(seeds.length<Math.max(4,(cx1-cx0)*(cy1-cy0)*.02)){result.reason='The charm does not stand out from its surroundings clearly enough to measure.';return result;}
    // Grow the charm from its seeds, no further than one box size beyond the
    // AI box. Reaching that limit inside the photo means the shape blended into
    // the scenery, so the measurement is not trusted.
    const reach=Math.round(size*.9),lx0=Math.max(0,bx0-reach),ly0=Math.max(0,by0-reach),lx1=Math.min(w,bx1+reach),ly1=Math.min(h,by1+reach);
    const seen=new Uint8Array(w*h),queue=seeds.slice();seeds.forEach(o=>seen[o]=1);
    let minX=w,minY=h,maxX=-1,maxY=-1,leak=false,count=0;const edgeRuns={top:new Set(),bottom:new Set(),left:new Set(),right:new Set()};
    for(let q=0;q<queue.length;q++){const o=queue[q],x=o%w,y=(o-x)/w;count++;
      if(x<minX)minX=x;if(x>maxX)maxX=x;if(y<minY)minY=y;if(y>maxY)maxY=y;
      if(y===0)edgeRuns.top.add(x);if(y===h-1)edgeRuns.bottom.add(x);if(x===0)edgeRuns.left.add(y);if(x===w-1)edgeRuns.right.add(y);
      if((x===lx0&&lx0>0)||(x===lx1-1&&lx1<w)||(y===ly0&&ly0>0)||(y===ly1-1&&ly1<h))leak=true;
      for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){if(!dx&&!dy)continue;const nx=x+dx,ny=y+dy;if(nx<lx0||ny<ly0||nx>=lx1||ny>=ly1)continue;const n=ny*w+nx;if(!seen[n]&&solid[n]){seen[n]=1;queue.push(n);}}
    }
    const mw=maxX-minX+1,mh=maxY-minY+1;
    if(leak){result.reason='The charm blends into the scenery, so its full outline could not be measured.';return result;}
    if(count<bw*bh*.08){result.reason='Too little of the charm could be separated from the background to measure it.';return result;}
    if(mw*mh>bw*bh*6){result.reason='The measured shape is much larger than the charm, so it was not trusted.';return result;}
    // A charm that meets a photo edge along a wide stretch is cut off by the
    // photo itself. A chain or cord leaving the frame is narrow and removed
    // by the opening above, so it does not count.
    const across={top:mw,bottom:mw,left:mh,right:mh};
    for(const edge of ['top','right','bottom','left'])if(edgeRuns[edge].size>=Math.max(2,across[edge]*.2)){result.cut[edge]=true;result.cutEdges.push(edge);}
    const pad=Math.max(1,Math.round(Math.max(mw,mh)*.03));
    const measured={x:Math.max(0,minX-pad)/w,y:Math.max(0,minY-pad)/h,width:(Math.min(w,maxX+1+pad)-Math.max(0,minX-pad))/w,height:(Math.min(h,maxY+1+pad)-Math.max(0,minY-pad))/h};
    const box=union(result.box,measured);
    Object.assign(result,{confident:true,measured,box:{x:clamp(box.x,0,1),y:clamp(box.y,0,1),width:Math.min(box.width,1-clamp(box.x,0,1)),height:Math.min(box.height,1-clamp(box.y,0,1))},reason:result.cutEdges.length?'The photo itself cuts off the charm at the '+joinEdges(result.cutEdges)+'.':null});
    return result;
  }

  // Declared axis-aligned bounds of an editor layer, honouring its origin.
  // Fabric 7 treats a missing origin as the centre.
  function layerRect(o){
    const sx=Math.abs(o.scaleX==null?1:Number(o.scaleX)),sy=Math.abs(o.scaleY==null?1:Number(o.scaleY));
    const isText='text'in o&&o.type!=='Group';
    const w=(Number(o.width)||0)*sx,h=(Number(isText&&finite(o.aiBoxHeight)?o.aiBoxHeight:o.height)||0)*sy;
    let left=Number(o.left)||0,top=Number(o.top)||0;const ox=o.originX==null?'center':o.originX,oy=o.originY==null?'center':o.originY;
    if(ox==='center')left-=w/2;else if(ox==='right')left-=w;
    if(oy==='center')top-=h/2;else if(oy==='bottom')top-=h;
    const angle=((Number(o.angle)||0)%360+360)%360;
    if(angle>.5&&angle<359.5){const r=angle*Math.PI/180,c=Math.abs(Math.cos(r)),s=Math.abs(Math.sin(r)),cx=left+w/2,cy=top+h/2,W=w*c+h*s,H=w*s+h*c;return {left:cx-W/2,top:cy-H/2,width:W,height:H,rotated:true};}
    return {left,top,width:w,height:h};
  }
  const isImage=o=>String(o?.type).toLowerCase()==='image';
  const COPY_ROLES=['headline','description','brand','button'];
  // The product photograph: the layer marked as the photo, else the largest
  // image that is not a brand asset or a decorative surface.
  function productLayer(objects,brandKeys){
    const photos=objects.filter(o=>isImage(o)&&o.editorRole!=='shape'&&o.editorRole!=='brand'&&!(brandKeys&&brandKeys.has(o.sourceKey)));
    return photos.find(o=>o.editorRole==='photo')||photos.sort((a,b)=>b.width*b.height*Math.abs(b.scaleX||1)*Math.abs(b.scaleY||1)-a.width*a.height*Math.abs(a.scaleX||1)*Math.abs(a.scaleY||1))[0]||null;
  }
  // Where the charm lands on the ad: the charm box mapped through the photo
  // layer's crop and scale, plus how much of it the layer and the ad show.
  function subjectOnBoard(photo,source,box,board){
    if(!photo||!source||!validBox(box))return null;
    const iw=Number(source.width),ih=Number(source.height);if(!(iw>0&&ih>0))return null;
    const r=layerRect(photo);if(r.rotated)return {unsupported:'The product photo is rotated, so its charm position cannot be checked.'};
    const sx=Math.abs(Number(photo.scaleX)||1),sy=Math.abs(Number(photo.scaleY)||1),cropX=Number(photo.cropX)||0,cropY=Number(photo.cropY)||0,vw=Number(photo.width)||iw,vh=Number(photo.height)||ih;
    const s0x=box.x*iw,s0y=box.y*ih,s1x=(box.x+box.width)*iw,s1y=(box.y+box.height)*ih;
    const flipX=!!photo.flipX,flipY=!!photo.flipY;
    const mapX=v=>flipX?r.left+(vw-(v-cropX))*sx:r.left+(v-cropX)*sx,mapY=v=>flipY?r.top+(vh-(v-cropY))*sy:r.top+(v-cropY)*sy;
    const full={left:Math.min(mapX(s0x),mapX(s1x)),top:Math.min(mapY(s0y),mapY(s1y))};full.width=Math.abs(mapX(s1x)-mapX(s0x));full.height=Math.abs(mapY(s1y)-mapY(s0y));
    // Visible part: inside the photo layer's own crop window and inside the ad.
    const vx0=Math.max(full.left,r.left,0),vy0=Math.max(full.top,r.top,0),vx1=Math.min(full.left+full.width,r.left+r.width,board.width),vy1=Math.min(full.top+full.height,r.top+r.height,board.height);
    const visible=vx1>vx0&&vy1>vy0?{left:vx0,top:vy0,width:vx1-vx0,height:vy1-vy0}:null;
    const tolX=Math.max(.75,full.width*.01),tolY=Math.max(.75,full.height*.01),edges=[];
    if(!visible)edges.push('top','right','bottom','left');
    else{if(visible.top-full.top>tolY)edges.push('top');if(full.left+full.width-(visible.left+visible.width)>tolX)edges.push('right');if(full.top+full.height-(visible.top+visible.height)>tolY)edges.push('bottom');if(visible.left-full.left>tolX)edges.push('left');}
    return {full,visible,edges,visibleFraction:visible?visible.width*visible.height/(full.width*full.height):0};
  }
  const overlap=(a,b)=>{const w=Math.min(a.left+a.width,b.left+b.width)-Math.max(a.left,b.left),h=Math.min(a.top+a.height,b.top+b.height)-Math.max(a.top,b.top);return w>0&&h>0?{width:w,height:h,area:w*h}:null;};

  // Check one ad size. `sources` maps sourceKey to {width,height,focus,
  // focusCheck}; focusCheck (from measureProduct) supplies the verified charm
  // box and whether the photo itself cuts the charm. `options.rects` may map a
  // layer id to its rendered bounds (the browser measures fitted text).
  function checkDocument(doc,board,sources,options={}){
    const objects=Array.isArray(doc?.objects)?doc.objects:[],lookup=sources instanceof Map?sources:new Map((sources||[]).map(s=>[s.id,s]));
    const brandKeys=options.brandKeys instanceof Set?options.brandKeys:new Set(options.brandKeys||[]);
    const label=options.label||board.key||(board.width+'x'+board.height),issues=[],warnings=[];
    const photo=productLayer(objects,brandKeys);
    if(!photo)return {version:VERSION,key:label,ok:false,issues:[{kind:'missing',message:'No product photo is in this size.'}],warnings};
    const source=lookup.get(photo.sourceKey),check=source?.focusCheck;
    const box=validBox(check?.box)?check.box:validBox(source?.focus)?source.focus:null;
    if(!box)return {version:VERSION,key:label,ok:true,verified:false,issues,warnings:[{kind:'unverified',message:'The charm position in this photo is unknown, so this size was not checked.'}]};
    if(check?.cutEdges?.length)issues.push({kind:'source-cut',edges:check.cutEdges.slice(),sourceKey:photo.sourceKey,message:'The photo itself cuts off the charm at the '+joinEdges(check.cutEdges)+'.'});
    const where=subjectOnBoard(photo,source,box,board);
    if(!where)return {version:VERSION,key:label,ok:!issues.length,verified:false,issues,warnings:[{kind:'unverified',message:'The photo size is unknown, so this size was not checked.'}]};
    if(where.unsupported)return {version:VERSION,key:label,ok:!issues.length,verified:false,issues,warnings:[{kind:'unverified',message:where.unsupported}]};
    if(where.edges.length)issues.push({kind:'cut',edges:where.edges,message:'Charm cut off at the '+joinEdges(where.edges)+'.'});
    // Text, the button and the logo must stay clear of the charm.
    const charm=where.visible||where.full,covering=[];
    for(const o of objects){
      if(o===photo||o.visible===false||!(COPY_ROLES.includes(o.editorRole)||'text'in o||(isImage(o)&&brandKeys.has(o.sourceKey))))continue;
      if(o.editorRole==='shape')continue;
      const rect=options.rects&&o.id&&options.rects[o.id]||layerRect(o),hit=overlap(rect,charm);
      if(hit&&hit.width>1.5&&hit.height>1.5&&hit.area>charm.width*charm.height*.005)covering.push(o.id||o.name||o.editorRole||'layer');
    }
    if(covering.length)issues.push({kind:'covered',layers:covering,message:(covering.length>1?'Text or the logo covers':'A text or logo layer covers')+' the charm.'});
    const clearance=Math.min(charm.left,charm.top,board.width-(charm.left+charm.width),board.height-(charm.top+charm.height));
    if(!where.edges.length&&clearance<Math.min(board.width,board.height)*.01)warnings.push({kind:'tight',message:'The charm touches the edge of the ad.'});
    if(check&&!check.confident&&check.reason)warnings.push({kind:'unconfirmed',message:check.reason});
    return {version:VERSION,key:label,ok:!issues.length,verified:true,charm:where.full,visible:where.visible,visibleFraction:where.visibleFraction,issues,warnings};
  }

  // One plain sentence per failing size, for status lines and save prompts.
  function describe(results){
    const failing=(results||[]).filter(r=>r&&!r.ok);
    if(!failing.length)return {ok:true,failing:[],message:'The complete charm is visible and clear of text in every size.'};
    return {ok:false,failing:failing.map(r=>r.key),message:failing.length+' ad size'+(failing.length>1?'s need':' needs')+' a fix: '+failing.map(r=>r.key+' ('+r.issues.map(i=>i.message.replace(/\.$/,'').toLowerCase()).join('; ')+')').join(', ')+'.'};
  }

  const api={VERSION,measureProduct,checkDocument,describe,layerRect,subjectOnBoard,productLayer,grid};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdPlacement=api;
})(typeof window==='object'?window:globalThis);
