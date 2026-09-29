/* Charm placement check for every ad size. It confirms where the charm really
 * is in each photograph (the AI's box is only a starting point), then checks
 * each layout keeps the complete charm inside the ad and clear of text,
 * buttons and the logo. Pure geometry and pixel counting: no provider
 * requests, no charges. Shared by the browser editor and the Netlify design
 * engine, so a size that fails here fails everywhere. */
(function(root){
  'use strict';
  const VERSION=2;
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  const finite=v=>typeof v==='number'&&Number.isFinite(v);
  const validBox=b=>!!b&&['x','y','width','height'].every(k=>finite(b[k]))&&b.width>0&&b.height>0;
  const union=(a,b)=>{const x=Math.min(a.x,b.x),y=Math.min(a.y,b.y);return {x,y,width:Math.max(a.x+a.width,b.x+b.width)-x,height:Math.max(a.y+a.height,b.y+b.height)-y};};
  const EDGE_NAMES={top:'top',right:'right side',bottom:'bottom',left:'left side'};
  const joinEdges=edges=>{const n=edges.map(e=>EDGE_NAMES[e]||e);return n.length<2?n.join(''):n.slice(0,-1).join(', ')+' and '+n[n.length-1];};

  // sRGB 0-255 to linear light once, then CIE Lab. Distances in Lab track what
  // a person sees, so gold on pale fabric and silver on dark wood both read as
  // clearly different.
  const LINEAR=new Float32Array(256);for(let i=0;i<256;i++){const v=i/255;LINEAR[i]=v<=.04045?v/12.92:((v+.055)/1.055)**2.4;}
  const labF=t=>t>.008856?Math.cbrt(t):7.787*t+16/116;

  // Average the photo down to a small Lab grid. Callers may pass a pre-shrunk
  // image (browser canvas, sharp resize); large inputs are shrunk here.
  function grid(image,maxSide=256){
    const width=Number(image?.width),height=Number(image?.height),data=image?.data;
    if(!data||!(width>0)||!(height>0))throw Error('Photo pixels are unavailable for the charm check.');
    const channels=Number(image.channels)||Math.round(data.length/(width*height));
    if(channels<3)throw Error('The charm check needs a colour photograph.');
    if(data.length<width*height*channels)throw Error('The photo pixels are incomplete, so the charm check cannot read them.');
    const s=Math.min(1,maxSide/Math.max(width,height)),w=Math.max(1,Math.round(width*s)),h=Math.max(1,Math.round(height*s)),n=w*h;
    const bytes=data instanceof Uint8Array||data instanceof Uint8ClampedArray,lin=v=>LINEAR[bytes?v:clamp(Math.round(v)||0,0,255)];
    const R=new Float32Array(n),G=new Float32Array(n),Bl=new Float32Array(n),count=new Uint32Array(n),col=new Int32Array(width);
    for(let x=0;x<width;x++)col[x]=Math.min(w-1,Math.floor(x*w/width));
    for(let y=0;y<height;y++){const row=Math.min(h-1,Math.floor(y*h/height))*w;for(let x=0,i=y*width*channels;x<width;x++,i+=channels){const o=row+col[x];R[o]+=lin(data[i]);G[o]+=lin(data[i+1]);Bl[o]+=lin(data[i+2]);count[o]++;}}
    const L=new Float32Array(n),A=new Float32Array(n),B=new Float32Array(n);
    for(let o=0;o<n;o++){const c=count[o]||1,r=R[o]/c,g=G[o]/c,b=Bl[o]/c,X=labF((r*.4124+g*.3576+b*.1805)/.95047),Y=labF(r*.2126+g*.7152+b*.0722),Z=labF((r*.0193+g*.1192+b*.9505)/1.08883);L[o]=116*Y-16;A[o]=500*(X-Y);B[o]=200*(Y-Z);}
    return {w,h,L,A,B};
  }

  // A few colour centres describe the photo around the charm (surface, shadow,
  // props) and inside the AI box. A colour far more common inside the box than
  // around it is the charm's own, so it never counts as surface even when a
  // misplaced box leaves part of the charm in the surrounding ring. k-means over
  // a bounded sample keeps this fast.
  const WL=.7;
  function surfaceModel(g,ring,core){
    const pick=(list,limit)=>{const out=[],step=Math.max(1,list.length/limit);for(let i=0;i<list.length;i+=step)out.push(list[i|0]);return out;};
    const rs=pick(ring,3000),cs=pick(core,1200),sample=rs.concat(cs),K=Math.min(6,sample.length),cL=new Float64Array(K),cA=new Float64Array(K),cB=new Float64Array(K),label=new Uint8Array(sample.length);
    for(let c=0;c<K;c++){const o=sample[Math.floor((c+.5)*sample.length/K)];cL[c]=g.L[o]*WL;cA[c]=g.A[o];cB[c]=g.B[o];}
    for(let it=0;it<9;it++){const sL=new Float64Array(K),sA=new Float64Array(K),sB=new Float64Array(K),n=new Float64Array(K);
      for(let i=0;i<sample.length;i++){const o=sample[i],l=g.L[o]*WL,a=g.A[o],b=g.B[o];let best=0,bd=Infinity;for(let c=0;c<K;c++){const d=(l-cL[c])**2+(a-cA[c])**2+(b-cB[c])**2;if(d<bd){bd=d;best=c;}}label[i]=best;sL[best]+=l;sA[best]+=a;sB[best]+=b;n[best]++;}
      if(it<8)for(let c=0;c<K;c++)if(n[c]){cL[c]=sL[c]/n[c];cA[c]=sA[c]/n[c];cB[c]=sB[c]/n[c];}}
    const ringShare=new Float64Array(K),coreShare=new Float64Array(K);
    for(let i=0;i<sample.length;i++)(i<rs.length?ringShare:coreShare)[label[i]]++;
    for(let c=0;c<K;c++){ringShare[c]/=Math.max(1,rs.length);coreShare[c]/=Math.max(1,cs.length);}
    let surface=[];for(let c=0;c<K;c++)if(ringShare[c]>=.03&&!(coreShare[c]>=ringShare[c]*2.5+.05))surface.push(c);
    if(!surface.length){let best=0;for(let c=1;c<K;c++)if(ringShare[c]>ringShare[best])best=c;surface=[best];}
    // Distance to the nearest surface colour. A darker pixel with the surface's
    // own hue is that surface in shadow, not the charm.
    const sL=surface.map(c=>cL[c]/WL),sA=surface.map(c=>cA[c]),sB=surface.map(c=>cB[c]);
    const distance=o=>{const l=g.L[o],a=g.A[o],b=g.B[o];let best=Infinity;
      for(let i=0;i<sL.length;i++){const dl=(l-sL[i])*WL,da=a-sA[i],db=b-sB[i];let d=Math.sqrt(dl*dl+da*da+db*db);
        if(l<sL[i]){const s=(l+16)/(sL[i]+16),ea=a-s*sA[i],eb=b-s*sB[i],ds=Math.sqrt(ea*ea+eb*eb)+Math.max(0,.84-s)*110;if(ds<d)d=ds;}
        if(d<best)best=d;}
      return best;};
    const spread=rs.map(distance).sort((a,b)=>a-b),T=clamp((spread[Math.floor(spread.length*.85)]||0)*2.4,12,40);
    return {distance,T,Tlo:Math.max(7,T*.55)};
  }

  // Where is the complete charm in this photograph? `focus` is the AI's box in
  // fractions of the full image. Returns a box that covers the whole charm
  // (the AI box joined with the measured shape when the measurement is
  // trustworthy) and which photo edges cut the charm itself.
  function measureProduct(image,focus,options={}){
    const result={version:VERSION,confident:false,box:validBox(focus)?{x:focus.x,y:focus.y,width:focus.width,height:focus.height}:null,measured:null,cut:{top:false,right:false,bottom:false,left:false},cutEdges:[],reason:null};
    const fail=reason=>{result.reason=reason;return result;};
    if(!validBox(focus))return fail('No charm position is known for this photo.');
    const g=grid(image,options.maxSide||256),{w,h}=g,N=w*h;
    const bx0=clamp(Math.floor(focus.x*w),0,w-1),by0=clamp(Math.floor(focus.y*h),0,h-1),bx1=clamp(Math.ceil((focus.x+focus.width)*w),bx0+1,w),by1=clamp(Math.ceil((focus.y+focus.height)*h),by0+1,h),bw=bx1-bx0,bh=by1-by0,size=Math.max(bw,bh);
    // Search window: the AI box and one and a quarter box sizes around it.
    const reach=Math.max(3,Math.round(size*1.25)),wx0=Math.max(0,bx0-reach),wy0=Math.max(0,by0-reach),wx1=Math.min(w,bx1+reach),wy1=Math.min(h,by1+reach);
    // Surface samples: a ring around the AI box, or the photo's outer border
    // when the box fills most of the photo. Seeds: the centre of the AI box.
    const inner=Math.max(1,Math.round(size*.35)),ring=[],core=[];
    for(let y=wy0;y<wy1;y++)for(let x=wx0;x<wx1;x++)if(x<bx0-inner||x>=bx1+inner||y<by0-inner||y>=by1+inner)ring.push(y*w+x);
    if(ring.length<Math.max(60,N*.03)){ring.length=0;const m=Math.max(1,Math.round(Math.min(w,h)*.06));for(let y=0;y<h;y++)for(let x=0;x<w;x++)if(x<m||y<m||x>=w-m||y>=h-m)ring.push(y*w+x);}
    const cx0=bx0+Math.floor(bw*.2),cy0=by0+Math.floor(bh*.2),cx1=Math.max(cx0+1,bx1-Math.floor(bw*.2)),cy1=Math.max(cy0+1,by1-Math.floor(bh*.2));
    for(let y=cy0;y<cy1;y++)for(let x=cx0;x<cx1;x++)core.push(y*w+x);
    const model=surfaceModel(g,ring,core),T=model.T,D=new Float32Array(N).fill(-1),dist=o=>D[o]>=0?D[o]:(D[o]=model.distance(o));
    const seeds=core.filter(o=>dist(o)>T);
    if(seeds.length<Math.max(2,core.length*.03))return fail('The charm does not stand out from its surroundings clearly enough to measure.');
    // The charm: clearly different pixels joined to the seeds (bridging
    // one-pixel gaps), plus a two-pixel rim of weaker ones for soft edges.
    const R=new Uint8Array(N),queue=new Int32Array(N),inWin=(x,y)=>x>=wx0&&x<wx1&&y>=wy0&&y<wy1;let qn=0;
    for(const o of seeds){R[o]=1;queue[qn++]=o;}
    for(let q=0;q<qn;q++){const o=queue[q],x=o%w,y=(o-x)/w;
      for(let dy=-2;dy<=2;dy++)for(let dx=-2;dx<=2;dx++){const nx=x+dx,ny=y+dy;if(!inWin(nx,ny))continue;const n=ny*w+nx;if(R[n]||dist(n)<=T)continue;R[n]=1;queue[qn++]=n;const m=(y+(dy/2|0))*w+x+(dx/2|0);if(!R[m])R[m]=2;}}
    let front=[];for(let y=wy0;y<wy1;y++)for(let x=wx0;x<wx1;x++)if(R[y*w+x])front.push(y*w+x);
    for(let layer=0;layer<2;layer++){const next=[];for(const o of front){const x=o%w,y=(o-x)/w;for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){const nx=x+dx,ny=y+dy;if(!inWin(nx,ny))continue;const n=ny*w+nx;if(!R[n]&&dist(n)>model.Tlo){R[n]=3;next.push(n);}}}front=next;}
    let rx0=w,ry0=h,rx1=-1,ry1=-1;for(let y=wy0;y<wy1;y++)for(let x=wx0;x<wx1;x++)if(R[y*w+x]){if(x<rx0)rx0=x;if(x>rx1)rx1=x;if(y<ry0)ry0=y;if(y>ry1)ry1=y;}
    // Chessboard distance to the region's outline. Beyond the photo edge counts
    // as charm (a charm cut by the frame keeps its thickness there); beyond the
    // search window it counts as background.
    const INF=30000,dt=new Int16Array(N),at=(x,y)=>x<0||y<0||x>=w||y>=h?INF:x<rx0||x>rx1||y<ry0||y>ry1?0:dt[y*w+x];
    for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++){const o=y*w+x;if(R[o])dt[o]=Math.min(INF,Math.min(at(x-1,y),at(x-1,y-1),at(x,y-1),at(x+1,y-1))+1);}
    for(let y=ry1;y>=ry0;y--)for(let x=rx1;x>=rx0;x--){const o=y*w+x;if(R[o])dt[o]=Math.min(dt[o],Math.min(at(x+1,y),at(x+1,y+1),at(x,y+1),at(x-1,y+1))+1);}
    let maxSeed=0;for(const o of seeds)if(dt[o]>maxSeed)maxSeed=dt[o];
    // Chains, cords, veins and shadows that carry the region to the edge of the
    // search window, or out of the photo through a thin contact, are thinner
    // than the charm. Erode by the smallest radius that separates the charm
    // from them, then restore the charm's outline at that radius.
    const thin=Math.max(1,Math.floor(maxSeed*.25)),edgePx=(x,y)=>x===0||y===0||x===w-1||y===h-1;
    const target=(o,x,y)=>(x===wx0&&wx0>0)||(x===wx1-1&&wx1<w)||(y===wy0&&wy0>0)||(y===wy1-1&&wy1<h)||(edgePx(x,y)&&dt[o]<=thin);
    const mark=new Int32Array(N);let stamp=0;
    const grow=r=>{stamp++;let n=0,leak=false;for(const o of seeds)if(dt[o]>r&&mark[o]!==stamp){mark[o]=stamp;queue[n++]=o;}
      for(let q=0;q<n;q++){const o=queue[q],x=o%w,y=(o-x)/w;if(!leak&&target(o,x,y))leak=true;
        for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){const nx=x+dx,ny=y+dy;if(nx<rx0||nx>rx1||ny<ry0||ny>ry1)continue;const m=ny*w+nx;if(dt[m]>r&&mark[m]!==stamp){mark[m]=stamp;queue[n++]=m;}}}
      return {n,leak};};
    const rMax=Math.max(1,Math.floor(maxSeed*.5));let r=0,grown;
    for(;;r++){grown=grow(r);if(!grown.n)return fail('The charm blends into the scenery, so its full outline could not be measured.');if(!grown.leak)break;if(r>=rMax)return fail('The charm blends into the scenery, so its full outline could not be measured.');}
    const body=new Uint8Array(N),coreList=queue.slice(0,grown.n);
    if(!r)for(const o of coreList)body[o]=1;
    else{// Restore the outline: region pixels within r of the eroded charm.
      const near=new Int16Array(N),nb=(x,y)=>x<rx0||x>rx1||y<ry0||y>ry1?INF:near[y*w+x];
      for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++)near[y*w+x]=INF;for(const o of coreList)near[o]=0;
      for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++){const o=y*w+x;if(near[o])near[o]=Math.min(INF,Math.min(nb(x-1,y),nb(x-1,y-1),nb(x,y-1),nb(x+1,y-1))+1);}
      for(let y=ry1;y>=ry0;y--)for(let x=rx1;x>=rx0;x--){const o=y*w+x;if(near[o])near[o]=Math.min(near[o],Math.min(nb(x+1,y),nb(x+1,y+1),nb(x,y+1),nb(x-1,y+1))+1);}
      for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++){const o=y*w+x;if(R[o]&&near[o]<=r)body[o]=1;}}
    let kx0=w,ky0=h,kx1=-1,ky1=-1;for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++)if(body[y*w+x]){if(x<kx0)kx0=x;if(x>kx1)kx1=x;if(y<ky0)ky0=y;if(y>ky1)ky1=y;}
    // What erosion removed: thin parts of the charm itself (jump ring, beak,
    // pointed tip) return; parts leading away (chains, props) return only
    // close to the charm.
    const final=body.slice(),k=Math.max(1,Math.round(Math.max(kx1-kx0+1,ky1-ky0+1)*.12)),seen=new Uint8Array(N);
    if(r)for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++){const s=y*w+x;if(!R[s]||body[s]||seen[s])continue;
      let n=0,away=false;seen[s]=1;queue[n++]=s;
      for(let q=0;q<n;q++){const o=queue[q],ox=o%w,oy=(o-ox)/w;if(!away&&(target(o,ox,oy)||dt[o]>r))away=true;
        for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){const nx=ox+dx,ny=oy+dy;if(nx<rx0||nx>rx1||ny<ry0||ny>ry1)continue;const m=ny*w+nx;if(R[m]&&!body[m]&&!seen[m]){seen[m]=1;queue[n++]=m;}}}
      for(let q=0;q<n;q++){const o=queue[q],ox=o%w,oy=(o-ox)/w;if(!away||ox>=kx0-k&&ox<=kx1+k&&oy>=ky0-k&&oy<=ky1+k)final[o]=1;}}
    let minX=w,minY=h,maxX=-1,maxY=-1,count=0,inside=0;
    for(let y=ry0;y<=ry1;y++)for(let x=rx0;x<=rx1;x++)if(final[y*w+x]){count++;if(x>=bx0&&x<bx1&&y>=by0&&y<by1)inside++;if(x<minX)minX=x;if(x>maxX)maxX=x;if(y<minY)minY=y;if(y>maxY)maxY=y;}
    const mw=maxX-minX+1,mh=maxY-minY+1;
    if(count<Math.max(6,bw*bh*.04))return fail('Too little of the charm could be separated from the background to measure it.');
    if(mw*mh>bw*bh*9)return fail('The measured shape is much larger than the charm, so it was not trusted.');
    if(inside<count*.1)return fail('The measured shape lies away from where the charm was expected, so it was not trusted.');
    // A charm that meets a photo edge along a wide stretch is cut off by the
    // photo itself. Chains leaving the frame were removed above.
    const contact={top:0,right:0,bottom:0,left:0},across={top:kx1-kx0+1,bottom:kx1-kx0+1,left:ky1-ky0+1,right:ky1-ky0+1};
    for(let x=kx0;x<=kx1;x++){if(ky0===0&&body[x]&&dist(x)>T)contact.top++;if(ky1===h-1&&body[(h-1)*w+x]&&dist((h-1)*w+x)>T)contact.bottom++;}
    for(let y=ky0;y<=ky1;y++){if(kx0===0&&body[y*w]&&dist(y*w)>T)contact.left++;if(kx1===w-1&&body[y*w+w-1]&&dist(y*w+w-1)>T)contact.right++;}
    for(const edge of ['top','right','bottom','left'])if(contact[edge]>=Math.max(2,Math.ceil(across[edge]*.15))){result.cut[edge]=true;result.cutEdges.push(edge);}
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
