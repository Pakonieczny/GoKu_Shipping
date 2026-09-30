// Deterministic close framing for the motion masters. The film model does not reliably obey "frame the piece large": a
// landscape master can hold the charm at a fifth of the frame height however the prompt is worded. Nothing here calls
// a provider. The charm is found in the master's own pixels (the measurement that already checks the static ads,
// brites-ad-placement.js, started from the product box the layout stage saved) and, when it is smaller than its format
// calls for, the one fixed crop the composition applies is closed in about it. The crop never moves during the film.
const clamp=(v,a,b)=>Math.min(b,Math.max(a,v)),pct=v=>Math.round(v*100);
const CLOSE={
 maxZoom:2,     // never enlarge the master more than this: past it the upscale shows
 minSamples:3,  // sampled frames in which the charm must be found, and at least half of all of them
 edge:.012,     // a measured product box this close to a frame side already runs out of the frame there (a hanging chain)
 flag:.52,      // still below this share after every enlargement allowed: the film is reported as too small
 // Shares are of the measured charm box, which carries a thin margin around the visible metal, so the visible charm reads
 // about 15% smaller: landscape 66% aims for a visible 55-60% of the frame height, as the film prompt asks.
 // axis: what the film prompt sizes the charm by (height of the frame, or width for portrait). low: enlarge only below it.
 // target: the share an enlargement aims for. at: where the charm's centre sits in the window (fractions from the left
 // and top), keeping the caption area open. cap: the most of each dimension the charm may fill, so the copy keeps room.
 landscape:{axis:'h',low:.58,target:.66,at:[.70,.50],cap:{w:.50,h:.75}},
 square:{axis:'h',low:.55,target:.62,at:[.62,.62],cap:{w:.70,h:.70}},
 portrait:{axis:'w',low:.58,target:.70,at:[.50,.62],cap:{w:.75,h:.56}}
};
const median=v=>{const s=[...v].sort((a,b)=>a-b),i=s.length>>1;return s.length%2?s[i]:(s[i-1]+s[i])/2;};
// Where the charm's own body is, over raw sampled frames ({data,width,height,channels}) and the product box saved by the
// layout stage ({x,y,w,h} as fractions of the frame). Thin chains and cords are excluded by the measurement itself.
// Returns null unless the charm is found consistently in most of the frames.
function charmBody(frames,focus,options={}){
 if(!Array.isArray(frames)||!frames.length||!focus||['x','y','w','h'].some(k=>!Number.isFinite(focus[k])))return null;
 const placement=require('../../brites-ad-placement'),box={x:focus.x,y:focus.y,width:focus.w,height:focus.h},found=[];
 for(const frame of frames){
  let r;try{r=placement.measureProduct(frame,box,{maxSide:options.maxSide||320});}catch{continue;}
  const m=r&&r.confident?r.measured:null;if(m&&m.width>0&&m.height>0)found.push({x:m.x,y:m.y,w:m.width,h:m.height});
 }
 const need=Math.max(CLOSE.minSamples,Math.ceil(frames.length/2));if(found.length<need)return null;
 const centre=f=>[f.x+f.w/2,f.y+f.h/2],mw=median(found.map(f=>f.w)),mh=median(found.map(f=>f.h)),mx=median(found.map(f=>centre(f)[0])),my=median(found.map(f=>centre(f)[1]));
 // A frame that disagrees with the rest (a rock or a shadow taken for the charm) is dropped, never averaged in.
 const agree=found.filter(f=>f.w>=mw*.5&&f.w<=mw*2&&f.h>=mh*.5&&f.h<=mh*2&&Math.abs(centre(f)[0]-mx)<=Math.max(.06,mw)&&Math.abs(centre(f)[1]-my)<=Math.max(.06,mh));
 if(agree.length<need)return null;
 const x0=clamp(Math.min(...agree.map(f=>f.x)),0,1),y0=clamp(Math.min(...agree.map(f=>f.y)),0,1),x1=clamp(Math.max(...agree.map(f=>f.x+f.w)),0,1),y1=clamp(Math.max(...agree.map(f=>f.y+f.h)),0,1);
 return {x:x0,y:y0,w:x1-x0,h:y1-y0,mw:median(agree.map(f=>f.w)),mh:median(agree.map(f=>f.h)),n:agree.length,of:frames.length};
}
const shareOf=(spec,body,crop)=>spec.axis==='h'?body.mh/crop.h:body.mw/crop.w;
// Close the fixed crop in about the measured charm when it is below its format's size band. `crop` is the window the
// composition already chose, `subject` the product box (its `body` is the pixel measurement) and `base` the format's
// untightened window, from which the enlargement limit is counted. Returns {crop,moved,framing}; framing is null when
// there was nothing to judge (no measurement, or one that does not describe the product box).
function closeUp(format,crop,subject,base){
 const spec=CLOSE[format?.key],body=subject&&subject.body,none={crop,moved:false,framing:null};
 if(!spec||!body||!(body.n>=CLOSE.minSamples)||!(body.mw>0&&body.mh>0))return none;
 const cx=body.x+body.w/2,cy=body.y+body.h/2;
 // The pixel measurement must agree with the saved product box: the charm is most of its width and sits inside it.
 // A pendant on a long necklace is not (its box is wide), so a whole necklace is never cropped down to its pendant.
 if(body.w<subject.w*.45||cx<subject.x||cx>subject.x+subject.w||cy<subject.y||cy>subject.y+subject.h)return none;
 const name=spec.axis==='h'?'height':'width',before=shareOf(spec,body,crop);
 const result=(framed,moved)=>{
  const after=shareOf(spec,body,framed),zoom=base.w/framed.w,small=after<CLOSE.flag;
  // Enlarged, or still small after every enlargement allowed: say so, so the complete-ad review sees it and can send the film back.
  const note=moved||small?format.key+' film: the charm fills about '+pct(before)+'% of the '+name+' of the generated film'+(moved?'; the film was enlarged '+zoom.toFixed(1)+'x about the charm to about '+pct(after)+'%':'')+(small?(moved?', which is still small and distant':', and could not be enlarged enough')+'. The charm needs to be framed close in this film.':'.'):null;
  return {crop:framed,moved,framing:{status:small?'too-small':moved?'zoomed':'ok',before,after,zoom,...(note?{note}:{})}};
 };
 if(before>=spec.low)return result(crop,false);
 // The window must keep the whole jewelry. Where the product box already runs out of the frame on a side (a chain hanging
 // from the top edge), that side is free and only the charm itself is held there, with a margin for its sway.
 const px=Math.max(.015,body.w*.06),py=Math.max(.015,body.h*.06),E=CLOSE.edge;
 const hold={x0:Math.min(subject.x<=E?1:subject.x,body.x-px),x1:Math.max(subject.x+subject.w>=1-E?0:subject.x+subject.w,body.x+body.w+px),y0:Math.min(subject.y<=E?1:subject.y,body.y-py),y1:Math.max(subject.y+subject.h>=1-E?0:subject.y+subject.h,body.y+body.h+py)};
 for(const key of Object.keys(hold))hold[key]=clamp(hold[key],0,1);
 let k=before/spec.target;                                                            // window shrink that reaches the target share
 k=Math.max(k,base.w/CLOSE.maxZoom/crop.w,base.h/CLOSE.maxZoom/crop.h);               // never past the largest enlargement
 k=Math.max(k,(hold.x1-hold.x0)/crop.w,(hold.y1-hold.y0)/crop.h);                     // the jewelry stays whole
 k=Math.max(k,body.mw/(spec.cap.w*crop.w),body.mh/(spec.cap.h*crop.h));               // the charm leaves the copy its room
 if(!(k<.95))return result(crop,false);
 const w=crop.w*k,h=crop.h*k,put=(c,a,size,lo,hi)=>{const from=Math.max(0,hi-size),to=Math.min(lo,1-size);return from>to?null:clamp(c-size*a,from,to);};
 const x=put(cx,spec.at[0],w,hold.x0,hold.x1),y=put(cy,spec.at[1],h,hold.y0,hold.y1);
 if(x===null||y===null)return result(crop,false);
 return result({...crop,x,y,w,h},true);
}
// The part of a box that lies inside a window, in master fractions. A close crop may cut the chain that already ran out of frame.
function within(box,win){
 const x0=Math.max(box.x,win.x),y0=Math.max(box.y,win.y),x1=Math.min(box.x+box.w,win.x+win.w),y1=Math.min(box.y+box.h,win.y+win.h);
 return x1>x0&&y1>y0?{...box,x:x0,y:y0,w:x1-x0,h:y1-y0}:box;
}
module.exports={CLOSE,charmBody,closeUp,within};
