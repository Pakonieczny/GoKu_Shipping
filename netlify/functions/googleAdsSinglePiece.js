// One product per film. A catalog photograph can show several pieces side by
// side (two necklaces, a pair of earrings, a set). Sent as the identity
// reference it invites the film model to reproduce all of them. When the
// photograph is a clean studio shot, this finds separate pieces by the empty
// gaps between them and keeps only the most prominent one. It is free (local
// pixels only), conservative, and never crops a busy scene or a single piece.
const sharp=require('sharp');
const SIZE=240,INK=40,GAP=.06,MIN_SHARE=.15,MIN_SPAN=.3;

// Split a 1-D ink profile at empty runs at least GAP of its length wide.
function segments(profile){
 const limit=Math.max(1,Math.round(profile.length*GAP)),out=[];let start=null,empty=0;
 for(let i=0;i<=profile.length;i++){
  const filled=i<profile.length&&profile[i]>0;
  if(filled){if(start===null)start=i;empty=0;continue;}
  empty++;
  if(start!==null&&(i===profile.length||empty>=limit)){const end=i-empty+1;out.push({start,end,mass:profile.slice(start,end).reduce((a,b)=>a+b,0)});start=null;}
 }
 return out;
}
function pick(list,length,total){
 const real=list.filter(s=>s.mass>=total*MIN_SHARE);if(real.length<2)return null;
 const centre=length/2,best=[...real].sort((a,b)=>b.mass-a.mass);
 const lead=best[0],close=best.filter(s=>s.mass>=lead.mass*.85).sort((a,b)=>Math.abs((a.start+a.end)/2-centre)-Math.abs((b.start+b.end)/2-centre))[0];
 return {chosen:close,real};
}
// Returns {buffer,pieces,box} when the photograph holds several separate pieces on a plain background, otherwise null.
async function isolate(buffer){
 let raw,info;
 try{({data:raw,info}=await sharp(buffer).rotate().resize({width:SIZE,height:SIZE,fit:'inside'}).removeAlpha().raw().toBuffer({resolveWithObject:true}));}catch{return null;}
 const {width:w,height:h}=info,px=(x,y)=>(y*w+x)*3;
 const edge=[];for(let x=0;x<w;x++)for(const y of [0,1,h-2,h-1])edge.push(px(x,y));for(let y=0;y<h;y++)for(const x of [0,1,w-2,w-1])edge.push(px(x,y));
 const bg=[0,1,2].map(c=>{const v=edge.map(i=>raw[i+c]).sort((a,b)=>a-b);return v[v.length>>1];});
 const differs=i=>Math.max(Math.abs(raw[i]-bg[0]),Math.abs(raw[i+1]-bg[1]),Math.abs(raw[i+2]-bg[2]))>INK;
 // A busy scene or mannequin set has no plain background to separate against.
 if(edge.filter(differs).length>edge.length*.06)return null;
 const cols=new Array(w).fill(0),rows=new Array(h).fill(0);let total=0;
 for(let y=0;y<h;y++)for(let x=0;x<w;x++)if(differs(px(x,y))){cols[x]++;rows[y]++;total++;}
 if(total<w*h*.004)return null;
 for(const axis of ['x','y']){
  const profile=axis==='x'?cols:rows,length=profile.length,found=pick(segments(profile),length,total);
  if(!found)continue;
  const {chosen,real}=found,before=real.filter(s=>s.end<=chosen.start).pop(),after=real.find(s=>s.start>=chosen.end),
   room=Math.round(length*.05),lo=Math.max(before?Math.floor((before.end+chosen.start)/2):0,chosen.start-room),hi=Math.min(after?Math.ceil((chosen.end+after.start)/2):length,chosen.end+room);
  if((hi-lo)/length<MIN_SPAN)continue;
  const full=await sharp(buffer).rotate().metadata(),W=full.width,H=full.height,scale=(axis==='x'?W:H)/length;
  const from=Math.max(0,Math.floor(lo*scale)),size=Math.min((axis==='x'?W:H)-from,Math.ceil((hi-lo)*scale));
  const region=axis==='x'?{left:from,top:0,width:size,height:H}:{left:0,top:from,width:W,height:size};
  return {buffer:await sharp(buffer).rotate().extract(region).jpeg({quality:92}).toBuffer(),pieces:real.length,axis,box:region};
 }
 return null;
}
// The instruction that accompanies every film request, and the reviewer's rule.
const FILM_RULE=`ONE PIECE ONLY.
The whole film shows exactly one piece of jewelry: one charm on one chain, or one necklace. Never two, never a pair, never a set, never side by side, never a duplicate of it.
No second separate necklace, chain, charm, earring, ring or bracelet anywhere in the frame, near or far, in focus or blurred, on the scenery, on a stand or on a model.
A chain or cord that the piece itself hangs from is part of that same one piece, not a second item: only a second, separate item of jewelry is forbidden. A charm that the catalog photograph shows without a chain is still filmed without one.
No grid, no collage, no split frame, no repeated copies. If the attached photograph shows more than one item, film only the most central, most prominent one and ignore the others completely.
The wide frame is filmed with one piece in it, and the area beyond its middle holds none either. Every frame from the first to the last has this one piece and no other jewelry.`;
const REVIEW_RULE=' Exactly ONE piece of jewelry may appear in a film: one charm on one chain or one necklace. Set multipleProducts=true when any sampled frame shows two or more pieces (two necklaces or pendants side by side, a pair, a duplicate, a set, an extra separate chain, charm, earring, ring or bracelet in view, or a collage or grid of products), even when each piece looks faithful. Name the affected format in issues when you set it. A necklace and its own single pendant are one piece, and so is a charm with the chain or cord it hangs from; leaves, fabric, water, petals, grass or light moving gently around the piece are scenery, never extra jewelry, and never a reason to fail; a piece and its own reflection or shadow are not a second piece.';
module.exports={isolate,FILM_RULE,REVIEW_RULE};
