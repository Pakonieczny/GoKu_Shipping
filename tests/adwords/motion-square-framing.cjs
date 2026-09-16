// Square has no film of its own: it is cropped from a master. Cropping portrait
// keeps the full width, so the piece stays centred with open space above for the
// message; cropping landscape keeps the full height and leaves the piece pushed
// to the side it was staged on. These checks hold that difference, and hold the
// composition offset that reserves the caption area against the zoom.
const assert=require('assert');
const {geometry}=require('../../netlify/functions/googleAdsMotionComposition.js');
const SQ={key:'square',width:720,height:720};
const centre=p=>({x:p.x+p.w/2,y:p.y+p.h/2});

// Staged as each master is instructed to stage it.
const landscapeStaged={x:.58,y:.30,w:.22,h:.44};   // toward the right, left third clear
const portraitStaged={x:.30,y:.46,w:.40,h:.30};    // lower middle, upper third clear

{ // Portrait crops to a square that reads as a square.
  const g=geometry(SQ,portraitStaged,'portrait',{mode:'full'}),c=centre(g.product);
  assert(Math.abs(c.x-.5)<=.08,'a square from portrait keeps the piece horizontally centred, was '+c.x.toFixed(2));
  assert(c.y>=.55,'a square from portrait keeps the piece low, leaving the space above for the message: '+c.y.toFixed(2));
  const top=g.zones.find(z=>z.name==='top');
  assert(top&&top.h>=140,'that space above is a real caption zone, was '+(top?Math.round(top.h):0)+'px');
}
{ // Landscape keeps its own staging, which is why it is the fallback and not the default.
  const g=geometry(SQ,landscapeStaged,'landscape',{mode:'full'}),c=centre(g.product);
  assert(c.x>.58,'a square from landscape leaves the piece off to its staged side: '+c.x.toFixed(2));
}
// The zoom must not quietly recentre the frame: the offset that reserves the
// caption area has to survive it, in every format and both crop modes.
for(const [src,staged] of [['landscape',landscapeStaged],['portrait',portraitStaged]]){
 for(const f of [SQ,{key:'portrait',width:720,height:1280},{key:'landscape',width:1280,height:720}]){
  for(const mode of ['full','band']){
   const g=geometry(f,staged,src,{mode}),p=g.product;
   assert(p.x>=-.002&&p.y>=-.002&&p.x+p.w<=1.002&&p.y+p.h<=1.002,f.key+'/'+src+'/'+mode+' keeps the whole piece in frame');
   const film=g.hero?{w:g.hero.w/f.width,h:g.hero.h/f.height}:{w:1,h:1};
   assert(Math.max(p.w/film.w,p.h/film.h)>=.5,f.key+'/'+src+'/'+mode+' still frames the piece close');
  }
 }
}
{ // A piece staged off-centre stays off-centre after the zoom, rather than being pulled to the middle.
  const g=geometry({key:'landscape',width:1280,height:720},landscapeStaged,'landscape',{mode:'full'});
  assert(centre(g.product).x>.55,'landscape keeps its right-side staging through the zoom, so the left third stays clear');
}
console.log('PASS square is framed as a square, and the zoom preserves the reserved caption area');
