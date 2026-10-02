const fs=require('fs'),assert=require('assert/strict'),{JSDOM}=require('jsdom'),{createCanvas,Path2D}=require('@napi-rs/canvas');
const dom=new JSDOM('',{runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window;
w.matchMedia=()=>({matches:false});w.requestAnimationFrame=()=>1;w.setInterval=()=>1;w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));
const ctx=createCanvas(120,120).getContext('2d'),at=Date.UTC(2026,8,29,2,15),models=[...Object.entries(w.Seal.FAMILY).map(([family,f])=>({family,action:f.name})),{family:'fulfilment',action:'ORDER COMPLETE',icon:'complete'},{family:'prepared',action:'QR LABEL PRINTED',icon:'qr'},{family:'prepared',action:'SENT TO SHEET',icon:'prepared'},{family:'engraving',action:'CUT PLAIN',icon:'plain'},{family:'laser',action:'LASER READY',icon:'laserReady'},{family:'engraving',action:'BACK ENGRAVING',icon:'engraving'}];
const failures=[];let count=0;
for(const model of models){
 for(const hour of [2,10,23]){
 const holder=w.document.createElement('div');holder.innerHTML=w.Seal.face({...model,at:at+hour*3600000,by:'Original Person'});
 const svg=holder.firstChild,path=new Path2D(svg.querySelector('[data-seal-outline]').getAttribute('d'));
 for(const t of svg.querySelectorAll('[data-seal-text]')){
  const size=+t.getAttribute('font-size'),y=+t.getAttribute('y'),width=+t.getAttribute('textLength');ctx.font=`700 ${size}px Arial`;const m=ctx.measureText(t.textContent);
  const top=y-m.actualBoundingBoxAscent,bottom=y+m.actualBoundingBoxDescent,left=60-width/2,right=60+width/2;
  for(const xx of [left,left+1,right-1,right])for(const yy of [top,top+1,bottom-1,bottom])if(!ctx.isPointInPath(path,xx,yy))failures.push({family:model.family,action:model.action,text:t.textContent,point:[xx,yy],box:[left,top,right,bottom]});
  count++;
 }
 }
}
w.close();console.log(JSON.stringify({textChecks:count,failures},null,2));assert.equal(failures.length,0,'all label/date/time bounds stay inside the real seal path');
