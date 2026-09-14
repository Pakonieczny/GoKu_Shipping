const assert=require('node:assert/strict'),R=require('../../brites-ad-responsive'),{plan}=require('./ad-responsive.cjs');
const image={id:'photo',width:2048,height:2048,focus:{x:.3,y:.2,width:.4,height:.5}};
for(const title of ['Peach Charm','Peach Fruit Charm','Your Little Duck']){
 const p={...plan,copy:{...plan.copy,shortHeadline:title},style:{...plan.style,treatment:'soft-fade'}};
 for(const device of ['mobile','desktop']){
  const board=R.boards.find(b=>b.key==='display_300x1050'),d=R.document(p,image,board,device),h=d.objects.find(o=>o.editorRole==='headline'),b=d.objects.find(o=>o.editorRole==='button');assert(b.top-h.top-h.aiBoxHeight<=20.01);assert(b.top-h.top-h.aiBoxHeight>=10);assert(h.fontSize>=40);assert.equal(b.height,48);assert(b.top+b.height<1050);assert(h.left>=0&&h.left+h.width<=300);
  for(const key of ['display_728x90','display_970x90','display_980x120']){const board=R.boards.find(b=>b.key===key),d=R.document(p,image,board,device),h=d.objects.find(o=>o.editorRole==='headline'),edge=Math.min(board.width*.27,board.height*1.08);assert(h.left-edge>=14);assert(h.fontSize>=Math.min(board.height*.38,42));assert(h.left+h.width<=board.width);}
 }
}
console.log('PASS tall headline/action grouping and desktop banner insets without smaller type');

for(const key of ['display_120x600','display_160x600','display_300x600']){const board=R.boards.find(b=>b.key===key),d=R.document({...plan,style:{...plan.style,treatment:'soft-fade'}},image,board,'desktop'),photo=d.objects.find(o=>o.sourceKey==='photo'&&o.editorRole==='photo'),left=photo.left+(image.focus.x*image.width-(photo.cropX||0))*photo.scaleX,right=left+image.focus.width*image.width*photo.scaleX;assert(left>=board.width*.10,key+' left product margin');assert(right<=board.width*.90,key+' right product margin');}
