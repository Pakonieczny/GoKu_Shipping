const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const G=require('../../charm-nest-geom.js'),Backs=require('../../charm-nest-backs.js'),Text=require('../../charm-nest-text.js');
const ot=require('../../vendor/opentype-1.3.4.min.js');
const base=ot.loadSync('vendor/fonts/SourceSans3-Regular.otf'),emoji=ot.loadSync('vendor/fonts/NotoEmoji-Regular.ttf'),data=require('../../vendor/fonts/emoji-sequences.json');
const font=Text.withEmoji(base,emoji,data,ot.Path);
for(const text of ['❤️ Hecht!','♥ Anna ☺','😊 😂 🥰','👨‍👩‍👧‍👦','👍🏽','🇨🇦 🇺🇸','1️⃣','❤️‍🔥','☀︎','é ñ Ω']){
 assert(G.glyphCoverage(font,text).ok,text); const p=font.getPath(text,0,0,20);assert(p.commands.length>4,text+' has vector ink');assert(font.getAdvanceWidth(text,20)>0);
}
assert(!G.glyphCoverage(font,'\u{10ffff}').ok,'unknown glyph cannot silently become a box');
assert.throws(()=>font.getPath('\u{10ffff}',0,0,10),/Unsupported/);
const a=font.getPath('A',0,0,20).commands,b=base.getPath('A',0,0,20).commands;assert.deepEqual(a,b,'ordinary text unchanged');
assert.equal(Text.graphemes('👨‍👩‍👧‍👦👍🏽❤️').length,3,'complete emoji sequences');
for(const seq of Object.values(data.sequences))for(const [id] of seq)assert(id>0&&id<emoji.numGlyphs,'valid shaped glyph index');
const rect=(x,y,w,h)=>[['m',[x,y]],['l',[x+w,y]],['l',[x+w,y+h]],['l',[x,y+h]],['h']];
const outline={kind:'path',closed:true,stroke:true,strokeRGB:[0,0,0],subpaths:[rect(0,0,40,60),rect(1,1,38,58)],bbox:[0,0,40,60]},hole={kind:'path',closed:true,stroke:true,strokeRGB:[0,0,0],subpaths:[rect(17,52,6,6)],bbox:[17,52,23,58]};
const charm={outline,members:[outline,hole],bbox:outline.bbox};
const ordinary=G.backView(charm,{upAngle:90});const solid=G.backView(charm,{solidBack:true,upAngle:90});
assert(G.area(solid.mask)>G.area(ordinary.mask)*3,'compound ink outline can use its solid back');
assert.equal(G.at(solid.mask,20,55),0,'physical hole preserved');assert.equal(outline.subpaths.length,2,'front unchanged');
const mask=G.engraveMask(solid,{marginMm:.3});
for(const text of ['I dissent','❤️ Hecht!']){const fit=G.fitText([text],font,mask,{minStrokeMm:0,minGapMm:0,tryRotated:false});assert(fit.ok,fit.reason);assert(G.verifyInk(fit.cmds,mask).ok,'every emoji/text path on solid metal');}
const records=[1,2,3].map(i=>({poolId:'100_200_'+i,order:'100',sku:'SAME',copy:i,text:'❤️',approvedAt:10}));
const sh={id:'one',charms:[{id:'a',poolId:records[0].poolId},{id:'b',poolId:records[1].poolId}],placements:[{id:'a'}]};
assert.deepEqual(Backs.forSheet(sh,records).map(b=>b.copy),[1],'overflow not assigned to current sheet');
const moved={id:'two',poolIds:[records[0].poolId,records[2].poolId]};
assert.deepEqual(Backs.forSheet(moved,records).map(b=>b.copy),[1,3]);assert.equal(Backs.forSheet(moved,records)[0].sheetId,'two');
assert.equal(Backs.forSheet(moved,[records[0],{...records[0],approvedAt:20,text:'new'}]).length,1);
assert.equal(Backs.forSheet(moved,[{...records[0],invalidated:true}]).length,0);
assert(!Backs.markup([{...records[0],png:'javascript:alert(1)',text:'<script>'}]).includes('<script>'));
// Production uses require-corp: a plain cross-origin img is blocked even when
// its saved PNG exists. Exercise compact history and full saved records alike.
const storagePng='https://firebasestorage.googleapis.com/v0/b/test/o/back%2Fcopy.png?alt=media&token=test';
for(const back of [{png:storagePng},{outputs:{png:{url:storagePng}}},{png:storagePng+'&c=1'}]) {
 const html=Backs.markup([{...records[0],...back}]);
 assert.match(html,/<img crossorigin="anonymous" referrerpolicy="no-referrer" src=/);
 assert.equal((html.match(/&amp;c=1/g)||[]).length,1,'one CORS cache key');
 assert(html.includes('back%2Fcopy.png?alt=media&amp;token=test'),'preserve object and download token');
}
assert(Backs.markup([{...records[0],preview:'data:image/png;base64,AAAA'}]).includes('src="data:image/png;base64,AAAA"'),'instant approval preview preserved');
assert(!Backs.markup([{...records[0],png:'https://['}]).includes('<img'),'malformed historical URL cannot break the sheet');
const compact=Backs.markup([{...records[0],png:storagePng,previewWPt:60,previewHPt:70}],{wPt:283.46,hPt:141.73});
assert(!compact.includes('<figcaption>'),'order numbers are no longer visible below backs');
assert(compact.includes('data-pool-id="100_200_1"'),'copy tracking remains');
assert(compact.includes('data-stock-w="283.46"')&&compact.includes('data-preview-w="60"'),'physical dimensions reach every preview');
assert.deepEqual(Backs.dimensions({previewWPt:60,previewHPt:70}),{w:60,h:70,pad:3*72/25.4});
const legacy=Backs.dimensions({pageWPt:60+4*72/25.4+.5,pageHPt:70+4*72/25.4+.5});
assert(Math.abs(legacy.w-60)<1e-8&&Math.abs(legacy.h-70)<1e-8,'existing back exports have a compatible scale fallback');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
const start=source.indexOf('  function libraryGroups('),end=source.indexOf('  let _cache',start);const context=vm.createContext({O:require("../../charm-nest-orders.js")});vm.runInContext(source.slice(start,end),context);
const groups=context.libraryGroups([{setId:'released',day:'2026-09-18',orders:{}}],[{id:'1',runId:'work',day:'2026-09-19',metal:'gold'},{id:'2',runId:'work',day:'2026-09-19',metal:'silver'},{id:'3',setId:'released',day:'2026-09-18',metal:'gold'}]);
assert.equal(groups.length,2);assert.equal(groups[0].sheets.length,2);assert.equal(groups[0].setId,null);assert.equal(groups[1].sheets.length,1);
console.log('Engraving updates OK: emoji vectors, solid back option, exact-copy ownership, historical working groups');
(async()=>{
 global.window=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');const P=CharmNestPDF;
 const fit=G.fitText(['❤️ Hecht!'],font,mask,{minStrokeMm:0,minGapMm:0,tryRotated:false});
 const glyphs=fit.glyphs.map(g=>({cmds:g.cmds.map(c=>{const o={...c};for(const suffix of ['','1','2'])if(o['x'+suffix]!=null){o['x'+suffix]-=solid.cx;o['y'+suffix]-=solid.cy;}return o;})}));
 const built=await P.buildBackFile({charm,parsed:{},cutMembers:solid.cutMembers,cx:solid.cx,cy:solid.cy,angleDeg:solid.angleDeg,glyphs,view:'asSeenFromBack',meta:{text:'❤️ Hecht!'}});
 assert(built.reference.redrawn,'simplified outline is actually exported');
 const parsed=await P.parseSource(built.bytes,'emoji-back.ai');
 const fills=parsed.segments.concat(parsed.nested).filter(s=>s.kind==='path'&&s.fill&&!s.stroke);assert(fills.length>0,'export contains engraving paths');
 const checkStart=source.indexOf('  async function verifyBackFile('),checkEnd=source.indexOf('  /** back/back-index',checkStart);
 const c=vm.createContext({P,G,S:{settings:{backFileView:'asSeenFromBack'}}});vm.runInContext(source.slice(checkStart,checkEnd),c);
 const verified=await c.verifyBackFile(built.bytes,{view:solid});assert(verified.ok,verified.why);
 console.log('Back export OK: emoji ink and simplified mirrored cut reference reparse and verify');
})().catch(e=>{console.error(e);process.exitCode=1;});

const O=require('../../charm-nest-orders.js');
for(const metal of ['gold10k','gold14k']) {
 const unselected={id:'solid',metal,runId:'work',day:'2026-09-19',draft:true};
 assert(O.libraryGroup(unselected).standalone);assert.notEqual(O.libraryGroup(unselected).key,O.libraryGroup({...unselected,metal:'gold'}).key);
 assert.equal(O.libraryGroup({...unselected,draft:false,setId:'released',solidIncluded:true}).key,'set:released');
 assert(O.libraryGroup({...unselected,draft:false,setId:'stale',solidIncluded:false}).standalone);
}
console.log('Solid gold grouping OK: manual nesting stays standalone for both 10K and 14K');

const square=[{type:'M',x:0,y:0},{type:'L',x:10,y:0},{type:'L',x:10,y:10},{type:'L',x:0,y:10},{type:'Z'}];
const sameWinding=G.rasterGlyphs(square.concat(square),{w:12,h:12,res:1,ox:-1,oy:-1});
assert.equal(G.at(sameWinding,5,5),1,'overlapping emoji components remain ink under PDF nonzero fill');

assert.equal(require("node:crypto").createHash("sha256").update(fs.readFileSync("vendor/fonts/NotoEmoji-Regular.ttf")).digest("hex"),data.fontSha256,"shape map matches exact font build");
