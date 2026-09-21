const assert=require('node:assert/strict'),fs=require('fs'),vm=require('vm');global.self=global;require('../../charm-nest-pdf.js');const P=CharmNestPDF;
const members=JSON.parse(fs.readFileSync(__dirname+'/fixtures/middle_5903-paths.json'));
const source=fs.readFileSync('charm-nest-bridge.js','utf8');let parses=0,traces=0;
const B={pool:{sources:new Map()}},S={settings:{},poolSources:{}};
const context={B,S,performance,Map,Set,agent(){},sizeEntry:e=>e,Master:{keepOutOf:()=>[]},CharmNestAssets:{bytes:async()=>new Uint8Array([1])},P:{...P,
 parseSource:async()=>{parses++;return {segments:structuredClone(members),nested:[],pageW:80,pageH:80};},
 thumbnail:async c=>{assert.equal(c.outline.layer,'CUT');assert.equal(P.cutLinesOf(c).length,1);assert.equal(c.ringGeometryVersion,3);return 'assembled-black-preview';},
 buildSilhouettes:async(_,cs)=>{traces++;for(const c of cs)c.thumb='current-sheet-preview';}
}};
vm.createContext(context);vm.runInContext(source.slice(source.indexOf('  const masterLoads='),source.indexOf('  /** Upgrade cached geometry')),context);
(async()=>{
 const entry={sku:'HAND',aiPath:'hand.ai',aiUrl:'https://fixture/hand.ai'};
 const [a,b]=await Promise.all([context.masterPreview(entry),context.masterPreview(entry)]);
 assert.equal(a,'assembled-black-preview');assert.equal(a,b);assert.equal(parses,1,'parallel cards share the same parse');assert.equal(traces,0,'catalog preview does not build nesting masks');assert.equal(B.pool.sources.size,0);assert.equal(Object.keys(S.poolSources).length,0,'browsing does not enlarge saved workspace');
 for(let i=0;i<85;i++)await context.masterPreview({...entry,aiPath:'preview'+i});
 assert.equal(vm.runInContext('masterPreviewCache.size',context),80,'thumbnail cache bounded');
 const before=parses;const [x,y]=await Promise.all([context.masterCharm(entry),context.masterCharm(entry)]);assert.equal(x,y);assert.equal(parses,before+1);assert.equal(traces,1);
 assert.equal(await context.masterPreview(entry),'current-sheet-preview','loaded sheet geometry takes priority over old catalog thumbnail');
 console.log('Material previews OK: assembled hoops, one parse per concurrent load, bounded lightweight catalog cache, shared sheet geometry');
})().catch(e=>{console.error(e);process.exitCode=1;});
