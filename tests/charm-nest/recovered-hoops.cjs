const assert=require('node:assert/strict'),fs=require('fs'),vm=require('vm');
global.self=global;require('../../charm-nest-pdf.js');const P=global.CharmNestPDF;
const code=fs.readFileSync('charm-nest-bridge.js','utf8');
const rect=(x,y,w,h)=>[['m',[x,y]],['l',[x+w,y]],['l',[x+w,y+h]],['l',[x,y+h]],['h']];
const circle=(r)=>{const k=.5522847498*r,c=20;return [['m',[c+r,c]],['c',[c+r,c+k],[c+k,c+r],[c,c+r]],['c',[c-k,c+r],[c-r,c+k],[c-r,c]],['c',[c-r,c-k],[c-k,c-r],[c,c-r]],['c',[c+k,c-r],[c+r,c-k],[c+r,c]],['h']];};
const outline={kind:'path',stroke:true,closed:true,strokeRGB:[0,0,0],lwPt:.2,subpaths:[rect(0,0,20,20)],bbox:[0,0,20,20],index:0};
const ring={...outline,strokeRGB:[0,1,1],subpaths:[circle(3),circle(1.65)],bbox:[17,17,23,23],index:1};
const c={sourceId:'s',poolId:'p',outline,members:[outline,ring],bbox:[0,0,23,23],pinned:{angle:90}};
let traces=0;const ctx={G:require('../../charm-nest-geom.js'),P:{...P,buildSilhouettes:async(_,charms)=>{traces++;charms[0].thumb='repaired';}},S:{settings:{silhouetteRes:6}}};vm.createContext(ctx);
vm.runInContext(code.slice(code.indexOf('  async function repairRecoveredGeometry('),code.indexOf('  function cloneCharm(')),ctx);
(async()=>{
 const good={id:'good',ringGeometryVersion:3,outline},pg={charms:[c],placements:[{id:'c'}],outputs:{ai:'old'},verification:{ok:true},backPool:[{poolId:'p'},{poolId:'good'}]},untouched={charms:[good],placements:[{id:'good'}],outputs:{ai:'keep'}};
 const d={sources:[{id:'s',charms:[c],parsed:{}}],sheets:[{pages:[pg,untouched]}],jobs:[{key:'j',copies:['p'],state:'written',fit:{},approvedBy:'Operator'},{key:'good',copies:['good'],state:'written',fit:{good:true}}],orders:{rows:[{key:'j',engrave:{approved:true}}]},review:[{jobKey:'j'}],run:{status:'stopped',step:'commit'}};
 assert.equal(await ctx.repairRecoveredGeometry(d),1);assert.equal(traces,1,'shared source/placed charm repaired once');assert.equal(c.ringGeometryVersion,3);assert.equal(c.thumb,'repaired');assert.equal(c.pinned,null);
 assert(pg.dirty);assert.equal(pg.outputs,null);assert.equal(pg.verification,null);assert.equal(pg.placements.length,0);assert.equal(pg.backPool.length,1);assert.equal(untouched.outputs.ai,'keep');
 assert.equal(d.jobs[0].state,'ready');assert.equal(d.jobs[0].fit,null);assert.equal(d.orders.rows[0].engrave.approved,false);assert.equal(d.jobs[1].state,'written');assert.equal(d.run.step,'nest');assert.equal(d.review.length,0);
 assert.equal(await ctx.repairRecoveredGeometry(d),0);assert.equal(traces,1,'new geometry is not retraced on every reload');
 assert(code.includes('await Pool.repairRecoveredGeometry(d);'),'recovery runs before the workspace is displayed');
 // A previous release welded the hand's HATCH artwork, preserving its ink
 // cavities as holes. Recovery must restart from the unmodified CUT vectors.
 const members=JSON.parse(fs.readFileSync(__dirname+'/fixtures/middle_5903-paths.json'));
 const parsed={segments:members.map((m,index)=>({...m,index,start:index,end:index+1})),nested:[],pageW:80,pageH:80};
 const badOutline=parsed.segments.find(m=>m.layer==='HATCH');
 const legacy={sourceId:'hand',poolId:'hand-copy',outline:badOutline,members:parsed.segments.slice(),bbox:[4.9,4.9,26.4,44.6],strokePt:.2,topIndices:[0,1,2]};
 P.integrateRings(legacy);for(const m of legacy.members)delete m.manufacturingRole;legacy.ringGeometryVersion=2;
 const page={charms:[legacy],placements:[{id:'hand'}],best:{placements:[{id:'hand'}]},bestKey:'old',backPool:[]};
 const recovered={sources:[{id:'hand',parsed,charms:[legacy]}],sheets:[{pages:[page]}],jobs:[{key:'draft',copies:['turtle'],state:'review',fit:{old:true}},{key:'approved',copies:['turtle'],state:'approved',fit:{keep:true}}]};
 assert.equal(await ctx.repairRecoveredGeometry(recovered),1);
 assert.equal(legacy.outline.layer,'CUT');assert.equal(P.cutLinesOf(legacy).length,1,'only the actual hoop aperture remains');
 assert.equal(page.best,null);assert.equal(page.bestKey,null,'old packing checkpoint cannot resurrect wrong geometry');
 assert.equal(recovered.jobs[0].fit,null);assert.equal(recovered.jobs[0].state,'ready');assert(recovered.jobs[1].fit.keep,'unchanged approved work is retained');
 console.log('Recovered hoops OK: cached geometry upgraded once, preview retraced, changed sheets re-nested and engraving rechecked; unaffected work retained');
})().catch(e=>{console.error(e);process.exitCode=1;});
