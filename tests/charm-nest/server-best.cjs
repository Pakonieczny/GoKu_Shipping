// Exercise the server's decode, checkpoint and finalization boundary without network.
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const Solver = require('../../charm-nest-solver.js');
const source = fs.readFileSync(require.resolve('../../netlify/functions/charmNestSolve-background.js'), 'utf8');
(async () => {
  for (const failure of [false, true]) {
    const saved = {}, writes = [];
    const ref = { get: async () => ({ exists:true, data:()=>saved }), set:async x => { Object.assign(saved,x); writes.push(x); } };
    const firestore = Object.assign(()=>({ collection:()=>({doc:()=>ref}) }),{FieldValue:{serverTimestamp:()=>123}});
    const best = { placements:[{id:'a',xPt:1.123456789,yPt:1,wPt:2,hPt:2},{id:'b',xPt:4,yPt:1,wPt:2,hPt:2}],rejects:[],contactQuality:1.987654321,density:.4,trial:1,freePt2:600 };
    const fakeSolver = {...Solver, verify:()=>({ok:true}), solve:async(job,cb)=>{
      assert.equal(job.pieces[0].order,'old'); assert.equal(job.pieces[0].orderDate,100);
      assert.equal(job.pieces[1].order,'old'); assert.equal(job.pieces[0].bits[0],1);
      cb.onBest(best); best.placements[0].xPt=90;
      const worse={...best,placements:best.placements.slice(0,1),rejects:['b'],elapsedMs:50,endedBy:'budget',trials:4,params:{seed:7}};
      cb.onBest(worse);
      if(failure) throw new Error('worker interrupted');
      return worse;
    }};
    const ctx={exports:{},Buffer,console:{log(){},warn(){},error(){}},require:name=>name==='./firebaseAdmin'?{firestore}:name==='./_charmNestSolver'?fakeSolver:{parseBody:e=>e}};
    vm.runInNewContext(source,ctx);
    await ctx.exports.handler({id:'test',job:{sheet:{wPt:100,hPt:50},pieces:['a','b'].map(id=>({id,order:'old',orderDate:100,w:1,h:1,bits:'AQ=='}))}});
    assert.equal(saved.status,failure?'error':'done');
    assert.equal(saved.best.placements.length,2,'a worse or failed final result preserves the live checkpoint');
    assert.equal(saved.best.placements[0].xPt,1.123456789,'checkpoint coordinates stay exact and detached');
    assert.equal(saved.best.contactQuality,1.987654321,'comparison metrics survive serialization');
    if(!failure){assert.equal(saved.result.placements.length,2);assert.equal(saved.result.endedBy,'budget');assert.equal(saved.result.params.seed,7);}
  }
  console.log('Server best OK: whole-order metadata, exact detached checkpoints, expiry and failure retention');
})().catch(e=>{console.error(e);process.exit(1);});
