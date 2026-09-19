// Solver unit test: synthetic shapes, real densities. `node tests/charm-nest/solver.cjs`
const S = require('../../charm-nest-solver.js');
const assert = require('assert');
function shape(id, kind, wPt, hPt, scale = 6) {
  const w = Math.round(wPt * scale), h = Math.round(hPt * scale), bits = new Uint8Array(w * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const nx = (x + .5) / w * 2 - 1, ny = (y + .5) / h * 2 - 1;
    let on = kind === 'rect' ? 1 : kind === 'ellipse' ? (nx * nx + ny * ny <= 1) : kind === 'ring' ? (nx * nx + ny * ny <= 1 && nx * nx + ny * ny >= .35) : (Math.abs(nx) + Math.abs(ny) <= 1);
    if (on) bits[y * w + x] = 1;
  }
  let a = 0; for (const b of bits) a += b;
  return { id, w, h, scale, bits, areaPt2: a / (scale * scale) };
}
(async () => {
  const kinds = ['rect', 'ellipse', 'ring', 'diamond'];
  const pieces = [];
  let r = S.rng(7);
  for (let i = 0; i < 21; i++) pieces.push(shape('c' + i, kinds[i % 4], 55 + r() * 95, 55 + r() * 95));
  const job = { sheet: { wPt: 513.03, hPt: 434.19, insetPt: 1.5 }, clearancePt: -0.5, angles: [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330], timeBudgetMs: 25000, seed: 3, pieces };
  const tot = pieces.reduce((s, p) => s + p.areaPt2, 0);
  console.log('total charm area', tot.toFixed(0), 'pt² of', (513 * 434).toFixed(0), '=', (tot / (513 * 434) * 100).toFixed(1) + '% density needed');
  let placedEvents = 0, trials = 0; const t = Date.now();
  const res = await S.solve(job, { onPlaced: () => placedEvents++, onTrial: s => { trials++; if (trials <= 3 || s.better) console.log(' trial', s.trial, s.placed + '/' + s.total, 'density', (s.density * 100).toFixed(1) + '%', 'better=' + s.better, (s.elapsedMs / 1000).toFixed(1) + 's'); } });
  console.log('result', res.placements.length + '/' + pieces.length, 'endedBy', res.endedBy, 'trials', res.trials, 'density', (res.density * 100).toFixed(1) + '%', 'pocket', res.pocket, 'in', ((Date.now() - t) / 1000).toFixed(1) + 's');
  const v = S.verify(job, res.placements, 6);
  console.log('verify', v);
  assert(v.ok, 'verifier found overlap/outside');
  assert(v.minGapPt >= job.clearancePt - 0.2, 'gap below clearance: ' + v.minGapPt);
  assert(res.placements.length >= 15, 'too few placed');
  // orders travel whole: a small sheet, three 2-piece orders and singles; whatever is rejected is whole orders only
  {
    const ps = [];
    for (let i = 0; i < 6; i++) ps.push(Object.assign(shape('o' + i, 'rect', 90, 90), { order: 'order' + (i >> 1) }));
    for (let i = 0; i < 6; i++) ps.push(Object.assign(shape('s' + i, 'ellipse', 70, 70), { order: 's' + i }));
    const j2 = { sheet: { wPt: 300, hPt: 200, insetPt: 1.5 }, clearancePt: 0, angles: [0, 90], timeBudgetMs: 6000, seed: 1, pieces: ps, maxFill: 0.74 };
    const r2 = await S.solve(j2, {});
    const placed = new Set(r2.placements.map(p => p.id));
    for (let k = 0; k < 3; k++) { const a = placed.has('o' + (2 * k)), b = placed.has('o' + (2 * k + 1)); assert(a === b, `order${k} split: ${a}/${b}`); }
    assert(r2.rejects.length > 0, 'the small sheet should reject something');
    console.log('orders whole', r2.placements.length + '/' + ps.length, 'rejects', r2.rejects.join(','), 'endedBy', r2.endedBy);
    assert(S.verify(j2, r2.placements, 6).ok, 'order test overlap');
  }
  // Arrival display order is irrelevant: oldest Etsy orders own earlier sheets, whole.
  {
    const ps=[Object.assign(shape('new','rect',8,8,1),{order:'new',orderDate:300}),
      Object.assign(shape('old-a','rect',8,8,1),{order:'old',orderDate:100}),
      Object.assign(shape('middle','rect',8,8,1),{order:'middle',orderDate:200}),
      Object.assign(shape('old-b','rect',8,8,1),{order:'old',orderDate:100})];
    const job={sheet:{wPt:20,hPt:12,insetPt:0},clearancePt:0,angles:[0,90],fineRes:1,coarseRes:1,timeBudgetMs:1200,maxFill:.74,pieces:ps};
    const result=await S.solve(job,{});assert.deepEqual(new Set(result.placements.map(p=>p.id)),new Set(['old-a','old-b']));
    assert(S.verify(job,result.placements,1).ok);
    const remainder=ps.filter(p=>result.rejects.includes(p.id));
    const second=await S.solve({...job,pieces:remainder},{});assert.equal(second.placements.length,2);
    // A pinned younger piece cannot jump ahead of an unplaceable older order.
    const oversized=Object.assign(shape('oversized','rect',30,30,1),{order:'old',orderDate:1});
    const pinned={...ps[0],pinned:{cxPt:4,cyPt:4,angle:0}};
    const blocked=await S.solve({...job,pieces:[pinned,oversized]},{});
    assert.equal(blocked.placements.length,0);assert.equal(blocked.rejects.length,2);
    const stopped=await S.solve({...job,pieces:ps},{shouldStop:()=>true});
    assert.equal(stopped.rejects.length,ps.length,'stopping in preparation cannot lose pieces');
    // Releasing arrival pins creates room before allocating another sheet.
    const a=Object.assign(shape('a','rect',12,8,1),{order:'a',orderDate:1,pinned:{cxPt:10,cyPt:10,angle:0}});
    const b=Object.assign(shape('b','rect',12,8,1),{order:'b',orderDate:2});
    const space={...job,sheet:{wPt:20,hPt:20,insetPt:0},maxFill:.9,pieces:[a,b]};
    const fixed=await S.solve(space,{}), packed=await S.solve({...space,pieces:[{...a,pinned:null},b]},{});
    assert.equal(fixed.placements.length,1);assert.equal(packed.placements.length,2);assert(S.verify(space,packed.placements,1).ok);
    console.log('FIFO and repack OK · oldest whole orders, overflow, pinned priorities, interrupted preparation, reclaimed space');
  }
  // Chronology controls membership, not the physical placement sequence: put the
  // newer U down first so the older square can occupy its open middle.
  {
    const old = {...shape('old-square','rect',4,4,1),order:'old',orderDate:1};
    const u = {...shape('new-u','rect',20,20,1),order:'new',orderDate:2};
    u.areaPt2 = 0;
    for(let y=0;y<20;y++)for(let x=0;x<20;x++){u.bits[y*20+x]=+(x<4 || x>=16 || y>=16);u.areaPt2+=u.bits[y*20+x];}
    const j={sheet:{wPt:20,hPt:20,insetPt:0},clearancePt:0,angles:[0,90,180,270],fineRes:1,coarseRes:1,maxFill:.74,timeBudgetMs:3000,maxTrials:1,pieces:[old,u]};
    const r=await S.solve(j,{});
    assert.equal(r.placements.length,2,'the old square must not lock a corner needed by the U');
    assert.equal(r.placements[0].id,'new-u','younger geometry may be placed first within the same chronological sheet');
    assert(S.verify(j,r.placements,1).ok);assert(Number.isFinite(r.density) && r.density<=.74);
    const impossible={...j,sheet:{wPt:18,hPt:20,insetPt:0},pieces:[old,u,{...shape('youngest','rect',3,3,1),order:'youngest',orderDate:3}]};
    const blocked=await S.solve(impossible,{});
    assert.deepEqual(blocked.placements.map(p=>p.id),['old-square'],'younger filler cannot bypass an older unplaceable order');
  }
  // Advice changes search heuristics only: unfamiliar rotations cannot bypass
  // the cap, chronology, pins, or verification, and must retain finite areas.
  {
    const pieces=[1,2,3].map(i=>({...shape('guide'+i,'rect',11,11,1),order:'order'+i,orderDate:i}));
    const job={sheet:{wPt:20,hPt:20,insetPt:0},clearancePt:0,angles:[0,90],fineRes:1,coarseRes:1,maxFill:.99,timeBudgetMs:2500,maxTrials:4,pieces,packingHints:{priority:['guide3','guide2','guide1'],angles:{guide1:[37],guide2:[-17],guide3:[731]}}};
    const result=await S.solve(job,{});
    assert(result.params.guidedTrials>0,'advice participates in a search');
    assert(Number.isFinite(result.density) && result.density<=.99,'rotated repair area stays finite and capped');
    const ids=new Set(result.placements.map(p=>p.id));if(ids.has('guide3'))assert(ids.has('guide1')&&ids.has('guide2'));if(ids.has('guide2'))assert(ids.has('guide1'));
    assert(S.verify(job,result.placements,1).ok);
  }
  console.log('OK');
})().catch(e => { console.error(e); process.exit(1); });
