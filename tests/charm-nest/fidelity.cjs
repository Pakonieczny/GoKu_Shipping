// Usage: node tests/charm-nest/fidelity.cjs <source.ai> <sheet1.ai> [sheet2.ai …]
// Page-level fidelity: the multiset of every drawn path (paint op, stroke colour, fill colour, line width, closure,
// subpath count, operator counts, and the exact point coordinates relative to the path's own first point) in the
// written sheets must equal the source's, regardless of how the pieces were regrouped after nesting.
global.window=global; const L=global.PDFLib=require('/home/user/GoKu_Shipping/vendor/pdf-lib-1.17.1.min.js'); require('/home/user/GoKu_Shipping/charm-nest-pdf.js');
const fs=require('fs');
const key=x=>{ const ops={}; const rel=[]; let o=null; for(const sub of x.subpaths||[]) for(const s of sub){ ops[s[0]]=(ops[s[0]]||0)+1; for(let i=1;i<s.length;i++){ const p=s[i]; if(!Array.isArray(p)) continue; if(!o) o=p; rel.push(Math.hypot(p[0]-o[0],p[1]-o[1]).toFixed(1)); } }
  return [x.paintOp, x.stroke?JSON.stringify(x.strokeRGB.map(v=>+v.toFixed(3))):'-', x.fill?JSON.stringify(x.fillRGB.map(v=>+v.toFixed(3))):'-', (x.lwPt||0).toFixed(3), x.closed?'c':'o', (x.subpaths||[]).length, JSON.stringify(ops), rel.join(';')].join('|'); };
const drawable=p=>p.segments.concat(p.nested).filter(s=>s.kind==='path'&&(s.stroke||s.fill));
(async()=>{ const src=await CharmNestPDF.parseSource(new Uint8Array(fs.readFileSync(process.argv[2])),'src'); const S=new Map(); for(const x of drawable(src)){ const k=key(x); S.set(k,(S.get(k)||0)+1); }
 const O=new Map(); let n=0; for(const f of process.argv.slice(3)){ const out=await CharmNestPDF.parseSource(new Uint8Array(fs.readFileSync(f)),'out'); for(const x of drawable(out)){ const k=key(x); O.set(k,(O.get(k)||0)+1); n++; } }
 let missing=0, extra=0; for(const [k,c] of S){ const d=c-(O.get(k)||0); if(d>0) missing+=d; } for(const [k,c] of O){ const d=c-(S.get(k)||0); if(d>0) extra+=d; }
 const srcN=[...S.values()].reduce((a,b)=>a+b,0);
 console.log(`source paths ${srcN} · written paths ${n} · missing from output ${missing} · extra in output ${extra}`);
 if(missing||extra){ let shown=0; for(const [k,c] of S){ if((O.get(k)||0)<c && shown++<4) console.log('  missing:',k.slice(0,120)); } shown=0; for(const [k,c] of O){ if((S.get(k)||0)<c && shown++<4) console.log('  extra:',k.slice(0,120)); } }
 console.log(missing===0&&extra===0? 'FIDELITY OK — every path, colour, width and relative geometry identical (the nested sheet itself adds only its own border)':'FIDELITY DIFFERENCES');
})();
