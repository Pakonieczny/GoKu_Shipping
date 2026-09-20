const assert=require('node:assert/strict'),E=require('../../charm-nest-export.js');
function inspect(text) {
  const lines=text.trimEnd().split(/\r?\n/);assert.equal(lines.length%2,0);
  const records=[];for(let i=0;i<lines.length;i+=2){const tag=[Number(lines[i]),lines[i+1]];assert(Number.isInteger(tag[0]));if(tag[0]===0)records.push([]);records.at(-1).push(tag);}
  const val=(r,c)=>r.find(t=>t[0]===c)?.[1],type=r=>val(r,0);
  const handles=new Map();for(const r of records){if(type(r)==='SECTION')continue;const h=val(r,5)||val(r,105);if(h){assert(!handles.has(h),'unique handles');handles.set(h,r);}}
  for(const r of records)for(const [code,h] of r)if([330,340,347,350,360,390].includes(code)&&h!=='0')assert(handles.has(h),`unresolved ${code} reference ${h}`);
  const sections=records.filter(r=>type(r)==='SECTION').map(r=>val(r,2));for(const n of ['HEADER','TABLES','BLOCKS','ENTITIES','OBJECTS'])assert(sections.includes(n));
  for(const name of ['*Model_Space','*Paper_Space']){
    const block=records.find(r=>type(r)==='BLOCK_RECORD'&&val(r,2)===name);assert(block,`${name} block record`);
    const h=val(block,5),layout=handles.get(val(block,340));assert.equal(type(layout),'LAYOUT');assert(layout.some(([c,v])=>c===330&&v===h));
    assert(records.some(r=>type(r)==='BLOCK'&&val(r,330)===h));assert(records.some(r=>type(r)==='ENDBLK'&&val(r,330)===h));
  }
  const model=records.find(r=>type(r)==='BLOCK_RECORD'&&val(r,2)==='*Model_Space');
  for(const e of records.filter(r=>type(r)==='LWPOLYLINE')){assert.equal(val(e,330),val(model,5));assert.equal(+val(e,90),e.filter(t=>t[0]===10).length);}
  const header=records.find(r=>type(r)==='SECTION'&&val(r,2)==='HEADER'),seed=header[header.findIndex(t=>t[1]==='$HANDSEED')+1][1];assert(parseInt(seed,16)>Math.max(...[...handles.keys()].map(h=>parseInt(h,16))));
  assert(!text.includes('{{'),'all scaffold fields resolved');return records;
}
const path=(layer,rgb)=>({layer,stroke:true,strokeRGB:rgb,subpaths:[[['m',[0,0]],['l',[72,0]],['l',[72,36]],['h']]]});
const result=E.dxf([path('cut',[1,0,0]),path('CUT',[0,0,1]),path('Grüße',[.2,.4,.6])]);inspect(result.text);
assert.equal(new Set(result.layers.map(([,v])=>v.toLowerCase())).size,3,'case-insensitive CAD layer names never merge');
assert(result.text.includes('420\r\n3368601\r\n'),'true RGB retained');assert(result.text.includes('10\r\n25.4\r\n'),'mm coordinates retained');
inspect(E.dxf([]).text);
assert.throws(()=>E.dxf([{...path('bad',[1,0,0]),subpaths:[[['m',[0,0]],['l',[Infinity,1]]]]}]),/non-finite/);
console.log('DXF structure OK: complete layouts, owned entities, resolved unique handles, mm/RGB and empty drawing');
