'use strict';
// Exercise the deployment entry classifier and its actual staging script in
// disposable fixtures; never run application handlers or a production build.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'../..'),script=fs.readFileSync(path.join(root,'scripts/build-netlify.cjs'),'utf8');
const manifest=()=>JSON.parse(fs.readFileSync(path.join(root,'scripts/netlify-function-entries.json'),'utf8'));
test('every real top-level JavaScript function source is classified exactly once before deployment',()=>{
  const m=manifest(),all=[...m.endpoints,...m.modules],sources=fs.readdirSync(path.join(root,'netlify/functions')).filter(name=>name.endsWith('.js')).sort();
  assert.equal(new Set(all).size,all.length,'duplicate endpoint/module classification');
  assert.deepEqual([...all].sort(),sources,'new helper or endpoint requires explicit manifest classification');
  for(const name of all){assert.match(name,/^[\w-]+\.js$/);assert.equal(fs.lstatSync(path.join(root,'netlify/functions',name)).isFile(),true);}
});
test('private helper modules are import dependencies rather than standalone Netlify endpoints',()=>{
  const m=manifest();assert.deepEqual(m.endpoints.filter(name=>name.startsWith('_')),[]);
  for(const name of ['_britesGrowthConversionTagEvidence.js','_britesGrowthTransactionContinuity.js','_britesMilestoneDiscovery.js']){
    assert.equal(m.modules.includes(name),true,name+' must remain an intentional helper');
    assert.equal(m.endpoints.includes(name),false,name+' must not become a public endpoint');
    const code=fs.readFileSync(path.join(root,'netlify/functions',name),'utf8');
    assert.doesNotMatch(code,/^export default\b|^exports\.handler\s*=|^module\.exports\s*=\s*(?:async\s*)?function/m);
  }
});
function fixture(t,{entries=['modern.js','legacy.js'],modules=['_helper.js'],extra={}}={}){
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'brites-manifest28-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
  const write=(p,text)=>{fs.mkdirSync(path.dirname(path.join(dir,p)),{recursive:true});fs.writeFileSync(path.join(dir,p),text);};
  write('scripts/build-netlify.cjs',script);write('scripts/netlify-function-entries.json',JSON.stringify({endpoints:entries,modules}));
  write('scripts/build-public.cjs',"require('node:fs').writeFileSync(require('node:path').join(__dirname,'public-build-called'),'fixture');\n");
  write('netlify/functions/modern.js',"import helper from './_helper.js';\nexport default async()=>helper;\nexport const config={path:'/api/fixture-modern'};\n");
  write('netlify/functions/legacy.js',"const helper=require('./_helper.js');\nexports.handler=async()=>helper;\nexports.config={schedule:'0 * * * *'};\n");
  write('netlify/functions/_helper.js',"throw Error('Build must never execute helpers or application handlers');\nmodule.exports={fixture:true};\n");
  write('netlify/functions/fonts/font.txt','fixture-font');write('netlify/functions/prompts/prompt.txt','fixture-prompt');
  write('.netlify/state.json','keep-site-linkage');write('.netlify/functions/stale.js','stale-bundle');write('netlify/production-functions/stale.js','stale-entry');
  for(const [p,text]of Object.entries(extra))write(p,text);
  return {dir,read:p=>fs.readFileSync(path.join(dir,p),'utf8'),exists:p=>fs.existsSync(path.join(dir,p)),run:()=>spawnSync(process.execPath,['scripts/build-netlify.cjs'],{cwd:dir,encoding:'utf8'})};
}
test('actual staging preserves literal endpoint configs, excludes helpers and retains site linkage',t=>{
  const f=fixture(t),result=f.run();assert.equal(result.status,0,result.stderr);
  assert.match(f.read('netlify/production-functions/modern.js'),/import handler from '\.\.\/functions\/modern\.js';/);assert.match(f.read('netlify/production-functions/modern.js'),/export const config=\{path:'\/api\/fixture-modern'\};/);
  assert.match(f.read('netlify/production-functions/legacy.js'),/module\.exports = require\('\.\.\/functions\/legacy\.js'\)/);assert.match(f.read('netlify/production-functions/legacy.js'),/exports\.config=\{schedule:'0 \* \* \* \*'\};/);
  assert.equal(f.exists('netlify/production-functions/_helper.js'),false);assert.equal(f.exists('netlify/production-functions/stale.js'),false);assert.equal(f.exists('.netlify/functions/stale.js'),false);
  assert.equal(f.read('.netlify/state.json'),'keep-site-linkage');assert.equal(f.exists('scripts/public-build-called'),true);
  assert.equal(f.read('netlify/production-functions/fonts/font.txt'),'fixture-font');assert.equal(f.read('netlify/production-functions/prompts/prompt.txt'),'fixture-prompt');
});
for(const [label,options,error]of[
  ['unclassified helper',{extra:{'netlify/functions/_new.js':'module.exports={};'}},/Classify the new source/],
  ['duplicate manifest entry',{modules:['_helper.js','modern.js']},/Duplicate function manifest entry/],
  ['missing helper source',{modules:['_helper.js','_missing.js']},/Invalid or missing function entrypoint/]
])test('deployment classification rejects '+label+' before deleting staged files or building public assets',t=>{
  const f=fixture(t,options),result=f.run();assert.notEqual(result.status,0);assert.match(result.stderr,error);
  assert.equal(f.read('netlify/production-functions/stale.js'),'stale-entry');assert.equal(f.read('.netlify/functions/stale.js'),'stale-bundle');assert.equal(f.read('.netlify/state.json'),'keep-site-linkage');assert.equal(f.exists('scripts/public-build-called'),false);
});
