const fs=require('node:fs'),assert=require('node:assert/strict');
const build=fs.readFileSync('scripts/build-public.cjs','utf8');
const assets=new Set(JSON.parse(build.match(/const assets = (\[[\s\S]*?\]);/)[1]));
const worker=fs.readFileSync('charm-nest-worker.js','utf8');
for(const call of worker.matchAll(/importScripts\(([^)]*)\)/g))for(const item of call[1].matchAll(/"([^"]+)"/g)){
 const file=item[1].split('?')[0];assert(assets.has(file),'Worker dependency missing from deployment: '+file);assert(fs.existsSync(file));
}
assert(worker.includes('if (m.job.learned && !self.CharmNestLearned) importScripts("charm-nest-learned.js")'),'learned dependency must not block standard worker startup');
console.log('Public assets OK: every worker import ships; learned module loads only for learned jobs');

for(const file of ['charm-nest-rose.js','charm-nest-rose-ui.js','charm-nest-assets.js','charm-nest-backs.js','charm-nest-text.js','charm-nest-export.js','charm-nest-export-ui.js','charm-nest-vector.js','vendor/clipper-6.4.2.js','vendor/clipper-6.4.2-LICENSE.txt'])assert(assets.has(file));
for(const file of ['NotoEmoji-Regular.ttf','emoji-sequences.json','NotoEmoji-OFL.txt'])assert(fs.existsSync('vendor/fonts/'+file));

for(const file of ["vendor/fonts/emoji-sequences.json","vendor/fonts/NotoEmoji-OFL.txt"])assert(assets.has(file));
