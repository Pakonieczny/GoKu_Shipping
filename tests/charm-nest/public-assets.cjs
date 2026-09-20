const fs=require('node:fs'),assert=require('node:assert/strict');
const build=fs.readFileSync('scripts/build-public.cjs','utf8');
const assets=new Set(JSON.parse(build.match(/const assets = (\[[\s\S]*?\]);/)[1]));
const worker=fs.readFileSync('charm-nest-worker.js','utf8');
for(const call of worker.matchAll(/importScripts\(([^)]*)\)/g))for(const item of call[1].matchAll(/"([^"]+)"/g)){
 assert(assets.has(item[1]),'Worker dependency missing from deployment: '+item[1]);assert(fs.existsSync(item[1]));
}
assert(worker.includes('if (m.job.learned && !self.CharmNestLearned) importScripts("charm-nest-learned.js")'),'learned dependency must not block standard worker startup');
console.log('Public assets OK: every worker import ships; learned module loads only for learned jobs');

for(const file of ['charm-nest-backs.js','charm-nest-text.js','charm-nest-export.js','charm-nest-export-ui.js'])assert(assets.has(file));
for(const file of ['NotoEmoji-Regular.ttf','emoji-sequences.json','NotoEmoji-OFL.txt'])assert(fs.existsSync('vendor/fonts/'+file));

for(const file of ["vendor/fonts/emoji-sequences.json","vendor/fonts/NotoEmoji-OFL.txt"])assert(assets.has(file));
