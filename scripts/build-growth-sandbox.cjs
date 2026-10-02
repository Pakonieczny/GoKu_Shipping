'use strict';
// An isolated deployment contains only growth endpoints: no shipping, Etsy
// sending, ad-budget mutation, or trading schedules are copied into it.
const fs=require('node:fs'),path=require('node:path');
const root=path.resolve(__dirname,'..'),out=path.resolve(process.argv[2]||path.join(root,'growth-sandbox-source'));
if(out===root||!out.startsWith(path.dirname(root)+path.sep))throw Error('Choose an isolated staging directory.');
fs.mkdirSync(out,{recursive:true});
const assets=['brites-growth.html','brites-growth.js','brites-growth.css','brites-growth-ad-integration.js','concierge-sandbox.html','concierge-sandbox.js','brites-concierge.js','brites-concierge.css','brites-concierge-avatar.js','brites-concierge-avatar.css','assets/brites-concierge-avatar-scene.mjs','assets/brites-concierge/avatar-concept.png','concierge-avatar-qa.html','concierge-avatar-checklist.html'];
for(const file of assets){const target=path.join(out,'public-site',file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.copyFileSync(path.join(root,file),target);}
fs.writeFileSync(path.join(out,'public-site','index.html'),'<meta http-equiv="refresh" content="0;url=/concierge-sandbox.html">');
const endpoints=['britesGrowthApi.js','britesConcierge.js','britesGrowthCatalogue-background.js','britesGrowthTick.js'];
const modules=['_britesGrowth.js','_britesGrowthDemandStore.js','_britesGrowthController.js','_britesGrowthReceiptReconciliation.js','_britesGrowthReceiptSandboxCheck.js','_britesGrowthEtsyCacheReadOnly.js','_britesGrowthHistoricalLookup.js','_britesConciergeDiagnostics.js','_britesConcierge.js','_googleAdsClaude.js','_editPasscode.js'];
fs.mkdirSync(path.join(out,'netlify/functions'),{recursive:true});fs.mkdirSync(path.join(out,'netlify/production-functions'),{recursive:true});
for(const file of [...endpoints,...modules])fs.copyFileSync(path.join(root,'netlify/functions',file),path.join(out,'netlify/functions',file));
for(const file of endpoints){const source=fs.readFileSync(path.join(root,'netlify/functions',file),'utf8'),config=source.match(/^export const config\s*=\s*(\{[\s\S]*?\});/m);fs.writeFileSync(path.join(out,'netlify/production-functions',file),`import handler from '../functions/${file}';\nexport default handler;\n`+(config?config[0]+'\n':''));}
fs.writeFileSync(path.join(out,'package.json'),JSON.stringify({name:'brites-growth-sandbox',version:'1.0.0',private:true,dependencies:{'@google-cloud/firestore':'^6.8.0','node-fetch':'^2.6.1'}},null,2)+'\n');
fs.writeFileSync(path.join(out,'netlify.toml'),'[build]\n publish="public-site"\n functions="netlify/production-functions"\n[functions]\n node_bundler="esbuild"\n external_node_modules=["@google-cloud/firestore"]\n[[headers]]\n for="/*"\n [headers.values]\n  X-Robots-Tag="noindex, nofollow, noarchive"\n  Cache-Control="max-age=0, no-cache, must-revalidate"\n[[headers]]\n for="/assets/brites-concierge-avatar-scene.mjs"\n [headers.values]\n  Access-Control-Allow-Origin="*"\n');
console.log('Prepared isolated growth sandbox source at '+out);
