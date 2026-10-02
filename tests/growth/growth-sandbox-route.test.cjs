'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),cp=require('node:child_process');
const root=path.resolve(__dirname,'../..');

test('the default isolated growth build includes the protected Ads route',()=>{
  const out=fs.mkdtempSync(path.join(path.dirname(root),'growth-default-stage-test-'));
  try{
    const result=cp.spawnSync(process.execPath,[path.join(root,'scripts/build-growth-sandbox.cjs'),out],{encoding:'utf8',timeout:120000});
    assert.equal(result.status,0,result.stderr||result.stdout);
    const entries=fs.readdirSync(path.join(out,'netlify/production-functions')).sort();
    assert.ok(entries.includes('britesGrowthAds.js'));
    assert.match(fs.readFileSync(path.join(out,'netlify/production-functions/britesGrowthAds.js'),'utf8'),/path: '\/api\/growth-ads'/);
    const manifest=JSON.parse(fs.readFileSync(path.join(out,'sandbox-ad-manifest.json'),'utf8'));
    assert.equal(manifest.adsEndpoint,'/api/growth-ads');
    assert.equal(manifest.adsWrites,false);
    assert.equal(manifest.paidAiKeysRequired,false);
    const qaScript=path.join(out,'public-site/concierge-avatar-qa.js'),qaPage=fs.readFileSync(path.join(out,'public-site/concierge-avatar-qa.html'),'utf8');
    assert.ok(fs.existsSync(qaScript),'the browser-visible avatar diagnostics are copied into the isolated stage');
    assert.match(fs.readFileSync(qaScript,'utf8'),/BritesAvatarAcceptance/);
    assert.match(qaPage,/<script src="\/concierge-avatar-qa\.js"><\/script>/);
  }finally{fs.rmSync(out,{recursive:true,force:true});}
});
