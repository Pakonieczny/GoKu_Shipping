const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
(async()=>{
 const html=fs.readFileSync('charm-nest-1.html','utf8'),bridge=fs.readFileSync('charm-nest-bridge.js','utf8');
 for(const initial of ['on','off']){
  let payload;const c={S:{settings:{sandbox:initial}},window:{},FN:'/functions',fetch:async(_,init)=>{payload=JSON.parse(init.body);return{ok:true,status:200};},readBody:async()=>'{"ok":true}'};vm.createContext(c);
  vm.runInContext(html.match(/const WORKSPACE_SANDBOX =[^\n]+/)[0],c);
  const a=html.indexOf('async function api('),b=html.indexOf('/* The rail',a);vm.runInContext(html.slice(a,b),c);
  const key=bridge.slice(bridge.indexOf('const Session =')).match(/const key =[^\n]+/)[0];vm.runInContext(key+'; globalThis.scope=key;',c);
  c.S.settings.sandbox=initial==='on'?'off':'on';await c.api('charmNestLibrary',{op:'runPut'});
  assert.equal(!!payload.sandbox,initial==='on','in-flight writes retain their initial namespace through mode switches');
  assert.equal(c.scope(),initial==='on'?'sandbox':'production','old workspace cannot overwrite the destination checkpoint during reload');
 }
 console.log('Workspace separation OK: mode switches cannot move pending writes or checkpoints between sandbox and production');
})().catch(e=>{console.error(e);process.exitCode=1;});
