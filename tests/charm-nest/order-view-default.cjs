const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const root=path.join(__dirname,'../..'),html=fs.readFileSync(path.join(root,'charm-nest-1.html'),'utf8'),bridge=fs.readFileSync(path.join(root,'charm-nest-bridge.js'),'utf8');
const defaults=html.slice(html.indexOf('const DEFAULTS'),html.indexOf('function loadSettings()'));
assert.match(defaults,/orderView:\s*"list"/);
const settingsCode=html.slice(html.indexOf('function loadSettings()'),html.indexOf('function saveSettings()'));
for(const [saved,expected] of [[null,'list'],[{v:22,orderView:'cards'},'list'],[{v:23,orderView:'cards'},'cards'],[{v:23,orderView:'list'},'list']]){
 const c=vm.createContext({DEFAULTS:{stock:{},angleStep:10,orderView:'list',v:23},localStorage:{getItem:()=>JSON.stringify(saved)}});
 vm.runInContext(settingsCode,c);assert.equal(vm.runInContext('loadSettings().orderView',c),expected);
}
// Old workspace snapshots must not undo the List migration; a subsequent
// explicit Cards preference still survives a workspace save and restore.
const restore=bridge.split('\n').find(line=>line.includes('Object.assign(Orders.view(), d.orderView'));
assert(restore);
for(const [d,setting,expected] of [[{orderView:{view:'cards',q:'turtle'}},'list','list'],[{orderViewVersion:1,orderView:{view:'cards',q:'turtle'}},'list','cards'],[{orderViewVersion:1},'list','list']]){
 const ov={},state=()=>({});vm.runInNewContext(restore,{d,S:{settings:{orderView:setting}},Orders:{view:()=>ov},Gate:{state},Recall:{state}});
 assert.equal(ov.view,expected);if(d.orderView)assert.equal(ov.q,'turtle');
}
console.log('Order view OK: fresh List default, existing-workspace migration, filters retained and explicit Cards preference preserved');
