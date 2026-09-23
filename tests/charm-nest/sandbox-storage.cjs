const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('design-1.html','utf8'),start=source.indexOf('<script type="module">',source.indexOf('<!-- Firebase Storage:'));
const moduleCode=source.slice(start+'<script type="module">'.length,source.indexOf('</script>',start)).replace(/import[\s\S]*?;/g,'');
function setup(sandbox,existing=false){
 const calls=[],warnings=[],app={options:{projectId:'test'}},window={},config={projectId:'test'};
 const ctx={SANDBOX:sandbox,window,firebaseConfig:config,console:{warn:(...xs)=>warnings.push(xs)},getApp(){calls.push('getApp');if(!existing)throw Error('no default app');return app;},initializeApp(c){assert.equal(c,config);calls.push('initializeApp');return app;},getStorage(a){assert.equal(a,app);calls.push('storage');return{};},getAuth(a){assert.equal(a,app);return{};},signInAnonymously(){calls.push('auth');return Promise.resolve();},ref:(_s,path)=>path,uploadBytesResumable:()=>({on:(_state,progress,_error,done)=>{progress({bytesTransferred:1,totalBytes:1});done();}}),getDownloadURL:async()=> 'https://fixture.invalid/upload.jpg'};
 vm.runInNewContext(moduleCode,ctx);return {ctx,calls,warnings};
}
(async()=>{
 const sandbox=setup(true);assert.equal(sandbox.calls.length,0,'sandbox never initializes or authenticates Storage');assert.equal(sandbox.warnings.length,0);assert.equal(sandbox.ctx.window.uploadViaResumable,undefined);
 for(const existing of [true,false]){const live=setup(false,existing);assert.equal(live.warnings.length,0);assert.equal(live.calls.filter(c=>c==='initializeApp').length,existing?0:1);let progress=0;assert.equal(await live.ctx.window.uploadViaResumable('chatImages/test',{type:'image/png'},p=>{progress=p;}),'https://fixture.invalid/upload.jpg');assert.equal(progress,100);}
 const stage=source.slice(source.indexOf('  function stage(list)'),source.indexOf('  function paintTray()',source.indexOf('  function stage(list)')));
 let notices=0;const c={SANDBOX:true,toast:()=>notices++};vm.runInNewContext(stage+';this.stage=stage;',c);c.stage([{type:'image/png'}]);assert.equal(notices,1,'pasted/dropped images do not enter a doomed sandbox upload queue');
 const html=fs.readFileSync('charm-nest-1.html','utf8'),form=html.slice(html.indexOf('id="signinForm"'),html.indexOf('</form>',html.indexOf('id="signinForm"')));
 assert(/autocomplete="username"/.test(form));assert(/autocomplete="current-password"/.test(form));
 for(const text of [source,html])for(const m of text.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))if(m[1].trim()&&!m[0].includes('type="module"'))new vm.Script(m[1]);
 console.log('Sandbox Storage OK: zero sandbox initialization/auth/upload, production fallback and resumable progress preserved, guarded attachments, accessible passcode form, scripts parse');
})().catch(e=>{console.error(e);process.exitCode=1;});
