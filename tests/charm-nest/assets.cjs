const assert=require('node:assert/strict'),{Readable}=require('node:stream');
const {serve,CHUNK}=require('../../netlify/functions/_charmNestAsset.js');
const A=require('../../charm-nest-assets.js');
const source='https://firebasestorage.googleapis.com/v0/b/test/o/charmnest%2Fsheets%2Ftest.ai?alt=media&token=valid';
let generation='1',reads=0,metadata=0,failures=0;
let content=Buffer.alloc(CHUNK*2+173);for(let i=0;i<content.length;i++)content[i]=i%251;
const bucket={name:'test',file:(path,options)=>({
  getMetadata:async()=>{metadata++;return [{size:String(content.length),generation,contentType:'application/pdf',metadata:{firebaseStorageDownloadTokens:'valid,second'}}];},
  createReadStream:({start,end})=>{reads++;assert.equal(options.generation,generation);return Readable.from([content.subarray(start,end+1)]);}
})};
const request=(url=source,headers={})=>new Request('https://app.test/.netlify/functions/charmNestAsset?url='+encodeURIComponent(url),{headers});
(async()=>{
 assert(A.url(source).startsWith('/.netlify/functions/charmNestAsset?url='));
 assert.equal(A.url(A.url(source)),A.url(source),'routing is idempotent');
 assert.equal(A.url('data:image/png;base64,AA=='),'data:image/png;base64,AA==');
 for(const value of [source.replace('https:','http:'),source.replace('/b/test/','/b/foreign/'),source.replace('firebasestorage.googleapis.com','evil.test'),source.replace('charmnest%2F','private%2F'),source.replace('&token=valid','')]){
   assert.equal((await serve(request(value),bucket)).status,400);
 }
 assert.equal(metadata,0,'foreign or unscoped URLs cannot read storage metadata');
 assert.equal((await serve(request(source.replace('token=valid','token=wrong')),bucket)).status,403);
 assert.equal(reads,0,'invalid tokens cannot read any object bytes');
 global.fetch=async(target,options)=>{
   assert(target.startsWith('/.netlify/functions/charmNestAsset?'),'browser only reads same-origin URLs');
   if(failures++===0)throw TypeError('temporary connection interruption');
   return serve(new Request('https://app.test'+target,{headers:options.headers,signal:options.signal}),bucket);
 };
 const [one,two]=await Promise.all([A.bytes(source),A.bytes(source)]);
 assert.equal(one,two,'concurrent requests share the transfer');
 assert.deepEqual(Buffer.from(one),content,'all chunks are reassembled byte-for-byte');
 assert.equal(reads,3,'large artwork uses bounded chunks');
 assert.equal(failures,4,'only the interrupted request retries');
 const res=await serve(request(source,{Range:'bytes=0-99'}),bucket);
 assert.equal(res.status,206);assert.equal(res.headers.get('content-range'),`bytes 0-99/${content.length}`);
 assert.equal(res.headers.get('cross-origin-resource-policy'),'same-origin');
 assert.equal(res.headers.get('content-type'),'application/pdf');
 await res.arrayBuffer();
 assert.equal((await serve(request(source,{Range:'bytes=99999999-'}),bucket)).status,416);
 assert.equal((await serve(request(source,{'If-Match':'"old"'}),bucket)).status,412);
 let calls=0;global.fetch=async(target,options)=>{if(calls++)generation='2';return serve(new Request('https://app.test'+target,{headers:options.headers}),bucket);};
 await assert.rejects(A.bytes(source),/412/,'concurrent re-nesting cannot mix file revisions');
 global.fetch=async()=>new Response(new Uint8Array([1]),{status:206,headers:{'Content-Range':'bytes 0-9/10',ETag:'"2"'}});
 await assert.rejects(A.bytes(source),/incomplete/,'truncated ranges are rejected');
 global.fetch=async()=>new Response('no',{status:403});
 await assert.rejects(A.bytes(source),/403/,'permission failures are never bypassed');
 console.log('Assets OK: token scope, same-origin transport, retry, shared transfers, large files, exact bytes, and revision/truncation checks');
})().catch(e=>{console.error(e);process.exitCode=1});
