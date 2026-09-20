// Read-only access to the same objects already authorized by Firebase download tokens.
const crypto = require('node:crypto');
const { Readable } = require('node:stream');
const CHUNK = 2 * 1024 * 1024;
const fail = (status, message) => new Response(message, {status, headers:{'Cache-Control':'no-store'}});
const same = (a,b) => crypto.timingSafeEqual(crypto.createHash('sha256').update(a).digest(),crypto.createHash('sha256').update(b).digest());
function identify(raw, bucket) {
  const url = new URL(raw), m = url.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
  if(url.protocol !== 'https:' || url.hostname !== 'firebasestorage.googleapis.com' || url.port || url.username || url.password || !m || decodeURIComponent(m[1]) !== bucket) throw Error('Invalid storage URL');
  const path=decodeURIComponent(m[2]),token=url.searchParams.get('token');
  if(!path.startsWith('charmnest/') || path.split('/').some(s=>s==='..') || !token || token.length>500) throw Error('Invalid artwork token');
  return {path,token};
}
async function serve(req, bucket) {
  if(!['GET','HEAD'].includes(req.method))return fail(405,'Method not allowed');
  let identity;
  try{identity=identify(new URL(req.url).searchParams.get('url'),bucket.name);}catch{return fail(400,'Invalid artwork URL');}
  try {
    const [meta]=await bucket.file(identity.path).getMetadata();
    const tokens=String(meta.metadata?.firebaseStorageDownloadTokens || '').split(',').filter(Boolean);
    if(!tokens.some(t=>same(t,identity.token)))return fail(403,'Artwork link expired');
    const size=Number(meta.size),etag='"'+String(meta.generation)+'"';
    if(!Number.isSafeInteger(size)||size<=0)return fail(404,'Artwork is empty');
    if(req.headers.get('if-match') && req.headers.get('if-match')!==etag)return fail(412,'Artwork changed during download');
    const headers={'Content-Type':/^(image\/(png|jpeg|webp)|application\/(pdf|illustrator|postscript|json|zip))$/.test(meta.contentType)?meta.contentType:'application/octet-stream',
      'Cache-Control':'private, no-cache','Cross-Origin-Resource-Policy':'same-origin','X-Content-Type-Options':'nosniff','Accept-Ranges':'bytes','ETag':etag};
    let start=0,end=size-1,status=200;
    const range=req.headers.get('range');
    if(range){
      const m=/^bytes=(\d+)-(\d*)$/.exec(range);
      if(!m)return fail(416,'Invalid byte range');
      start=Number(m[1]);end=Math.min(size-1,m[2]?Number(m[2]):start+CHUNK-1,start+CHUNK-1);
      if(!Number.isSafeInteger(start)||start<0||start>=size||end<start)return fail(416,'Invalid byte range');
      status=206;headers['Content-Range']=`bytes ${start}-${end}/${size}`;
    } else if(size>20*1024*1024)return fail(413,'Use ranged download for large artwork');
    headers['Content-Length']=String(end-start+1);
    if(!range && req.headers.get('if-none-match')===etag)return new Response(null,{status:304,headers});
    if(req.method==='HEAD')return new Response(null,{status,headers});
    // Pin the generation: a re-nest during transfer can never splice two different files.
    const stream=bucket.file(identity.path,{generation:meta.generation}).createReadStream({start,end});
    const abort=()=>stream.destroy();req.signal?.addEventListener('abort',abort,{once:true});
    stream.once('close',()=>req.signal?.removeEventListener('abort',abort));
    return new Response(Readable.toWeb(stream),{status,headers});
  } catch(e) {return fail(Number(e.code)===404?404:503,Number(e.code)===404?'Artwork not found':'Artwork temporarily unavailable');}
}
module.exports={serve,identify,CHUNK};
