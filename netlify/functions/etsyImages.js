'use strict';
const cache=require('./_etsyImageCache');
exports.handler=async event=>{
  const q=event.queryStringParameters||{};
  const r=await cache.read(q.listingId,{cacheOnly:q.sandbox==='1'});
  return {statusCode:r.status,headers:{'Content-Type':'application/json','Cache-Control':r.status===200&&!r.etsyCalls?(r.images.length?'public, max-age=86400':'public, max-age=300'):'no-store','X-Etsy-Image-Source':r.source,'X-Etsy-Calls':String(r.etsyCalls||0),...(r.retryAt?{'Retry-After':String(Math.max(1,Math.ceil((r.retryAt-Date.now())/1000)))}:{})},body:JSON.stringify({results:r.images,source:r.source,retryAt:r.retryAt})};
};
