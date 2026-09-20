const assert=require('node:assert/strict'),B=require('../../charm-nest-backs.js');
const png='data:image/png;base64,iVBORw0KGgo=',id={poolId:'order_line_1',sheetId:'sheet-test',approvedAt:123};
(async()=>{
 let calls=0;const resolve=async()=>{calls++;await new Promise(r=>setTimeout(r,5));return {dataUrl:png,approvedAt:123};};
 const results=await Promise.all([B.recoverPreview(id,resolve),B.recoverPreview(id,resolve)]);
 assert.deepEqual(results,[png,png]);assert.equal(calls,1,'same image shown on multiple cards recovers once');
 assert.equal(await B.recoverPreview(id,resolve),png);assert.equal(calls,1,'re-rendering uses the recovered image');
 const html=B.markup([{...id,text:'Caroline',outputs:{png:{url:'https://fixture/expired'}}}]);
 assert(html.includes(png));assert(!html.includes('src="https://fixture/expired'));assert(html.includes('alt=""'),'broken alt text cannot overflow into the shelf');
 let retry=0;const other={...id,poolId:'order_line_2'};
 await assert.rejects(B.recoverPreview(other,async()=>{retry++;throw Error('offline');}),/offline/);
 assert.equal(await B.recoverPreview(other,async()=>{retry++;return {dataUrl:png,approvedAt:123};}),png);assert.equal(retry,2,'temporary failure stays retryable');
 await assert.rejects(B.recoverPreview({...id,approvedAt:124},resolve),/changed/);
 await assert.rejects(B.recoverPreview({...id,poolId:'bad'},async()=>({dataUrl:'https://arbitrary',approvedAt:123})),/unavailable/);
 assert.equal(calls,2,'new approval never reuses an older cached image');
 console.log('Back preview OK: concurrent recovery, rerender cache, approval identity, transient retry and safe fallback');
})().catch(e=>{console.error(e);process.exitCode=1;});
