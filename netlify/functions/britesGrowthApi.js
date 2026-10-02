import core from './_britesGrowth.js';
import demandStore from './_britesGrowthDemandStore.js';
import controllerStore from './_britesGrowthController.js';
import receiptSandboxCheck from './_britesGrowthReceiptSandboxCheck.js';
import etsyCacheReadOnly from './_britesGrowthEtsyCacheReadOnly.js';
import historicalLookup from './_britesGrowthHistoricalLookup.js';
import conciergeDiagnostics from './_britesConciergeDiagnostics.js';

function environment(){const names=['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','SHOPIFY_STORE','SHOPIFY_CLIENT_ID','SHOPIFY_CLIENT_SECRET','BRITES_GROWTH_NAMESPACE','BRITES_GROWTH_ADMIN_KEY'];return Object.fromEntries(names.map(k=>[k,Netlify.env.get(k)]));}
function headers(req){const origin=req.headers.get('Origin');const allowed=origin&&(/https:\/\/(?:www\.)?britesjewelry\.com$/.test(origin)||origin===new URL(req.url).origin);return {'Content-Type':'application/json','Cache-Control':'no-store','Vary':'Origin',...(allowed?{'Access-Control-Allow-Origin':origin,'Access-Control-Allow-Headers':'Content-Type, X-Growth-Key, X-Edit-Passcode','Access-Control-Allow-Methods':'GET, POST, OPTIONS'}:{})};}
async function auth(req,env,db){const supplied=req.headers.get('X-Growth-Key')||req.headers.get('X-Edit-Passcode');if(!supplied)return false;if(core.sameSecret(supplied,env.BRITES_GROWTH_ADMIN_KEY))return true;const saved=await db.collection('config').doc('editPasscode').get();return saved.exists&&core.sameSecret(supplied,saved.data().passcode);}
export default async (req,context) => {
  const h=headers(req),json=(body,status=200)=>new Response(JSON.stringify(body),{status,headers:h});
  let publicRequest=false;
  if(req.method==='OPTIONS')return new Response(null,{status:204,headers:h});
  try{
    const env=environment(),db=core.makeDb(env),shopify=core.createShopify({env}),service=core.createGrowthService({db,env,shopify});
    const url=new URL(req.url),op=context.params?.op||url.searchParams.get('op')||'status';
    const publicOps=new Set(['catalogue','product','knowledge']);
    publicRequest=publicOps.has(op);
    if(!publicOps.has(op)&&!await auth(req,env,db))return json({error:'Operator sign-in required.'},401);
    if(publicOps.has(op)&&!await service.rateLimit(context.ip||'public-api',60))return json({error:'Please wait a moment before trying again.'},429);
    if(req.method==='GET'){
      if(op==='status')return json(await service.status());
      if(op==='etsy-cache-state')return json(await etsyCacheReadOnly.read({db}));
      if(op==='concierge-diagnostics'){
        if(service.namespace!=='Brites_Growth_Sandbox')return json({error:'The isolated sandbox namespace is required.'},403);
        return json(await conciergeDiagnostics.read({db,namespace:service.namespace,limit:20}));
      }
      if(op==='receipt-sandbox-check')return json({error:'Use POST for the isolated storage check.'},405);
      if(op==='historical-lookup')return json({error:'Use POST for the bounded historical lookup.'},405);
      if(op==='story-supplement')return json({error:'Use POST for story supplements.'},405);
      if(op==='milestone-index')return json({error:'Use POST to rebuild the private milestone index.'},405);
      if(op==='catalogue-export'){const cursor=url.searchParams.get('after')||'';if(cursor&&!/^[a-f0-9]{40}$/.test(cursor))return json({error:'Invalid catalogue cursor.'},400);let query=service.col('Products').orderBy('__name__').limit(200);if(cursor)query=query.startAfter(cursor);const snap=await query.get();return json({products:snap.docs.map(d=>d.data()),after:snap.size===200?snap.docs[snap.docs.length-1].id:null});}
      if(op==='catalogue'){const r=await shopify.search(url.searchParams.get('q')||'necklace');await service.saveProducts(r.products);const issues=await service.productIssues(r.products.map(p=>p.id));return json({products:core.applyProductIssues(r.products,issues).map(core.productProjection),pageInfo:r.pageInfo,live:true});}
      if(op==='product'){const p=await shopify.byHandle(url.searchParams.get('handle'));if(!p)return json({error:'This piece is not currently published.'},404);await service.saveProducts([p]);const issues=await service.productIssues([p.id]);return json({product:core.productProjection(core.applyProductIssues([p],issues)[0]),live:true});}
      if(op==='research'){const ids=(url.searchParams.get('ids')||'').split(',');const [dossiers,productIssues]=await Promise.all([service.research(ids),service.productIssues(ids)]);return json({dossiers,productIssues});}
      if(op==='issues')return json({products:await service.productIssues((url.searchParams.get('ids')||'').split(','))});
      if(op==='demand')return json({products:await demandStore.createDemandStore(service).read((url.searchParams.get('ids')||'').split(',').filter(Boolean))});
      if(op==='knowledge'){const ids=(url.searchParams.get('ids')||'').split(',');const [dossiers,issues,supplements]=await Promise.all([service.research(ids),service.productIssues(ids),typeof service.storySupplements==='function'?service.storySupplements(ids):[]]);const merged=core.mergeStorySupplements(dossiers,supplements,issues,Date.now());return json({products:core.publicMeanings(merged,ids,Date.now(),issues)});}
      return json({error:'Unknown operation.'},404);
    }
    if(req.method!=='POST')return json({error:'Method not allowed.'},405);
    const raw=await req.text();if(raw.length>900000)return json({error:'Request is too large.'},413);const body=JSON.parse(raw||'{}');
    if(!body||typeof body!=='object'||Array.isArray(body))return json({error:'Send a JSON object.'},400);
    if(op==='historical-lookup'){
      if(service.namespace!=='Brites_Growth_Sandbox')return json({error:'The isolated sandbox namespace is required.'},403);
      if(Date.now()>=core.STOP_AT)return json({stopped:true},410);
      return json(await historicalLookup.createHistoricalLookup({db,namespace:service.namespace,cursorSecret:env.BRITES_GROWTH_ADMIN_KEY}).read(body));
    }
    if(op==='import')return json(await service.importRanks(body.rows));
    if(op==='match'){
      if(!Number.isInteger(body.rank)||body.rank<1||body.rank>100)return json({error:'A top-ranked entry is required.'},400);
      const p=await service.getProduct(body.productId),m=body.match;
      if(!p||p.handle!==body.handle||!m||!['exact_handle','exact_sku','exact_title','manual'].includes(m.method)||!core.clean(m.evidence,1000)||!Number.isFinite(m.checkedAt)||m.checkedAt>Date.now()+60000||Date.now()-m.checkedAt>30*86400000)return json({error:'An inspected exact current product match is required.'},400);
      const research=(await service.research([p.id]))[0],ref=service.col('Queue').doc('rank-'+String(body.rank).padStart(3,'0'));
      await db.runTransaction(async tx=>{const s=await tx.get(ref);if(!s.exists)throw Error('Import this ranked entry first.');const row=s.data();if(row.leaseUntil>Date.now())throw Error('An active work lease owns this ranked entry.');if(m.method==='exact_handle'&&row.handle!==p.handle)throw Error('Exact handle evidence does not match the ranked entry.');if(m.method==='exact_title'&&core.textOf(row.title).toLowerCase()!==core.textOf(p.title).toLowerCase())throw Error('Exact title evidence does not match the ranked entry.');if(m.method==='exact_sku'&&!(p.variants||[]).some(v=>v.sku&&String(row.sku||'').split(/[\n,;|]+/).map(x=>x.trim()).includes(v.sku)))throw Error('Exact SKU evidence does not match a live product variant.');tx.update(ref,{productId:p.id,handle:p.handle,match:{method:m.method,evidence:core.clean(m.evidence,1000),checkedAt:m.checkedAt},status:research?.status==='approved'?'complete':'pending_research',dossierVersion:research?.status==='approved'?research.version:null,completedAt:research?.status==='approved'?Date.now():null,leaseToken:null,leaseOwner:null,leaseUntil:0,updatedAt:Date.now()});});
      return json({ok:true,status:research?.status==='approved'?'complete':'pending_research'});
    }
    if(op==='receipt-sandbox-check'){
      if(Object.keys(body).length)return json({error:'The storage check accepts no caller data.'},400);
      if(service.namespace!=='Brites_Growth_Sandbox')return json({error:'The isolated sandbox namespace is required.'},403);
      if(Date.now()>=core.STOP_AT)return json({stopped:true},410);
      return json(await receiptSandboxCheck.check({service,db,env:{...env,BRITES_GROWTH_NAMESPACE:service.namespace,BRITES_GROWTH_SANDBOX:'1'}}));
    }
    if(op==='story-supplement'){
      if(Object.keys(body).length!==1||!body.supplement)return json({error:'Provide only one story supplement.'},400);
      return json(await service.saveStorySupplement(body.supplement));
    }
    if(op==='milestone-index'){
      if(Object.keys(body).length)return json({error:'The milestone index rebuild accepts no caller data.'},400);
      if(service.namespace!=='Brites_Growth_Sandbox')return json({error:'The isolated sandbox namespace is required.'},403);
      if(Date.now()>=core.STOP_AT)return json({stopped:true},410);
      return json(await service.rebuildMilestoneIndex());
    }
    if(op==='save'){if(!body.dossier)return json({error:'A dossier is required.'},400);return json(await service.saveDossier(body.dossier));}
    if(op==='demand')return json(await demandStore.createDemandStore(service).save(body.evidence));
    if(op==='issue'){
      const p=await service.getProduct(body.productId),at=Date.now();
      if(!p||!Array.isArray(body.issues)||!body.issues.length||body.issues.some(issue=>issue.reviewed!==true||!Array.isArray(issue.evidence)||!issue.evidence.some(e=>core.publicUrl(e?.url)===core.publicUrl(p.url)&&core.clean(e?.quote||e?.text,1500)&&Number.isFinite(e.checkedAt)&&e.checkedAt<=at+60000&&at-e.checkedAt<=30*86400000)))return json({error:'Each reviewed issue needs fresh evidence from the exact current product page.'},400);
      return json(await service.recordProductIssue(body));
    }
    if(op==='claim')return json(await service.claim(body.kind,body.owner));
    if(op==='release')return json(await service.release(body.id,body.token,body.patch));
    if(op==='blocker')return json(await service.block(body));
    if(op==='sync')return json(await service.syncCatalogue());
    if(op==='control'){const allowed={};for(const k of ['enabled','aiEnabled'])if(typeof body[k]==='boolean')allowed[k]=body[k];if(body.aiDailyUsdCap!=null){const n=Number(body.aiDailyUsdCap);if(!Number.isFinite(n)||n<0||n>25)return json({error:'Daily runtime cap must be between 0 and 25 USD.'},400);allowed.aiDailyUsdCap=n;}await service.state().set({...allowed,updatedAt:Date.now()},{merge:true});return json({ok:true});}
    if(op==='controller'){
      const controller=controllerStore.createController(service);
      if(body.action==='read')return json(await controller.read());
      if(body.action==='claim')return json(await controller.claim(body.owner,body.leaseMinutes));
      if(body.action==='renew')return json(await controller.renew(body.owner,body.token,body.leaseMinutes));
      if(body.action==='release')return json(await controller.release(body.owner,body.token));
      return json({error:'Use a supported controller action.'},400);
    }
    if(op==='checkpoint')return json(await controllerStore.createController(service).save(body.value,{owner:body.owner,token:body.token,expectedUpdatedAt:body.expectedUpdatedAt}));
    if(op==='checkpoint-read'){const s=await service.col('State').doc('checkpoint').get();return json({checkpoint:s.exists?s.data():null});}
    if(op==='test'){await service.col('Tests').add({scenario:core.clean(body.scenario,150),result:core.clean(body.result,50),evidence:core.clean(body.evidence,6000),at:Date.now()});return json({ok:true});}
    return json({error:'Unknown operation.'},404);
  }catch(error){return json({error:publicRequest?'The live selection could not be checked right now. Please try again shortly.':core.clean(error.message,400)},/lease|invalid|required|does not match|not allowed|neutral|reviewed|stale|hold|supplement|dossier version|JSON|Unexpected/i.test(error.message)?400:503);}
};
export const config = { path: '/api/growth/:op' };
