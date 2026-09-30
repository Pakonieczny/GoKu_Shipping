// Gemini Omni uses Interactions, not the Veo operations or OpenAI videos API.
const MODEL='gemini-omni-1.1-flash',SECONDS=10,OUTPUT_USD_PER_SECOND=5792*17.5/1e6;
const API='https://generativelanguage.googleapis.com/v1beta/';
// Gemini offers only 9:16 and 16:9. The square film is asked for at 16:9; its middle square is the 1:1 format.
const ASPECT={portrait:'9:16',square:'16:9',landscape:'16:9'};
function requestBody(image,prompt,orientation){
 if(!Buffer.isBuffer(image)||!image.length)throw Error('A product reference is required for animation.');
 if(!Object.hasOwn(ASPECT,orientation))throw Error('Choose a supported video orientation.');
 return {model:MODEL,background:true,store:true,input:[{type:'image',mime_type:'image/jpeg',data:image.toString('base64')},{type:'text',text:prompt}],response_format:{type:'video',aspect_ratio:ASPECT[orientation],resolution:'720p',delivery:'uri'}};
}
// Explicit subject references avoid treating the catalogue photograph as a
// literal first frame. All images describe the same product/assembly.
// https://ai.google.dev/gemini-api/docs/omni#tasks-parameter
function referenceRequestBody(references,prompt,orientation){
 if(!Array.isArray(references)||!references.length||references.length>3||references.some(r=>!Buffer.isBuffer(r.bytes)||!r.bytes.length||!['image/jpeg','image/png','image/webp'].includes(r.mimeType)))throw Error('Original product photographs are required for reference-guided animation.');
 if(!Object.hasOwn(ASPECT,orientation))throw Error('Choose a supported video orientation.');
 return {model:MODEL,background:true,store:true,input:[...references.map(r=>({type:'image',mime_type:r.mimeType,data:r.bytes.toString('base64')})),{type:'text',text:prompt}],generation_config:{video_config:{task:'reference_to_video'}},response_format:{type:'video',aspect_ratio:ASPECT[orientation],resolution:'720p',delivery:'uri'}};
}
// One intact original is the initial frame. Additional images would invite
// interpolation or another subject; the historic reference path stays intact.
function firstFrameRequestBody(reference,prompt,orientation){
 const body=referenceRequestBody([reference],prompt,orientation);
 body.generation_config.video_config.task='image_to_video';
 return body;
}
// Scenery-only films use text-to-video. Sending an empty image would pin a
// blank first frame; sending product pixels would invite a generated redraw.
// https://ai.google.dev/gemini-api/docs/omni#text-to-video-generation
function sceneryRequestBody(prompt,orientation){
 if(typeof prompt!=='string'||!prompt.trim())throw Error('A moving scenery direction is required.');
 if(!Object.hasOwn(ASPECT,orientation))throw Error('Choose a supported video orientation.');
 return {model:MODEL,background:true,store:true,input:[{type:'text',text:prompt}],response_format:{type:'video',task:'text_to_video',aspect_ratio:ASPECT[orientation],resolution:'720p',delivery:'uri'}};
}
function outputVideo(data){return (data.steps||[]).filter(s=>s.type==='model_output').flatMap(s=>s.content||[]).find(c=>c.type==='video'&&(c.uri||c.data))||null;}

// Reading Google's replies. An interaction answers with plain JSON, but Google also sends Server-Sent-Events text ("event: error" then
// "data: {...}"), gateway pages and empty bodies. Every body is read as text first and only then decoded, so a stray reply is classified
// (transient, or a real refusal) instead of crashing a poll that guards a film already paid for.
const looksLikeInteraction=o=>!!o&&typeof o==='object'&&!Array.isArray(o)&&(typeof o.id==='string'||typeof o.status==='string'||Array.isArray(o.steps));
// One decoded payload: a whole interaction, an event wrapping one ({event_type:'interaction.complete',interaction}), or an error.
function sortPayload(payload){
 if(!payload||typeof payload!=='object'||Array.isArray(payload))return {};
 if(looksLikeInteraction(payload.interaction))return {interaction:payload.interaction};
 if(looksLikeInteraction(payload))return {interaction:payload};
 if(payload.error!=null)return {error:payload.error};
 if(payload.event_type==='error'||payload.type==='error')return {error:payload};
 return {};
}
// SSE text -> frames. Blank lines end a frame; a new "event:" line after data also starts one, so a stream missing its blank lines still parses.
function sseFrames(text){
 const out=[];let event=null,data=[];
 const flush=()=>{if(event!==null||data.length){const raw=data.join('\n');let json;try{json=raw===''?undefined:JSON.parse(raw);}catch{}out.push({event:event||'message',raw,json});}event=null;data=[];};
 for(const line of String(text).split(/\r\n|\r|\n/)){
  if(line===''){flush();continue;}
  if(line.startsWith(':'))continue;
  const at=line.indexOf(':'),field=at<0?line:line.slice(0,at),value=at<0?'':line.slice(at+1).replace(/^ /,'');
  if(field==='event'){if(data.length)flush();event=value;}else if(field==='data')data.push(value);
 }
 flush();
 return out;
}
// -> {interaction, error, sse}. `error` is the error payload of the last error event (or JSON error body) that is not followed by a newer
// interaction; `interaction` is the newest whole interaction (an interaction.complete event wins over earlier progress events).
function readReply(text){
 const body=String(text==null?'':text).replace(/^\uFEFF/,'').trim();
 if(!body)return {sse:false,interaction:null,error:null};
 let json;try{json=JSON.parse(body);}catch{}
 if(json!==undefined){const sorted=sortPayload(json);return {sse:false,json,interaction:sorted.interaction||null,error:sorted.error==null?null:sorted.error};}
 let interaction=null,error=null,errorAt=-1,interactionAt=-1;
 const frames=sseFrames(body);
 if(!frames.length)return {sse:false,interaction:null,error:null};
 frames.forEach((f,i)=>{
  if(f.event==='error'){error=f.json!==undefined?(f.json&&typeof f.json==='object'&&f.json.error!=null&&!looksLikeInteraction(f.json)?f.json.error:f.json):(f.raw||true);errorAt=i;return;}
  const sorted=sortPayload(f.json);
  if(sorted.interaction){interaction=sorted.interaction;interactionAt=i;}else if(sorted.error!=null){error=sorted.error;errorAt=i;}
 });
 if(interaction&&(interaction.status==='completed'||interactionAt>errorAt))error=null;
 return {sse:true,interaction,error};
}
function errorFacts(e){
 if(typeof e==='string')return {message:e};
 if(!e||typeof e!=='object')return {};
 const inner=e.error&&typeof e.error==='object'?e.error:e;
 return {code:inner.code,status:typeof inner.status==='string'?inner.status:undefined,message:typeof inner.message==='string'?inner.message:typeof e.message==='string'?e.message:undefined};
}
const PERMANENT_STATUS=/^(?:invalid[_ ]?argument|failed[_ ]?precondition|permission[_ ]?denied|not[_ ]?found|unauthenticated|invalid[_ ]?request|bad[_ ]?request|forbidden|unimplemented)$/i;
const BLOCKED=/safety|blocked|prohibited|policy violation|content (?:filter|polic)/i,QUOTA=/current quota|billing|daily|per day|free tier|out of credit/i;
const BUSY=/^(?:unavailable|internal|deadline|resource[_ ]?exhausted|aborted|overloaded|server[_ ]?error)/i;
// Permanent only when Google gave a real reason: 400/401/403/404/422 (or the same named statuses) with a message, a safety block, or quota exhaustion.
// Everything else (5xx, 429 rate limits, error events with no clear meaning, unreadable bodies, resets) is transient.
function permanent(http,facts){
 const message=String(facts.message||'');
 if(BLOCKED.test(message))return true;
 const code=Number.isInteger(Number(facts.code))&&facts.code!==''&&facts.code!==null?Number(facts.code):null,status=String(facts.status||(typeof facts.code==='string'?facts.code:'')),number=code||http||0;
 if(number===429||/^resource[_ ]?exhausted$/i.test(status))return QUOTA.test(message);
 if(!message)return false;
 return [400,401,403,404,422].includes(number)||PERMANENT_STATUS.test(status);
}
const WHAT={unreadable:'Google’s video service sent a reply that could not be read',busy:'Google’s video service is busy or unavailable right now',network:'The connection to Google’s video service was interrupted'};
function plain(kind,context,definite){
 const what=WHAT[kind]||WHAT.unreadable;
 if(context==='poll')return what+'. Your film is still being made on Google’s side and nothing was bought again. Press Check saved progress in a minute.';
 if(context==='download')return what+' while fetching the finished film. It is saved by Google and nothing was bought again. Press Check saved progress in a minute.';
 if(definite)return what+' and did not accept the film request, so nothing was started or charged. Press Resume animation in a minute to ask again.';
 return what+' while the film was being requested. Google may already be making it, so nothing is requested again automatically. Check saved progress in a minute.';
}
function problem({kind,transient,definite,message,technical,retryAfter}){
 const e=Error(message);e.transient=!!transient;e.definiteResponse=!!definite;e.retryAfter=retryAfter||null;if(technical)e.technical=String(technical).slice(0,700);if(kind)e.kind=kind;return e;
}
const snippet=t=>String(t||'').replace(/\s+/g,' ').trim().slice(0,220);
// Bounded backoff for reads only: a poll or a download may be repeated freely, a create may never be.
const RETRY_DELAYS_MS=[2000,6000,15000],RETRY_BUDGET_MS=90000;
const wait=ms=>new Promise(r=>setTimeout(r,ms));

function createGeminiVideo({apiKey,fetch,sleep=wait}){
 // One attempt. Returns the interaction, or throws an error carrying transient / definiteResponse / technical.
 async function once(route,method,body){
  let r,text;
  try{
   r=await fetch(API+route,{method,timeout:method==='GET'?60000:120000,headers:{'x-goog-api-key':apiKey,'Api-Revision':'2026-05-20','Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
   text=await r.text();
  }catch(cause){
   throw problem({kind:'network',transient:true,definite:false,message:plain('network',method==='GET'?'poll':'start',false),technical:'Gemini video: no usable reply ('+String(cause?.message||cause).slice(0,300)+')'});
  }
  const retryAfter=Number(r.headers?.get?.('retry-after'))||null,reply=readReply(text),create=method!=='GET';
  const validId=reply.interaction&&/^v1_[a-zA-Z0-9_.:-]+$/.test(reply.interaction.id||'');
  if(r.ok){
   // A create whose stream already named the interaction is a created film, whatever followed: keep its id.
   if(reply.interaction&&(!reply.error||reply.interaction.status==='completed'||(create&&validId)))return reply.interaction;
   if(!reply.error&&reply.json&&!reply.sse&&!Array.isArray(reply.json)&&typeof reply.json==='object'&&reply.json.error==null)return reply.json;
  }
  const facts=errorFacts(reply.error!=null?reply.error:reply.json&&reply.json.error!=null?reply.json.error:null);
  const http=r.status,detail=facts.message||snippet(text);
  const label='Gemini video: HTTP '+http+(reply.sse?' · event stream':reply.json?'':' · unreadable reply')+(reply.error!=null?' · error event':'')+(detail?' · '+detail:'');
  if(permanent(http,facts)){
   const e=problem({kind:'refused',transient:false,definite:!r.ok?http>=400&&http<500:true,message:'Gemini video: '+String(facts.message||'HTTP '+http).slice(0,700),retryAfter});
   throw e;
  }
  const busy=http>=500||http===408||http===429||BUSY.test(String(facts.status||(typeof facts.code==='string'?facts.code:'')))||[429,500,502,503,504].includes(Number(facts.code)),kind=busy?'busy':'unreadable';
  // A refusal the service answered outright (HTTP 4xx) never reached the film queue; anything else leaves a create unconfirmed.
  // The one exception is Google's own structured capacity answer (503 with an UNAVAILABLE / "high demand" error body, not a gateway page, an empty
  // body or an event stream): the request was turned away at the front door, so no film was queued and asking again on Resume cannot buy twice.
  const atCapacity=create&&http===503&&!reply.sse&&reply.json!==undefined&&reply.error!=null&&(/^unavailable$/i.test(String(facts.status||''))||/high demand|overloaded|try again later/i.test(String(facts.message||'')));
  const definite=(!r.ok&&http>=400&&http<500)||atCapacity;
  throw problem({kind,transient:true,definite,message:plain(kind,create?'start':'poll',definite),technical:label,retryAfter});
 }
 async function request(route,method='GET',body){
  if(!apiKey){const e=Error('Video generation needs the existing GEMINI_API_KEY connection. Images can still be designed.');e.definiteResponse=true;throw e;}
  if(!/^interactions(?:\/[A-Za-z0-9_.:-]+)?$/.test(route))throw Error('Invalid Gemini interaction reference.');
  return retrying(method==='GET',()=>once(route,method,body));
 }
 // Only an idempotent read is repeated: bounded tries, growing pauses, Retry-After honoured. The same URL is asked again; nothing is created.
 async function retrying(repeatable,attempt){
  const started=Date.now();
  for(let n=0;;n++){
   try{return await attempt();}catch(e){
    if(!repeatable||!e.transient||n>=RETRY_DELAYS_MS.length)throw e;
    const pause=Math.min(30000,Math.max(RETRY_DELAYS_MS[n],(Number(e.retryAfter)||0)*1000));
    if(Date.now()-started+pause>RETRY_BUDGET_MS)throw e;
    await sleep(pause);
   }
  }
 }
 async function download(url,googleApi){
  let r,bytes;
  try{
   // Never forward the API key to a storage host or redirect.
   r=await fetch(url.href,{timeout:120000,size:100000000,redirect:'error',headers:googleApi?{'x-goog-api-key':apiKey}:{}});
   if(r.ok){
    const type=String(r.headers?.get?.('content-type')||'');
    // An error page or event stream in place of the video is read as text and classified, never saved as a film.
    if(/json|event-stream|^text\//i.test(type)){
     const reply=readReply(await r.text()),facts=errorFacts(reply.error!=null?reply.error:reply.json&&reply.json.error!=null?reply.json.error:null);
     if(permanent(r.status,facts))throw problem({kind:'refused',transient:false,definite:true,message:'The saved Gemini video could not be downloaded: '+String(facts.message).slice(0,500)});
     throw problem({kind:'unreadable',transient:true,message:plain('unreadable','download'),technical:'Gemini video download: '+type+' body instead of a video · '+(facts.message||'no video data')});
    }
    bytes=await r.buffer();
   }
  }catch(cause){
   if(cause&&(cause.transient!==undefined||cause.type==='no-redirect'))throw cause;
   throw problem({kind:'network',transient:true,message:plain('network','download'),technical:'Gemini video download: no usable reply ('+String(cause?.message||cause).slice(0,300)+')'});
  }
  if(!r.ok){
   if(r.status>=500||r.status===408||r.status===429)throw problem({kind:'busy',transient:true,message:plain('busy','download'),technical:'Gemini video download: HTTP '+r.status,retryAfter:Number(r.headers?.get?.('retry-after'))||null});
   throw Error('The saved Gemini video could not be downloaded (HTTP '+r.status+').');
  }
  if(!bytes||!bytes.length)throw problem({kind:'unreadable',transient:true,message:plain('unreadable','download'),technical:'Gemini video download: empty body'});
  return bytes;
 }
 async function content(video){
  if(video?.data)return Buffer.from(video.data,'base64');
  let url;try{url=new URL(video?.uri);}catch{throw Error('Gemini returned no downloadable video.');}
  const googleApi=url.hostname==='generativelanguage.googleapis.com',storage=url.hostname==='storage.googleapis.com'||url.hostname.endsWith('.googleusercontent.com');
  if(url.protocol!=='https:'||(!googleApi&&!storage))throw Error('Gemini returned an unsupported video download host.');
  return retrying(true,()=>download(url,googleApi));
 }
 return {request,content};
}
module.exports={MODEL,SECONDS,OUTPUT_USD_PER_SECOND,requestBody,referenceRequestBody,firstFrameRequestBody,sceneryRequestBody,outputVideo,createGeminiVideo,readReply,RETRY_DELAYS_MS};
