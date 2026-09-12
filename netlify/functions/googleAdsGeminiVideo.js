// Gemini Omni uses Interactions, not the Veo operations or OpenAI videos API.
const MODEL='gemini-omni-1.1-flash',SECONDS=10,OUTPUT_USD_PER_SECOND=5792*17.5/1e6;
const API='https://generativelanguage.googleapis.com/v1beta/';
function requestBody(image,prompt,orientation){
 if(!Buffer.isBuffer(image)||!image.length)throw Error('A product reference is required for animation.');
 if(!['portrait','landscape'].includes(orientation))throw Error('Choose a supported video orientation.');
 return {model:MODEL,background:true,store:true,input:[{type:'image',mime_type:'image/jpeg',data:image.toString('base64')},{type:'text',text:prompt}],response_format:{type:'video',aspect_ratio:orientation==='portrait'?'9:16':'16:9',resolution:'720p',delivery:'uri'}};
}
function outputVideo(data){return (data.steps||[]).filter(s=>s.type==='model_output').flatMap(s=>s.content||[]).find(c=>c.type==='video'&&(c.uri||c.data))||null;}
function createGeminiVideo({apiKey,fetch}){
 async function request(route,method='GET',body){
  if(!apiKey){const e=Error('Video generation needs the existing GEMINI_API_KEY connection. Images can still be designed.');e.definiteResponse=true;throw e;}
  if(!/^interactions(?:\/[A-Za-z0-9_.:-]+)?$/.test(route))throw Error('Invalid Gemini interaction reference.');
  const r=await fetch(API+route,{method,timeout:120000,headers:{'x-goog-api-key':apiKey,'Api-Revision':'2026-05-20','Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
  const data=await r.json();if(!r.ok){const e=Error('Gemini video: '+String(data.error?.message||'HTTP '+r.status).slice(0,700));e.definiteResponse=r.status>=400&&r.status<500;e.retryAfter=Number(r.headers.get('retry-after'))||null;throw e;}
  return data;
 }
 async function content(video){
  if(video?.data)return Buffer.from(video.data,'base64');
  let url;try{url=new URL(video?.uri);}catch{throw Error('Gemini returned no downloadable video.');}
  const googleApi=url.hostname==='generativelanguage.googleapis.com',storage=url.hostname==='storage.googleapis.com'||url.hostname.endsWith('.googleusercontent.com');
  if(url.protocol!=='https:'||(!googleApi&&!storage))throw Error('Gemini returned an unsupported video download host.');
  // Never forward the API key to a storage host or redirect.
  const r=await fetch(url.href,{timeout:120000,size:100000000,redirect:'error',headers:googleApi?{'x-goog-api-key':apiKey}:{}});
  if(!r.ok)throw Error('The saved Gemini video could not be downloaded (HTTP '+r.status+').');
  return r.buffer();
 }
 return {request,content};
}
module.exports={MODEL,SECONDS,OUTPUT_USD_PER_SECOND,requestBody,outputVideo,createGeminiVideo};
