'use strict';
// Atomic coordination for independent Work resumptions. This is private
// sandbox state; neither lease tokens nor checkpoints are shopper knowledge.
const crypto=require('node:crypto'),core=require('./_britesGrowth');
function createController(service,{now=Date.now}={}){
  const ref=service.col('State').doc('controller'),checkpoint=service.col('State').doc('checkpoint');
  const validOwner=value=>typeof value==='string'&&/^[a-zA-Z0-9:_-]{1,100}$/.test(value);
  const projection=(lease,at)=>({owner:lease?.owner||null,leaseUntil:Number(lease?.leaseUntil)||0,active:Number(lease?.leaseUntil)>at,updatedAt:Number(lease?.updatedAt)||0});
  function owns(lease,owner,token,at){return validOwner(owner)&&lease?.owner===owner&&typeof token==='string'&&token.length>=32&&lease.leaseUntil>at&&core.sameSecret(core.hash(token),lease.tokenHash);}
  async function read(){const s=await ref.get();return {lease:projection(s.exists?s.data():null,now())};}
  async function claim(owner,leaseMinutes=45){
    if(!validOwner(owner)||!Number.isInteger(leaseMinutes)||leaseMinutes<5||leaseMinutes>90)throw Error('A valid owner and 5–90 minute controller lease are required.');
    const control=await service.setup(),at=now(),stopAt=Math.min(Number(control.stopAt)||core.STOP_AT,core.STOP_AT);
    if(!control.enabled||at>=stopAt)return {stopped:true};
    const token=crypto.randomBytes(32).toString('hex');
    return ref.firestore.runTransaction(async tx=>{
      const s=await tx.get(ref),old=s.exists?s.data():null;if(old?.leaseUntil>at)return {busy:true,lease:projection(old,at)};
      const lease={owner,tokenHash:core.hash(token),leaseUntil:Math.min(at+leaseMinutes*60000,stopAt),updatedAt:at};tx.set(ref,lease);return {ok:true,token,lease:projection(lease,at)};
    });
  }
  async function renew(owner,token,leaseMinutes=45){
    if(!Number.isInteger(leaseMinutes)||leaseMinutes<5||leaseMinutes>90)throw Error('A 5–90 minute controller lease is required.');
    const control=await service.setup(),at=now(),stopAt=Math.min(Number(control.stopAt)||core.STOP_AT,core.STOP_AT);if(!control.enabled||at>=stopAt)return {stopped:true};
    return ref.firestore.runTransaction(async tx=>{const s=await tx.get(ref),lease=s.exists?s.data():null;if(!owns(lease,owner,token,at))throw Error('Controller lease is missing, expired or owned by another writer.');const next={...lease,leaseUntil:Math.min(at+leaseMinutes*60000,stopAt),updatedAt:at};tx.set(ref,next);return {ok:true,lease:projection(next,at)};});
  }
  async function release(owner,token){return ref.firestore.runTransaction(async tx=>{const s=await tx.get(ref),lease=s.exists?s.data():null,at=now();if(!owns(lease,owner,token,at))throw Error('Controller lease is missing, expired or owned by another writer.');tx.set(ref,{owner:null,tokenHash:null,leaseUntil:0,updatedAt:at});return {ok:true};});}
  async function save(value,{owner,token,expectedUpdatedAt}={}){
    if(!value||typeof value!=='object'||Array.isArray(value)||Buffer.byteLength(JSON.stringify(value),'utf8')>250000)throw Error('A bounded checkpoint object is required.');
    if(expectedUpdatedAt!=null&&(!Number.isFinite(expectedUpdatedAt)||expectedUpdatedAt<0))throw Error('A valid checkpoint revision is required.');
    return ref.firestore.runTransaction(async tx=>{
      const [ls,cs]=await Promise.all([tx.get(ref),tx.get(checkpoint)]),lease=ls.exists?ls.data():null,current=cs.exists?cs.data():null,at=now();
      if(lease?.leaseUntil>at&&!owns(lease,owner,token,at))throw Error('An active controller lease owns this checkpoint.');
      if(expectedUpdatedAt!=null&&Number(current?.updatedAt||0)!==expectedUpdatedAt)throw Error('Checkpoint changed; reread the current revision before saving.');
      const updatedAt=Math.max(at,Number(current?.updatedAt||0)+1);tx.set(checkpoint,{...value,updatedAt});return {ok:true,updatedAt};
    });
  }
  return {read,claim,renew,release,save};
}
module.exports={createController};
